#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# PyStore: Flat-file datastore for timeseries data
# https://github.com/ranaroussi/pystore
#
# Copyright 2018-2020 Ran Aroussi
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import time
import shutil
import tempfile
import pandas as pd
import dask.dataframe as dd
import multitasking
from typing import Optional, Union

from . import utils
from .item import Item
from . import config
from .exceptions import (
    ItemNotFoundError, ItemExistsError, ValidationError,
    DataIntegrityError, StorageError
)
from .logger import get_logger

logger = get_logger(__name__)


class Collection(object):
    def __repr__(self):
        return "PyStore.collection <%s>" % self.collection

    def __init__(self, collection, datastore):
        self.datastore = datastore
        self.collection = collection
        self.items = self.list_items()
        self.snapshots = self.list_snapshots()

    def _item_path(self, item, as_string=False):
        p = utils.make_path(self.datastore, self.collection, item)
        if as_string:
            return str(p)
        return p

    @multitasking.task
    def _list_items_threaded(self, **kwargs):
        self.items = self.list_items(**kwargs)

    def list_items(self, **kwargs):
        dirs = utils.subdirs(utils.make_path(self.datastore, self.collection))
        if not kwargs:
            return set(dirs)

        matched = []
        for d in dirs:
            meta = utils.read_metadata(utils.make_path(
                self.datastore, self.collection, d))
            del meta["_updated"]

            m = 0
            keys = list(meta.keys())
            for k, v in kwargs.items():
                if k in keys and meta[k] == v:
                    m += 1

            if m == len(kwargs):
                matched.append(d)

        return set(matched)

    def item(self, item, snapshot=None, filters=None, columns=None):
        return Item(item, self.datastore, self.collection,
                    snapshot, filters, columns)

    def index(self, item, last=False):
        data = dd.read_parquet(self._item_path(item, as_string=True),
                               columns="index", engine="pyarrow")
        if not last:
            return data.index.compute()

        return float(str(data.index).split(
                     "\nName")[0].split("\n")[-1].split(" ")[0])

    def delete_item(self, item, reload_items=False):
        if not utils.path_exists(self._item_path(item)):
            raise ItemNotFoundError(f"Item '{item}' does not exist")
        
        try:
            shutil.rmtree(self._item_path(item))
            self.items.remove(item)
            if reload_items:
                self.items = self._list_items_threaded()
            logger.info(f"Successfully deleted item '{item}'")
            return True
        except Exception as e:
            logger.error(f"Failed to delete item '{item}': {e}")
            raise StorageError(f"Failed to delete item '{item}': {str(e)}") from e

    @multitasking.task
    def write_threaded(self, item, data, metadata={},
                       npartitions=None,
                       overwrite=False, epochdate=False,
                       reload_items=False, **kwargs):
        return self.write(item, data, metadata,
                          npartitions, overwrite,
                          epochdate, reload_items,
                          **kwargs)

    def write(self, item, data, metadata={},
              npartitions=None, overwrite=False,
              epochdate=False, reload_items=False,
              **kwargs):

        if utils.path_exists(self._item_path(item)) and not overwrite:
            raise ItemExistsError(
                f"Item '{item}' already exists. To overwrite, use overwrite=True. "
                "Otherwise, use collection.append()"
            )

        if isinstance(data, Item):
            data = data.to_pandas()
        else:
            # work on copy
            data = data.copy()

        if epochdate or "datetime" in str(data.index.dtype):
            data = utils.datetime_to_int64(data)

        if data.index.name == "":
            data.index.name = "index"

        if npartitions is None:
            memusage = data.memory_usage(deep=True).sum()
            if isinstance(data, dd.DataFrame):
                npartitions = int(
                    1 + memusage.compute() // config.PARTITION_SIZE)
                data = data.repartition(npartitions=npartitions)
            else:
                npartitions = int(
                    1 + memusage // config.PARTITION_SIZE)
                data = dd.from_pandas(data, npartitions=npartitions)
        else:
            if not isinstance(data, dd.DataFrame):
                data = dd.from_pandas(data, npartitions=npartitions)

        dd.to_parquet(data, self._item_path(item, as_string=True), overwrite=overwrite,
                      compression="snappy", engine="pyarrow", **kwargs)

        utils.write_metadata(utils.make_path(
            self.datastore, self.collection, item), metadata)

        # update items
        self.items.add(item)
        if reload_items:
            self._list_items_threaded()

    def append(self, item, data, npartitions=None, epochdate=False,
               threaded=False, reload_items=False, **kwargs):

        if not utils.path_exists(self._item_path(item)):
            raise ValueError(
                """Item do not exists. Use `<collection>.write(...)`""")

        # work on copy
        data = data.copy()

        try:
            if epochdate or ("datetime" in str(data.index.dtype) and
                             any(data.index.nanosecond) > 0):
                data = utils.datetime_to_int64(data)
            old_index = dd.read_parquet(self._item_path(item, as_string=True),
                                        columns=[], engine="pyarrow"
                                        ).index.compute()
            data = data[~data.index.isin(old_index)]
        except Exception:
            return

        if data.empty:
            return

        if data.index.name == "":
            data.index.name = "index"

        # combine old dataframe with new
        current = self.item(item)
        new = dd.from_pandas(data, npartitions=1)
        combined = dd.concat([current.data, new], axis=0)

        if npartitions is None:
            memusage = combined.memory_usage(deep=True).sum()
            if isinstance(combined, dd.DataFrame):
                memusage = memusage.compute()
            npartitions = int(1 + memusage // config.PARTITION_SIZE)

        combined = combined.repartition(npartitions=npartitions).drop_duplicates(
            keep="last"
            )
        tmp_item = "__" + item
        # write data
        write = self.write_threaded if threaded else self.write
        write(tmp_item, combined, npartitions=npartitions,
              metadata=current.metadata, overwrite=False,
              epochdate=epochdate, reload_items=reload_items, **kwargs)

        try:
            multitasking.wait_for_tasks()
            self.delete_item(item=item,reload_items=False)
            shutil.move(self._item_path(tmp_item),self._item_path(item))
            self._list_items_threaded()
        except Exception as errn:
            raise ValueError(
                "Error: %s" % repr(errn)
            ) from errn

    def create_snapshot(self, snapshot=None):
        if snapshot:
            snapshot = "".join(
                e for e in snapshot if e.isalnum() or e in [".", "_"])
        else:
            snapshot = str(int(time.time() * 1000000))

        src = utils.make_path(self.datastore, self.collection)
        dst = utils.make_path(src, "_snapshots", snapshot)

        shutil.copytree(src, dst,
                        ignore=shutil.ignore_patterns("_snapshots"))

        self.snapshots = self.list_snapshots()
        return True

    def list_snapshots(self):
        snapshots = utils.subdirs(utils.make_path(
            self.datastore, self.collection, "_snapshots"))
        return set(snapshots)

    def delete_snapshot(self, snapshot):
        if snapshot not in self.snapshots:
            # raise ValueError("Snapshot `%s` doesn't exist" % snapshot)
            return True

        shutil.rmtree(utils.make_path(self.datastore, self.collection,
                                      "_snapshots", snapshot))
        self.snapshots = self.list_snapshots()
        return True

    def delete_snapshots(self):
        snapshots_path = utils.make_path(
            self.datastore, self.collection, "_snapshots")
        shutil.rmtree(snapshots_path)
        os.makedirs(snapshots_path)
        self.snapshots = self.list_snapshots()
        return True
    
    def _validate_schema_compatibility(self, existing_data: dd.DataFrame, 
                                      new_data: Union[pd.DataFrame, dd.DataFrame]) -> None:
        """Validate schema compatibility between existing and new data"""
        existing_columns = set(existing_data.columns)
        new_columns = set(new_data.columns)
        
        if existing_columns != new_columns:
            missing_in_new = existing_columns - new_columns
            extra_in_new = new_columns - existing_columns
            
            error_msg = "Schema mismatch detected:\n"
            if missing_in_new:
                error_msg += f"  Missing columns in new data: {missing_in_new}\n"
            if extra_in_new:
                error_msg += f"  Extra columns in new data: {extra_in_new}\n"
                
            raise ValidationError(error_msg)
    
    def _handle_duplicates(self, existing_data: dd.DataFrame,
                          new_data: Union[pd.DataFrame, dd.DataFrame],
                          strategy: str) -> tuple:
        """Handle duplicate indices based on strategy"""
        if strategy == "keep_all":
            return new_data, False
            
        # Get existing index efficiently
        existing_index = existing_data.index.compute()
        new_index = new_data.index
        
        # Find overlapping indices
        if isinstance(new_index, dd.Index):
            new_index = new_index.compute()
            
        overlapping_indices = existing_index.intersection(new_index)
        has_duplicates = len(overlapping_indices) > 0
        
        if has_duplicates:
            logger.info(f"Found {len(overlapping_indices)} overlapping indices")
            
            if strategy == "error":
                raise DataIntegrityError(
                    f"Found {len(overlapping_indices)} duplicate indices. "
                    "Use duplicate_handling='keep_last' or 'keep_first' to handle them."
                )
            elif strategy == "keep_first":
                # Remove overlapping indices from new data
                new_data = new_data[~new_data.index.isin(overlapping_indices)]
                logger.debug(f"Removed {len(overlapping_indices)} duplicate rows from new data")
        
        return new_data, has_duplicates
    
    def _atomic_write(self, item: str, data: dd.DataFrame, metadata: dict,
                      npartitions: int, epochdate: bool = False, **kwargs) -> None:
        """Perform atomic write operation using temporary directory"""
        tmp_dir = None
        tmp_item_name = f"_tmp_{item}_{os.getpid()}"
        
        try:
            # Create temporary directory in the same filesystem
            tmp_dir = tempfile.mkdtemp(dir=utils.make_path(self.datastore, self.collection))
            tmp_path = utils.make_path(tmp_dir, "data")
            
            logger.debug(f"Writing to temporary location: {tmp_path}")
            
            # Write data to temporary location
            dd.to_parquet(data, str(tmp_path), 
                         compression="snappy", 
                         engine="pyarrow",
                         write_metadata_file=True,
                         **kwargs)
            
            # Write metadata
            utils.write_metadata(tmp_path, metadata)
            
            # Get paths
            final_path = self._item_path(item)
            backup_path = self._item_path(f"_backup_{item}")
            
            # Create backup of existing data
            if utils.path_exists(final_path):
                logger.debug(f"Creating backup at: {backup_path}")
                if utils.path_exists(backup_path):
                    shutil.rmtree(backup_path)
                shutil.move(str(final_path), str(backup_path))
            
            # Move temporary data to final location
            logger.debug(f"Moving data to final location: {final_path}")
            shutil.move(str(tmp_path), str(final_path))
            
            # Remove backup on success
            if utils.path_exists(backup_path):
                logger.debug("Removing backup after successful write")
                shutil.rmtree(backup_path)
                
        except Exception as e:
            # Restore from backup if it exists
            backup_path = self._item_path(f"_backup_{item}")
            if utils.path_exists(backup_path):
                logger.warning("Restoring from backup due to write failure")
                final_path = self._item_path(item)
                if utils.path_exists(final_path):
                    shutil.rmtree(final_path)
                shutil.move(str(backup_path), str(final_path))
            raise
        finally:
            # Clean up temporary directory
            if tmp_dir and os.path.exists(tmp_dir):
                shutil.rmtree(tmp_dir)
