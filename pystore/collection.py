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
from .partition import calculate_optimal_partitions, optimize_time_series_partitions

logger = get_logger(__name__)


class Collection(object):
    def __repr__(self):
        return "PyStore.collection <%s>" % self.collection

    def __init__(self, collection, datastore):
        self.datastore = datastore
        self.collection = collection
        self.items = self.list_items()
        self.snapshots = self.list_snapshots()
        self._metadata_cache = {}  # Cache for item metadata
        self._cache_timestamp = {}

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
    
    def get_item_metadata(self, item: str, use_cache: bool = True) -> dict:
        """Get item metadata with optional caching"""
        import time
        
        # Check cache first if enabled
        if use_cache and item in self._metadata_cache:
            # Check if cache is still valid (5 minutes)
            if time.time() - self._cache_timestamp.get(item, 0) < 300:
                logger.debug(f"Using cached metadata for item '{item}'")
                return self._metadata_cache[item].copy()
        
        # Read metadata from disk
        metadata_path = utils.make_path(self.datastore, self.collection, item)
        metadata = utils.read_metadata(metadata_path)
        
        # Update cache
        if use_cache:
            self._metadata_cache[item] = metadata.copy()
            self._cache_timestamp[item] = time.time()
        
        return metadata
    
    def clear_metadata_cache(self, item: Optional[str] = None) -> None:
        """Clear metadata cache for specific item or all items"""
        if item:
            self._metadata_cache.pop(item, None)
            self._cache_timestamp.pop(item, None)
            logger.debug(f"Cleared metadata cache for item '{item}'")
        else:
            self._metadata_cache.clear()
            self._cache_timestamp.clear()
            logger.debug("Cleared all metadata cache")

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
            # Use optimized partitioning
            is_time_series = pd.api.types.is_datetime64_any_dtype(data.index)
            
            if is_time_series and len(data) > 10000:
                # Use time-based partitioning for large time series
                logger.debug("Using time-based partitioning for time series data")
                if isinstance(data, pd.DataFrame):
                    temp_dd = dd.from_pandas(data, npartitions=1)
                    data, npartitions = optimize_time_series_partitions(temp_dd)
                else:
                    data, npartitions = optimize_time_series_partitions(data)
            else:
                # Use size-based partitioning
                logger.debug("Using size-based partitioning")
                npartitions = calculate_optimal_partitions(data)
                
                if isinstance(data, dd.DataFrame):
                    data = data.repartition(npartitions=npartitions)
                else:
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
    
    def append_stream(self, item: str, data_iterator, 
                     chunk_size: int = 10000,
                     epochdate: bool = False,
                     duplicate_handling: str = "keep_last",
                     validate_schema: bool = True,
                     reload_items: bool = True,
                     **kwargs) -> None:
        """
        Stream append data to an existing item for memory efficiency
        
        Parameters
        ----------
        item : str
            Name of the item to append to
        data_iterator : iterator of pd.DataFrame
            Iterator yielding DataFrames to append
        chunk_size : int, default 10000
            Size of chunks to process at a time
        epochdate : bool, default False
            Convert datetime index to epoch int64
        duplicate_handling : str, default "keep_last"
            How to handle duplicates: "keep_last", "keep_first", "keep_all", "error"
        validate_schema : bool, default True
            Validate schema compatibility before appending
        reload_items : bool, default True
            Reload items list after append
        **kwargs
            Additional parameters for to_parquet
        """
        logger.info(f"Starting streaming append for item '{item}'")
        
        # Validate item exists
        if not utils.path_exists(self._item_path(item)):
            raise ItemNotFoundError(f"Item '{item}' does not exist. Use write() to create new items.")
        
        # Get current item for schema validation
        current_item = self.item(item)
        schema_validated = False
        total_rows_appended = 0
        
        try:
            for chunk_num, data_chunk in enumerate(data_iterator):
                if not isinstance(data_chunk, pd.DataFrame):
                    raise ValidationError(f"Expected pandas DataFrame, got {type(data_chunk)}")
                
                if data_chunk.empty:
                    logger.debug(f"Skipping empty chunk {chunk_num}")
                    continue
                
                # Validate schema on first chunk
                if validate_schema and not schema_validated:
                    logger.debug("Validating schema compatibility")
                    self._validate_schema_compatibility(current_item.data, data_chunk)
                    schema_validated = True
                
                # Process chunk
                logger.debug(f"Processing chunk {chunk_num} with {len(data_chunk)} rows")
                self.append(item, data_chunk, 
                          epochdate=epochdate,
                          duplicate_handling=duplicate_handling,
                          validate_schema=False,  # Already validated
                          reload_items=False,  # Reload only at end
                          **kwargs)
                
                total_rows_appended += len(data_chunk)
                logger.debug(f"Appended {len(data_chunk)} rows (total: {total_rows_appended})")
                
            if reload_items:
                self._list_items_threaded()
            
            logger.info(f"Successfully appended {total_rows_appended} total rows to item '{item}'")
            
        except Exception as e:
            logger.error(f"Error during streaming append: {e}")
            raise
    
    def write_batch(self, items_data: dict, metadata: dict = None,
                   npartitions: dict = None, overwrite: bool = False,
                   epochdate: bool = False, parallel: bool = True,
                   **kwargs) -> None:
        """
        Write multiple items in batch for better performance
        
        Parameters
        ----------
        items_data : dict
            Dictionary mapping item names to DataFrames
        metadata : dict, optional
            Dictionary mapping item names to metadata dicts
        npartitions : dict, optional
            Dictionary mapping item names to number of partitions
        overwrite : bool, default False
            Whether to overwrite existing items
        epochdate : bool, default False
            Convert datetime index to epoch int64
        parallel : bool, default True
            Whether to write items in parallel
        **kwargs
            Additional parameters for to_parquet
        """
        logger.info(f"Starting batch write for {len(items_data)} items")
        
        if metadata is None:
            metadata = {}
        if npartitions is None:
            npartitions = {}
        
        # Write function wrapper
        def write_single(item_name, data):
            try:
                item_metadata = metadata.get(item_name, {})
                item_npartitions = npartitions.get(item_name, None)
                
                if parallel:
                    self.write_threaded(item_name, data, 
                                      metadata=item_metadata,
                                      npartitions=item_npartitions,
                                      overwrite=overwrite,
                                      epochdate=epochdate,
                                      reload_items=False,
                                      **kwargs)
                else:
                    self.write(item_name, data,
                              metadata=item_metadata,
                              npartitions=item_npartitions,
                              overwrite=overwrite,
                              epochdate=epochdate,
                              reload_items=False,
                              **kwargs)
                              
                logger.debug(f"Successfully wrote item '{item_name}'")
                return True
            except Exception as e:
                logger.error(f"Failed to write item '{item_name}': {e}")
                return False
        
        # Process all items
        success_count = 0
        for item_name, data in items_data.items():
            if write_single(item_name, data):
                success_count += 1
        
        # Wait for parallel tasks if needed
        if parallel:
            multitasking.wait_for_tasks()
        
        # Reload items once at the end
        self._list_items_threaded()
        
        logger.info(f"Batch write completed: {success_count}/{len(items_data)} items written successfully")
        
        if success_count < len(items_data):
            raise StorageError(f"Batch write partially failed: only {success_count}/{len(items_data)} items written")
    
    def read_batch(self, items: list, columns: dict = None,
                  filters: dict = None) -> dict:
        """
        Read multiple items in batch
        
        Parameters
        ----------
        items : list
            List of item names to read
        columns : dict, optional
            Dictionary mapping item names to column lists
        filters : dict, optional
            Dictionary mapping item names to filter lists
        
        Returns
        -------
        dict
            Dictionary mapping item names to DataFrames
        """
        logger.info(f"Starting batch read for {len(items)} items")
        
        if columns is None:
            columns = {}
        if filters is None:
            filters = {}
        
        results = {}
        
        for item_name in items:
            try:
                item_columns = columns.get(item_name, None)
                item_filters = filters.get(item_name, None)
                
                item = self.item(item_name, 
                               columns=item_columns,
                               filters=item_filters)
                results[item_name] = item.to_pandas()
                logger.debug(f"Successfully read item '{item_name}'")
            except Exception as e:
                logger.error(f"Failed to read item '{item_name}': {e}")
                results[item_name] = None
        
        successful_reads = sum(1 for v in results.values() if v is not None)
        logger.info(f"Batch read completed: {successful_reads}/{len(items)} items read successfully")
        
        return results
