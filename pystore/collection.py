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
import logging
import dask.dataframe as dd
import multitasking

from . import utils
from .item import Item
from . import config


logger = logging.getLogger('pystore')


class Collection(object):
    def __repr__(self):
        return "PyStore.collection <%s>" % self.collection

    def __init__(self, collection, datastore, engine="fastparquet"):
        self.engine = engine
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
                    snapshot, filters, columns, engine=self.engine)

    def index(self, item, last=False):
        data = dd.read_parquet(self._item_path(item, as_string=True),
                               columns="index", engine=self.engine)
        if not last:
            return data.index.compute()

        return float(str(data.index).split(
                     "\nName")[0].split("\n")[-1].split(" ")[0])

    def delete_item(self, item, reload_items=False):
        logger.info(f"Deleting item '{item}' from collection '{self.collection}'")
        shutil.rmtree(self._item_path(item))
        self.items.remove(item)
        if reload_items:
            self.items = self._list_items_threaded()
        logger.info(f"Successfully deleted item '{item}' from collection '{self.collection}'")
        return True

    def rename_item(self, old_item, new_item, reload_items=False):
        """Rename an item in the collection.

        Parameters
        ----------
        old_item : str
            The current name of the item
        new_item : str
            The new name for the item
        reload_items : bool, optional (default=False)
            If True, reload the list of items after renaming

        Returns
        -------
        bool : True if successful

        Raises
        ------
        ValueError : If old_item doesn't exist or new_item already exists
        """
        logger.info(f"Renaming item '{old_item}' to '{new_item}' in collection '{self.collection}'")

        # Check if old item exists
        if not utils.path_exists(self._item_path(old_item)):
            raise ValueError(f"Item '{old_item}' does not exist")

        # Check if new item doesn't already exist
        if utils.path_exists(self._item_path(new_item)):
            raise ValueError(f"Item '{new_item}' already exists")

        # Rename the item directory
        old_path = self._item_path(old_item, as_string=True)
        new_path = self._item_path(new_item, as_string=True)
        shutil.move(old_path, new_path)

        # Update items set
        self.items.discard(old_item)
        self.items.add(new_item)

        if reload_items:
            self._list_items_threaded()

        logger.info(f"Successfully renamed item '{old_item}' to '{new_item}' in collection '{self.collection}'")
        return True

    @multitasking.task
    def write_threaded(self, item, data, metadata={},
                       npartitions=None, chunksize=None,
                       overwrite=False, epochdate=False,
                       reload_items=False, **kwargs):
        return self.write(item, data, metadata,
                          npartitions, chunksize, overwrite,
                          epochdate, reload_items,
                          **kwargs)

    def write(self, item, data, metadata={},
              npartitions=None, chunksize=None, overwrite=False,
              epochdate=False, reload_items=False,
              **kwargs):

        logger.info(f"Writing item '{item}' to collection '{self.collection}'")

        if utils.path_exists(self._item_path(item)) and not overwrite:
            raise ValueError("""
                Item already exists. To overwrite, use `overwrite=True`.
                Otherwise, use `<collection>.append()`""")

        if isinstance(data, Item):
            data = data.to_pandas()
        else:
            # work on copy
            data = data.copy()

        if epochdate or "datetime" in str(data.index.dtype):
            data = utils.datetime_to_int64(data)
            if 1 in data.index.nanosecond and "times" not in kwargs:
                kwargs["times"] = "int96"

        if data.index.name == "":
            data.index.name = "index"

        if npartitions is None and chunksize is None:
            memusage = data.memory_usage(deep=True).sum()
            if isinstance(data, dd.DataFrame):
                npartitions = int(
                    1 + memusage.compute() // config.PARTITION_SIZE)
                data.repartition(npartitions=npartitions)
            else:
                npartitions = int(
                    1 + memusage // config.PARTITION_SIZE)
                data = dd.from_pandas(data, npartitions=npartitions)

        dd.to_parquet(data, self._item_path(item, as_string=True),
                      compression="snappy", engine=self.engine, **kwargs)

        utils.write_metadata(utils.make_path(
            self.datastore, self.collection, item), metadata)

        # update items
        self.items.add(item)
        if reload_items:
            self._list_items_threaded()

        logger.info(f"Successfully wrote item '{item}' to collection '{self.collection}'")

    def _get_item_schema(self, item):
        """Extract schema from existing item.

        Returns a dictionary containing column names, dtypes, and index type.
        """
        item_path = self._item_path(item, as_string=True)
        # Read metadata only (not full data) to get schema
        ddf = dd.read_parquet(item_path, engine=self.engine)
        schema = {
            'columns': list(ddf.columns),
            'dtypes': ddf.dtypes.to_dict(),
            'index_name': ddf.index.name,
            'index_type': str(type(ddf.index).__name__)
        }
        return schema

    def _validate_data_compatibility(self, new_data, existing_schema,
                                     strictness='strict',
                                     allow_extra_columns=False):
        """Validate that new data is compatible with existing data schema.

        Parameters
        ----------
        new_data : pandas.DataFrame
            The new data to validate
        existing_schema : dict
            Schema of existing data from _get_item_schema
        strictness : str
            'strict' - raises ValueError on mismatch
            'warn' - logs warning but proceeds
            'disabled' - no validation
        allow_extra_columns : bool
            If True, allows extra columns in new data

        Returns
        -------
        tuple : (is_valid, error_messages)
        """
        import warnings

        if strictness == 'disabled':
            return True, []

        errors = []
        existing_columns = set(existing_schema['columns'])
        new_columns = set(new_data.columns)

        # Check for missing columns (columns in existing but not in new)
        missing_columns = existing_columns - new_columns
        if missing_columns:
            errors.append(
                f"Missing columns in new data: {sorted(missing_columns)}"
            )

        # Check for extra columns (columns in new but not in existing)
        extra_columns = new_columns - existing_columns
        if extra_columns and not allow_extra_columns:
            errors.append(
                f"Extra columns in new data not present in existing: "
                f"{sorted(extra_columns)}"
            )

        # Check dtype compatibility for common columns
        common_columns = existing_columns & new_columns
        for col in common_columns:
            existing_dtype = existing_schema['dtypes'].get(col)
            new_dtype = new_data[col].dtype
            if existing_dtype is not None:
                # Check if dtypes are compatible
                if not self._are_dtypes_compatible(existing_dtype, new_dtype):
                    errors.append(
                        f"dtype mismatch for column '{col}': "
                        f"existing={existing_dtype}, new={new_dtype}"
                    )

        # Check index type
        existing_index_type = existing_schema.get('index_type', 'Index')
        new_index_type = str(type(new_data.index).__name__)
        if existing_index_type != new_index_type:
            errors.append(
                f"index type mismatch: existing={existing_index_type}, "
                f"new={new_index_type}"
            )

        # Handle validation based on strictness
        if errors:
            if strictness == 'strict':
                return False, errors
            elif strictness == 'warn':
                warning_msg = "Schema validation warnings: " + "; ".join(errors)
                warnings.warn(warning_msg)
                return True, []  # Still valid in warn mode

        return True, []

    def _are_dtypes_compatible(self, existing_dtype, new_dtype):
        """Check if two pandas dtypes are compatible.

        Parameters
        ----------
        existing_dtype : pandas dtype
            The existing dtype
        new_dtype : pandas dtype
            The new dtype

        Returns
        -------
        bool : True if compatible, False otherwise
        """
        import pandas as pd

        # Use pandas API for dtype comparison
        if pd.api.types.is_dtype_equal(existing_dtype, new_dtype):
            return True

        # Check for numeric type compatibility
        existing_is_numeric = pd.api.types.is_numeric_dtype(existing_dtype)
        new_is_numeric = pd.api.types.is_numeric_dtype(new_dtype)

        if existing_is_numeric and new_is_numeric:
            # Both numeric - check if both are integer or both are float
            existing_is_float = pd.api.types.is_float_dtype(existing_dtype)
            new_is_float = pd.api.types.is_float_dtype(new_dtype)
            return existing_is_float == new_is_float

        # Check for string type compatibility
        existing_is_string = (
            pd.api.types.is_string_dtype(existing_dtype) or
            existing_dtype == 'object'
        )
        new_is_string = (
            pd.api.types.is_string_dtype(new_dtype) or
            new_dtype == 'object'
        )
        return existing_is_string == new_is_string

    def append(self, item, data, npartitions=None, epochdate=False,
               threaded=False, reload_items=False, remove_duplicates=None,
               validate_schema=False, schema_strictness='strict',
               allow_extra_columns=False, **kwargs):
        """Append new data to the collection.

        Saves new data to the collection and optionially removes duplicates
        within the data.

        Parameters
        ----------
        item
        data
        npartitions
        epochdate
        threaded
        reload_items
        remove_duplicates : str, optional (default=None)
            Defines how duplicates within the combined dataframe will be
                handled.
            None = no check for duplicated data. This is the fastest option
                but the user is responsible for not having an overlap
                between the new and old data
            "index" = For data with unique index but non unique row values.
                Rows with duplicated indices will be deleted. Ignores the
                values
            "values" = For data with non unique index but unique row values.
                Rows with duplicated values will be deleted. Ignores the index
            "all" = For data with unique index and unique row values. Rows
                with duplicated indices will be deleted first and then all
                rows with duplicated values will be deleted
            "values_in_index" = For data with non unique index but unique row
                values within index duplicates. Rows with duplicated values
                within the same index will be deleted
        validate_schema : bool, optional (default=False)
            When True, validates schema compatibility before appending data.
            This checks column names, dtypes, and index type compatibility.
        schema_strictness : str, optional (default='strict')
            Controls behavior on validation failure. Valid values:
            'strict' - raises ValueError on mismatch
            'warn' - logs warning but proceeds with append
            'disabled' - no validation performed
        allow_extra_columns : bool, optional (default=False)
            When True, allows appended data to have columns not present in
            existing data. Only relevant when validate_schema=True.

        kwargs

        Returns
        -------

        """

        logger.info(f"Appending data to item '{item}' in collection '{self.collection}'")

        if not utils.path_exists(self._item_path(item)):
            raise ValueError(
                """Item do not exists. Use `<collection>.write(...)`""")

        # work on copy
        data = data.copy()

        # Validate schema if enabled
        if validate_schema:
            existing_schema = self._get_item_schema(item)
            is_valid, error_messages = self._validate_data_compatibility(
                data, existing_schema, schema_strictness, allow_extra_columns
            )
            if not is_valid:
                raise ValueError(
                    "Schema validation failed: " + "; ".join(error_messages)
                )

        try:
            if epochdate or ("datetime" in str(data.index.dtype) and
                             any(data.index.nanosecond) > 0):
                data = utils.datetime_to_int64(data)
            old_index = dd.read_parquet(self._item_path(item, as_string=True),
                                        columns=[], engine=self.engine
                                        ).index.compute()
            data = data[~data.index.isin(old_index)]
        except Exception:
            return

        if data.empty:
            return

        if data.index.name == "":
            data.index.name = "index"

        # get old and new dataframe
        current = self.item(item)
        new = dd.from_pandas(data, npartitions=1)

        # combine old dataframe with new and optionally remove duplicates from 
        # combined dataframe
        idx_name = data.index.name
        if remove_duplicates is None:
            combined = dd.concat([current.data, new])
        elif remove_duplicates == 'index':
            combined = dd.concat([current.data, new])\
                .reset_index()\
                .drop_duplicates(subset=idx_name, keep="last")\
                .set_index(idx_name)
        elif remove_duplicates == 'values':
            combined = dd.concat([current.data, new])\
                .drop_duplicates(keep="last")
        elif remove_duplicates == 'all':
            combined = dd.concat([current.data, new])\
                .reset_index()\
                .drop_duplicates(subset=idx_name, keep="last")\
                .set_index(idx_name)\
                .drop_duplicates(keep="last")
        elif remove_duplicates == 'values_in_index':
            combined = dd.concat([current.data, new])\
                .reset_index()\
                .drop_duplicates(keep="last")\
                .set_index(idx_name)
        else:
            raise ValueError(
                """argument remove_duplicates must either be None, 'index', 
                'values', 'all' or 'values_in_index'""")     
        
        if npartitions is None:
            memusage = combined.memory_usage(deep=True).sum()
            if isinstance(combined, dd.DataFrame):
                memusage = memusage.compute()
            npartitions = int(1 + memusage // config.PARTITION_SIZE)

        # write data
        write = self.write_threaded if threaded else self.write
        write(item, combined, npartitions=npartitions, chunksize=None,
              metadata=current.metadata, overwrite=True,
              epochdate=epochdate, reload_items=reload_items, **kwargs)

        logger.info(f"Successfully appended data to item '{item}' in collection '{self.collection}'")

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
