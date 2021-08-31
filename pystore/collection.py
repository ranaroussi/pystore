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
import dask.dataframe as dd
import multitasking

from . import utils
from .item import Item
from . import config


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
        shutil.rmtree(self._item_path(item))
        self.items.remove(item)
        if reload_items:
            self.items = self._list_items_threaded()
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

    def append(self, item, data, npartitions=None, epochdate=False,
               threaded=False, reload_items=False, remove_duplicates=None,
               **kwargs):
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

        kwargs

        Returns
        -------

        """
        
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
        idx_name = combined.index.name
        if remove_duplicates is None:
            # drops no duplicates
            combined = dd.concat([current.data, new])
        elif remove_duplicates == 'index':
            # drops all rows with duplicated indices (except the last one)
            # ignores the values
            combined = dd.concat([current.data, new])\
                .reset_index()\
                .drop_duplicates(subset=idx_name, keep="last")\
                .set_index(idx_name)
        elif remove_duplicates == 'values':
            # drops only rows with duplicated column values. Ignores the index
            combined = dd.concat([current.data, new])\
                .drop_duplicates(keep="last")
        elif remove_duplicates == 'all':
            # drops rows with duplicated indices first then drops duplicated 
            # column values
            combined = dd.concat([current.data, new])\
                .reset_index()\
                .drop_duplicates(subset=idx_name, keep="last")\
                .set_index(idx_name)\
                .drop_duplicates(keep="last")
        elif remove_duplicates == 'values_in_index':
            # drops all rows with duplicated values within an duplicated index
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
