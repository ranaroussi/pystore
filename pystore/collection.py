#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# PyStore: Flat-file datastore for timeseries data
# https://github.com/ranaroussi/pystore
#
# Copyright 2018-2019 Ran Aroussi
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

from . import utils
from .item import Item


class Collection(object):
    def __repr__(self):
        return 'PyStore.collection <%s>' % self.collection

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

    def list_items(self, **kwargs):
        dirs = utils.subdirs(utils.make_path(self.datastore, self.collection))
        if not kwargs:
            return dirs

        matched = []
        for d in dirs:
            meta = utils.read_metadata(utils.make_path(
                self.datastore, self.collection, d))
            del meta['_updated']

            m = 0
            keys = list(meta.keys())
            for k, v in kwargs.items():
                if k in keys and meta[k] == v:
                    m += 1

            if m == len(kwargs):
                matched.append(d)

        return matched

    def item(self, item, snapshot=None, filters=None, columns=None):
        return Item(item, self.datastore, self.collection,
                    snapshot, filters, columns)

    def index(self, item, last=False):
        data = dd.read_parquet(self._item_path(item, as_string=True),
                               columns='index',
                               engine='fastparquet')
        if not last:
            return data.index.compute()

        return float(str(data.index).split(
                     '\nName')[0].split('\n')[-1].split(' ')[0])

    def delete_item(self, item):
        shutil.rmtree(self._item_path(item))
        self.items = self.list_items()
        return True

    def write(self, item, data, metadata={},
              npartitions=None, chunksize=None, overwrite=False,
              epochdate=False, compression="snappy", **kwargs):

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

        if data.index.name == "":
            data.index.name = "index"

        if not isinstance(data, dd.DataFrame):
            if npartitions is None and chunksize is None:
                npartitions = None
                chunksize = int(1e6)

            data = dd.from_pandas(data,
                                  npartitions=npartitions,
                                  chunksize=chunksize)

        dd.to_parquet(data,
                      self._item_path(item, as_string=True),
                      compression=compression,
                      engine='fastparquet', **kwargs)

        utils.write_metadata(utils.make_path(
            self.datastore, self.collection, item), metadata)

        # update items
        self.items = self.list_items()

    def append(self, item, data, npartitions=None, chunksize=1e6,
               epochdate=False, compression="snappy", **kwargs):
        if not utils.path_exists(self._item_path(item)):
            raise ValueError(
                """Item do not exists. Use `<collection>.write(...)`""")

        # work on copy
        data = data.copy()

        try:
            if epochdate or ("datetime" in data.index.dtype_str and
                             any(data.index.nanosecond) > 0):
                data = utils.datetime_to_int64(data)
            old_index = dd.read_parquet(self._item_path(item, as_string=True),
                                        columns='index',
                                        engine='fastparquet'
                                        ).index.compute()
            data = data[~data.index.isin(old_index)]
        except Exception:
            return

        if data.empty:
            # if len(data.index) == 0:
            return

        if data.index.name == "":
            data.index.name = "index"

        data = dd.from_pandas(data,
                              npartitions=npartitions,
                              chunksize=int(chunksize))

        dd.to_parquet(data,
                      self._item_path(item, as_string=True),
                      append=True,
                      compression=compression,
                      engine='fastparquet', **kwargs)

    def create_snapshot(self, snapshot=None):
        if snapshot:
            snapshot = ''.join(
                e for e in snapshot if e.isalnum() or e in ['.', '_'])
        else:
            snapshot = str(int(time.time() * 1000000))

        src = utils.make_path(self.datastore, self.collection)
        dst = utils.make_path(src, '_snapshots', snapshot)

        shutil.copytree(src, dst,
                        ignore=shutil.ignore_patterns("_snapshots"))

        self.snapshots = self.list_snapshots()
        return True

    def list_snapshots(self):
        snapshots = utils.subdirs(utils.make_path(
            self.datastore, self.collection, '_snapshots'))
        return snapshots
        # return [s.parts[-1] for s in snapshots]

    def delete_snapshot(self, snapshot):
        if snapshot not in self.snapshots:
            # raise ValueError("Snapshot `%s` doesn't exist" % snapshot)
            return True

        shutil.rmtree(utils.make_path(self.datastore, self.collection,
                                      '_snapshots', snapshot))
        self.snapshots = self.list_snapshots()
        return True

    def delete_snapshots(self):
        snapshots_path = utils.make_path(
            self.datastore, self.collection, '_snapshots')
        shutil.rmtree(snapshots_path)
        os.makedirs(snapshots_path)
        self.snapshots = self.list_snapshots()
        return True
