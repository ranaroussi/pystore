#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# PyStore: Flat-file datastore for timeseries data
# https://github.com/ranaroussi/pystore
#
# Copyright 2018 Ran Aroussi
#
# Licensed under the GNU Lesser General Public License, v2.1 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.gnu.org/licenses/lgpl-2.1.en.html
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import json
from datetime import datetime
import shutil
import dask.dataframe as dd
import pandas as pd
import numpy as np

PATH = '~/.pystore'


def _datetime_to_int64(df):
    """ convert datetime index to epoch int
    allows for cross language/platform portability
    """
    if isinstance(df.index, pd.DatetimeIndex):
        df.index = df.index.astype(np.int64) / 1e9
    return df


def _subdirs(d):
    return [os.path.join(d, o).replace(d + '/', '') for o in os.listdir(d)
            if os.path.isdir(os.path.join(d, o))]

def _read_metadata(path):
    with open(path + '/metadata.json') as f:
        return json.load(f)


class Item(object):
    def __repr__(self):
        return 'PyStore.item <%s/%s>' % (self.collection, self.item)

    def __init__(self, item, datastore, collection, filters=None, columns=None):
        self.datastore = datastore
        self.collection = collection
        self.item = item
        self._path = datastore + '/' + collection + '/' + item

        self.metadata = _read_metadata(self._path)
        self.data = dd.read_parquet(
            self._path, engine='fastparquet', filters=filters, columns=columns)

    def to_pandas(self, parse_dates=True):
        df = self.data.compute()

        if parse_dates and "datetime" not in str(df.index.dtype):
            if str(df.index.dtype) == 'float64':
                df.index = pd.to_datetime(df.index, unit='s')
            else:
                df.index = pd.to_datetime(df.index)

        return df

    def head(self, n=5):
        return self.data.head(n)

    def tail(self, n=5):
        return self.data.tail(n)



class Collection(object):
    def __repr__(self):
        return 'PyStore.collection <%s>' % self.collection

    def __init__(self, collection, datastore):
        self.datastore = datastore
        self.collection = collection
        self.items = self.list_items()

    def _item_path(self, item):
        return self.datastore + '/' + self.collection + '/' + item

    def list_items(self):
        return _subdirs(self.datastore + '/' + self.collection)

    def item(self, item, filters=None, columns=None):
        return Item(item, self.datastore, self.collection, filters, columns)

    def index(self, item, last=False):
        data = dd.read_parquet(self._item_path(item),
                               columns='index',
                               engine='fastparquet')
        if not last:
            return data.index.compute()

        return float(str(data.index).split(
                     '\nName')[0].split('\n')[-1].split(' ')[0])

    def delete_item(self, item):
        shutil.rmtree(self._item_path(item))
        self.items = self.list_items()

    def write(self, item, data, metadata={},
              npartitions=None, chunksize=1e6, overwrite=False,
              epochdate=False, compression="snappy", **kwargs):

        if os.path.exists(self._item_path(item)) and not overwrite:
            raise ValueError("""
                Item already exists. To overwrite, use `overwrite=True`.
                Otherwise, use `<collection>.append()`""")

        if epochdate:
            data = _datetime_to_int64(data)
        data = dd.from_pandas(data,
                              npartitions=npartitions,
                              chunksize=int(chunksize))

        dd.to_parquet(data, self._item_path(item),
                      compression=compression,
                      engine='fastparquet', **kwargs)

        self.write_metadata(item, metadata)

        # update items
        self.items = self.list_items()

    def append(self, item, data, npartitions=None, chunksize=1e6,
               epochdate=False, compression="snappy", **kwargs):
        if not os.path.exists(self._item_path(item)):
            raise ValueError(
                """Item do not exists. Use `<collection>.write(...)`""")

        try:
            if epochdate:
                data = _datetime_to_int64(data)
            old_index = dd.read_parquet(self._item_path(item),
                                        columns='index',
                                        engine='fastparquet'
                                        ).index.compute()
            data = data[~data.index.isin(old_index)]
        except:
            return

        if data.empty:
            # if len(data.index) == 0:
            return

        data = dd.from_pandas(data,
                              npartitions=npartitions,
                              chunksize=int(chunksize))

        dd.to_parquet(data, self._item_path(item), append=True,
                      compression=compression,
                      engine='fastparquet', **kwargs)

    def write_metadata(self, item, metadata={}):
        now = datetime.now()
        # metadata['_updated'] = now.timestamp()
        metadata['_updated'] = now.strftime('%Y-%m-%d %H:%I:%S.%f')
        with open(self.datastore + '/' + self.collection + '/' + item +
                  '/metadata.json', 'w') as f:
            json.dump(metadata, f, ensure_ascii=False)


class store(object):
    def __repr__(self):
        return 'PyStore.datastore <%s>' % self.datastore

    def __init__(self, datastore):
        global PATH

        if not os.path.exists(PATH):
            os.makedirs(PATH)

        self.datastore = PATH + '/' + datastore  # <-- this is just a diretory
        if not os.path.exists(self.datastore):
            os.makedirs(self.datastore)

        self.collections = self.list_collections()

    def _create_collection(self, collection, overwrite=False):
        # create collection (subdir)
        if os.path.exists(self.datastore + '/' + collection):
            if overwrite:
                self.delete_collection(collection)
            else:
                raise ValueError(
                    "Collection already exists. To overwrite, use `overwrite=True`")

        os.makedirs(self.datastore + '/' + collection)

        # update collections
        self.collections = self.list_collections()

        # return the collection
        return Collection(collection, self.datastore)

    def delete_collection(self, collection):
        # delete collection (subdir)
        shutil.rmtree(self.datastore + '/' + collection)

        # update collections
        self.collections = self.list_collections()

    def list_collections(self):
        # lists collections (subdirs)
        return _subdirs(self.datastore)

    def collection(self, collection, overwrite=False):
        if collection in self.collections and not overwrite:
            return Collection(collection, self.datastore)
        else:
            # create it
            self._create_collection(collection, overwrite)
            return Collection(collection, self.datastore)


def set_path(path):
    global PATH

    if path is None:
        path = '~/.pystore'

    path = path.rstrip('/').rstrip('\\').rstrip(' ')
    if "://" in path and "file://" not in path:
        raise ValueError(
            "PyStore currently only works with local file system")

    # if path ot exist - create it
    PATH = path
    if not os.path.exists(PATH):
        os.makedirs(PATH)

    return PATH


def list_stores():
    global PATH

    if not os.path.exists(PATH):
        os.makedirs(PATH)

    return _subdirs(PATH)
