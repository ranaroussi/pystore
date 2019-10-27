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

import dask.dataframe as dd
import pandas as pd

from . import utils


class Item(object):
    def __repr__(self):
        return "PyStore.item <%s/%s>" % (self.collection, self.item)

    def __init__(self, item, datastore, collection,
                 snapshot=None, filters=None, columns=None,
                 engine="fastparquet"):
        self.engine = engine
        self.datastore = datastore
        self.collection = collection
        self.snapshot = snapshot
        self.item = item

        self._path = utils.make_path(datastore, collection, item)
        if not self._path.exists():
            raise ValueError(
                "Item `%s` doesn't exist. "
                "Create it using collection.write(`%s`, data, ...)" % (
                    item, item))
        if snapshot:
            snap_path = utils.make_path(
                datastore, collection, "_snapshots", snapshot)

            self._path = utils.make_path(snap_path, item)

            if not utils.path_exists(snap_path):
                raise ValueError("Snapshot `%s` doesn't exist" % snapshot)

            if not utils.path_exists(self._path):
                raise ValueError(
                    "Item `%s` doesn't exist in this snapshot" % item)

        self.metadata = utils.read_metadata(self._path)
        self.data = dd.read_parquet(
            self._path, engine=self.engine, filters=filters, columns=columns)

    def to_pandas(self, parse_dates=True):
        df = self.data.compute()

        if parse_dates and "datetime" not in str(df.index.dtype):
            df.index.name = ""
            if str(df.index.dtype) == "float64":
                df.index = pd.to_datetime(df.index, unit="s",
                                          infer_datetime_format=True)
            elif df.index.values[0] > 1e6:
                df.index = pd.to_datetime(df.index,
                                          infer_datetime_format=True)

        return df

    def head(self, n=5):
        return self.data.head(n)

    def tail(self, n=5):
        return self.data.tail(n)
