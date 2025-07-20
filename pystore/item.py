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

import dask.dataframe as dd
import pandas as pd

from . import utils
from .exceptions import ItemNotFoundError, SnapshotNotFoundError
from .logger import get_logger
from .dataframe import restore_dataframe_from_storage, DataTypeHandler, TimezoneHandler

logger = get_logger(__name__)


class Item(object):
    def __repr__(self):
        return "PyStore.item <%s/%s>" % (self.collection, self.item)

    def __init__(self, item, datastore, collection,
                 snapshot=None, filters=None, columns=None):
        self.datastore = datastore
        self.collection = collection
        self.snapshot = snapshot
        self.item = item

        self._path = utils.make_path(datastore, collection, item)
        if not self._path.exists():
            raise ItemNotFoundError(
                f"Item '{item}' doesn't exist. "
                f"Create it using collection.write('{item}', data, ...)"
            )
        if snapshot:
            snap_path = utils.make_path(
                datastore, collection, "_snapshots", snapshot)

            self._path = utils.make_path(snap_path, item)

            if not utils.path_exists(snap_path):
                raise SnapshotNotFoundError(f"Snapshot '{snapshot}' doesn't exist")

            if not utils.path_exists(self._path):
                raise ItemNotFoundError(
                    f"Item '{item}' doesn't exist in snapshot '{snapshot}'"
                )

        self.metadata = utils.read_metadata(self._path)
        self.data = dd.read_parquet(
            self._path, engine="pyarrow", filters=filters, columns=columns)

    def to_pandas(self, parse_dates=True):
        df = self.data.compute()

        # Restore complex types if needed
        if '_type_info' in self.metadata:
            df = DataTypeHandler.deserialize_complex_types(df, self.metadata['_type_info'])
        
        # Restore MultiIndex and other transformations
        if '_transform_metadata' in self.metadata:
            df = restore_dataframe_from_storage(df, self.metadata['_transform_metadata'])
        
        # Restore timezone information
        if '_timezone_info' in self.metadata:
            df = TimezoneHandler.restore_timezone_data(df, self.metadata['_timezone_info'])

        if parse_dates and "datetime" not in str(df.index.dtype):
            if not isinstance(df.index, pd.MultiIndex):  # Only for single index
                df.index.name = ""
                if str(df.index.dtype) == "float64":
                    df.index = pd.to_datetime(df.index, unit="s")
                elif df.index.values[0] > 1e6:
                    df.index = pd.to_datetime(df.index)

        return df

    def head(self, n=5):
        return self.data.head(n)

    def tail(self, n=5):
        return self.data.tail(n)
