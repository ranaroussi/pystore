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

from typing import cast

import dask.dataframe as dd
import pandas as pd

from . import utils
from .dataframe import DataTypeHandler, TimezoneHandler, restore_dataframe_from_storage
from .exceptions import ItemNotFoundError, SnapshotNotFoundError
from .logger import get_logger

logger = get_logger(__name__)


class Item:
    def __repr__(self):
        return f"PyStore.item <{self.collection}/{self.item}>"

    def __init__(
        self, item, datastore, collection, snapshot=None, filters=None, columns=None
    ):
        self.datastore = datastore
        self.collection = utils.validate_identifier(collection, "Collection")
        self.item = utils.validate_identifier(item, "Item")

        if snapshot is not None:
            self.snapshot = utils.validate_identifier(snapshot, "Snapshot")
            snap_path = utils.make_path(datastore, self.collection, "_snapshots", self.snapshot)
            if not utils.path_exists(snap_path):
                raise SnapshotNotFoundError(
                    f"Snapshot '{self.snapshot}' doesn't exist"
                )

            self._path = utils.make_path(snap_path, self.item)

            if not utils.path_exists(self._path):
                raise ItemNotFoundError(
                    f"Item '{self.item}' doesn't exist in snapshot '{self.snapshot}'"
                )
        else:
            self.snapshot = None
            self._path = utils.make_path(datastore, self.collection, self.item)
            if not utils.path_exists(self._path):
                raise ItemNotFoundError(
                    f"Item '{self.item}' doesn't exist. "
                    f"Create it using collection.write('{self.item}', data, ...)"
                )

        self.metadata = utils.read_metadata(self._path)
        self.data = dd.read_parquet(
            self._path, engine="pyarrow", filters=filters, columns=columns
        )

    def to_pandas(self, parse_dates=True):
        df = self.data.compute()

        # Restore complex types if needed
        if "_type_info" in self.metadata:
            df = DataTypeHandler.deserialize_complex_types(
                df, self.metadata["_type_info"]
            )

        # Restore MultiIndex and other transformations
        if "_transform_metadata" in self.metadata:
            df = restore_dataframe_from_storage(
                df, self.metadata["_transform_metadata"]
            )

        # Restore timezone information
        if "_timezone_info" in self.metadata:
            df = TimezoneHandler.restore_timezone_data(
                df, self.metadata["_timezone_info"]
            )

        if parse_dates and "datetime" not in str(df.index.dtype):
            if not isinstance(df.index, pd.MultiIndex):  # Only for single index
                # Preserve original index name
                original_name = df.index.name
                if str(df.index.dtype) == "float64":
                    df.index = pd.to_datetime(df.index, unit="s")
                elif len(df) > 0 and df.index.values[0] > 1e6:
                    df.index = pd.to_datetime(df.index)
                # Restore original name if it was None
                if original_name is None:
                    df.index.name = None

        df = self._restore_datetime_frequency(df)

        return df

    def _restore_datetime_frequency(self, df: pd.DataFrame) -> pd.DataFrame:
        """Restore DatetimeIndex frequency when parquet round-trips drop it.

        Uses the ``index_freq`` value stored in ``_transform_metadata`` at write
        time so that a ``freq=None`` index is never silently promoted to an
        inferred frequency on read-back.  Items written before this metadata
        field was introduced fall back to the old inference behaviour for
        backward compatibility.
        """
        if isinstance(df.index, pd.MultiIndex) or not isinstance(
            df.index, pd.DatetimeIndex
        ):
            return df

        transform_meta = self.metadata.get("_transform_metadata", {})

        if "index_freq" in transform_meta:
            stored_freq = transform_meta["index_freq"]
            if stored_freq is None:
                # Original index had freq=None; do not inject an inferred value.
                return df
            # Restore the exact freq recorded at write time.  This can fail
            # when the data has been filtered (e.g. via pushdown predicates)
            # and the remaining rows are no longer evenly spaced, so we
            # silently leave freq=None in that case.
            try:
                df = df.copy()
                df.index = pd.DatetimeIndex(
                    df.index, freq=stored_freq, name=df.index.name
                )
            except ValueError:
                pass
            return df

        # No freq metadata (item written before this fix) – fall back to
        # inference for backward compatibility.
        try:
            inferred_freq = pd.infer_freq(cast(pd.DatetimeIndex, df.index))
        except (TypeError, ValueError):
            inferred_freq = None

        if inferred_freq is None:
            return df

        df = df.copy()
        df.index = pd.DatetimeIndex(df.index, freq=inferred_freq, name=df.index.name)
        return df

    def head(self, n=5):
        return self.data.head(n)

    def tail(self, n=5):
        return self.data.tail(n)
