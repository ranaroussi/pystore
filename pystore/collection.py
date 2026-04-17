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

import concurrent.futures
import os
import shutil
import threading
import time
from typing import Any, Optional, Union, cast

import dask.dataframe as dd
import pandas as pd

from . import config, utils
from .dataframe import (
    DataTypeHandler,
    MultiIndexHandler,
    TimezoneHandler,
    are_dtypes_compatible,
    prepare_dataframe_for_storage,
    validate_dataframe_for_storage,
)
from .exceptions import (
    DataIntegrityError,
    ItemExistsError,
    ItemNotFoundError,
    SnapshotNotFoundError,
    StorageError,
    ValidationError,
)
from .item import Item
from .logger import get_logger
from .partition import calculate_optimal_partitions, optimize_time_series_partitions
from .transactions import with_lock

logger = get_logger(__name__)


class Collection:
    def __repr__(self):
        return f"PyStore.collection <{self.collection}>"

    # Maximum number of items to keep in the metadata cache before evicting
    # the least-recently-accessed entry.
    _METADATA_CACHE_MAX = 256

    def __init__(self, collection, datastore):
        self.datastore = datastore
        self.collection = utils.validate_identifier(collection, "Collection")
        self.items = self.list_items()
        self.snapshots = self.list_snapshots()
        self._metadata_cache: dict[str, dict[str, Any]] = {}  # Cache for item metadata
        self._cache_timestamp: dict[str, float] = {}
        self._validator = None  # Data validator
        self._schema_evolutions = {}  # Schema evolution per item
        self._items_lock = threading.Lock()  # Protects self.items mutations

    def get_item_path(self, item, as_string=False):
        """Get the filesystem path for an item.

        Parameters
        ----------
        item : str
            The item name
        as_string : bool, optional
            Return path as string instead of Path object

        Returns
        -------
        pathlib.Path or str
            The filesystem path to the item
        """
        item_name = utils.validate_identifier(item, "Item")
        p = utils.make_path(self.datastore, self.collection, item_name)
        if as_string:
            return str(p)
        return p

    def _item_path(self, item, as_string=False):
        """Deprecated: Use get_item_path instead"""
        import warnings

        warnings.warn(
            "_item_path is deprecated, use get_item_path instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.get_item_path(item, as_string)

    def _list_items_threaded(self, **kwargs):
        """Reload items list synchronously.

        .. deprecated::
            Previously decorated with ``@multitasking.task`` which made
            the method fire-and-forget.  Now runs synchronously and
            returns the updated items set.

        The ``_items_lock`` is held during the reassignment so that
        concurrent ``add``/``discard`` calls inside the lock are not
        silently overwritten.
        """
        fresh_items = self.list_items(**kwargs)
        with self._items_lock:
            self.items = fresh_items
        return self.items

    def list_items(self, **kwargs):
        dirs = utils.subdirs(utils.make_path(self.datastore, self.collection))
        if not kwargs:
            return set(dirs)

        matched = []
        for d in dirs:
            meta = utils.read_metadata(
                utils.make_path(self.datastore, self.collection, d)
            )
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
        return Item(item, self.datastore, self.collection, snapshot, filters, columns)

    def get_item_metadata(self, item: str, use_cache: bool = True) -> dict[str, Any]:
        """Get item metadata with optional caching"""
        # Check cache first if enabled
        if use_cache and item in self._metadata_cache:
            # Check if cache is still valid (5 minutes)
            if time.time() - self._cache_timestamp.get(item, 0) < 300:
                logger.debug(f"Using cached metadata for item '{item}'")
                return self._metadata_cache[item].copy()

        # Read metadata from disk
        metadata: dict[str, Any] = utils.read_metadata(self.get_item_path(item))

        # Update cache (with bounded eviction)
        if use_cache:
            self._metadata_cache[item] = metadata.copy()
            self._cache_timestamp[item] = time.time()

            # Evict least-recently-accessed entry when cache exceeds limit
            while len(self._metadata_cache) > self._METADATA_CACHE_MAX:
                oldest_item = min(
                    self._cache_timestamp, key=lambda k: self._cache_timestamp[k]
                )
                self._metadata_cache.pop(oldest_item, None)
                self._cache_timestamp.pop(oldest_item, None)

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
        data = dd.read_parquet(
            self.get_item_path(item, as_string=True), columns="index", engine="pyarrow"
        )
        if not last:
            return data.index.compute()

        # Compute the last index value directly instead of parsing the
        # string representation, which is fragile across Dask versions.
        idx = data.index.compute()
        if len(idx) == 0:
            return None
        last_val = idx[-1]
        # Return as float for backwards compatibility with numeric indices
        try:
            return float(last_val)
        except (TypeError, ValueError):
            return last_val

    def delete_item(self, item, reload_items=False):
        if not utils.path_exists(self.get_item_path(item)):
            raise ItemNotFoundError(f"Item '{item}' does not exist")

        try:
            shutil.rmtree(self.get_item_path(item))
            with self._items_lock:
                self.items.discard(item)
            if reload_items:
                self.items = self._list_items_threaded()
            logger.info(f"Successfully deleted item '{item}'")
            return True
        except Exception as e:
            logger.error(f"Failed to delete item '{item}': {e}")
            raise StorageError(f"Failed to delete item '{item}': {str(e)}") from e

    def set_validator(self, validator):
        """Set a data validator for this collection"""
        self._validator = validator

    def get_validator(self):
        """Get the current validator"""
        return self._validator

    def _validate_data(self, data):
        """Validate data before writing"""
        if self._validator is not None and self._validator.enabled:
            self._validator.validate(data)

    def enable_schema_evolution(self, item, strategy):
        """Enable schema evolution for an item"""
        from .schema_evolution import SchemaEvolution

        evolution = SchemaEvolution(strategy)
        # Set references for migrate_to_version to work
        evolution._collection = self
        evolution._item = item
        self._schema_evolutions[item] = evolution

    def get_item_evolution(self, item):
        """Get schema evolution instance for an item"""
        return self._schema_evolutions.get(item)

    def migrate_item_to_version(self, item: str, to_version: int) -> None:
        """Migrate an item to a specific schema version"""
        if item not in self._schema_evolutions:
            raise ValueError(f"Schema evolution not enabled for item '{item}'")

        evolution = self._schema_evolutions[item]
        current_data = self.item(item).to_pandas()

        # Apply migration
        migrated_data = evolution.migrate(current_data, 1, to_version)

        # Overwrite with migrated data
        self.write(item, migrated_data, overwrite=True)
        logger.info(f"Successfully migrated item '{item}' to version {to_version}")

    def write_threaded(self, *args, **kwargs):
        """Write data synchronously.

        .. deprecated::
            The ``@multitasking.task`` decorator was removed because it
            made the method fire-and-forget (returning ``None``
            immediately).  The method now runs synchronously and is
            equivalent to :meth:`write`.  For true asynchronous writes,
            use the ``AsyncCollection`` wrapper instead.
        """
        import warnings

        warnings.warn(
            "write_threaded is deprecated — it is now identical to write(). "
            "Use write() directly or AsyncCollection for async writes.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.write(*args, **kwargs)

    def _validate_write_item(self, item, overwrite):
        """Validate item doesn't exist unless overwrite is True."""
        if utils.path_exists(self.get_item_path(item)) and not overwrite:
            raise ItemExistsError(
                f"Item '{item}' already exists. To overwrite, use overwrite=True. "
                "Otherwise, use collection.append()"
            )

    def _prepare_write_data(self, data):
        """Prepare data for writing by converting Item to DataFrame and copying."""
        if isinstance(data, Item):
            return data.to_pandas()
        else:
            # work on copy
            return data.copy()

    def _apply_data_transformations(self, data, metadata):
        """Apply all data transformations and update metadata.

        .. note::
            The ``epochdate`` parameter was removed because datetime→int64
            conversion now happens in ``write()`` *after* partitioning, so
            that time-based partitioning can still detect DatetimeIndex data.
        """
        # Validate data
        self._validate_data(data)
        validate_dataframe_for_storage(data)

        # Handle MultiIndex and complex types
        data, transform_metadata = prepare_dataframe_for_storage(data)
        metadata = metadata.copy()
        metadata["_transform_metadata"] = transform_metadata

        # Handle complex data types — skip internal copy because
        # _prepare_write_data already gave us a private copy.
        data, type_info = DataTypeHandler.serialize_complex_types(data, copy=False)
        metadata["_type_info"] = type_info

        # Handle timezone-aware data (same: already a private copy)
        data, tz_info = TimezoneHandler.prepare_timezone_data(data, copy=False)
        metadata["_timezone_info"] = tz_info

        # NOTE: datetime → int64 conversion is deferred to AFTER partitioning
        # so that _determine_partitioning can still detect time-series data
        # and apply time-aligned divisions.  The conversion is applied in
        # the write() method after partitioning.

        # Set index name if empty
        if data.index.name == "":
            data.index.name = "index"

        return data, metadata

    def _determine_partitioning(self, data, npartitions, *, was_datetime_index=False):
        """Determine optimal partitioning strategy for the data."""
        if npartitions is not None:
            # Use provided partitions
            if not isinstance(data, dd.DataFrame):
                data = dd.from_pandas(data, npartitions=npartitions)
            return data, npartitions

        # Use optimized partitioning — check both the current dtype and
        # whether the index *was* datetime before epochdate conversion.
        is_time_series = (
            pd.api.types.is_datetime64_any_dtype(data.index)
            or was_datetime_index
        )

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

        return data, npartitions

    def _write_to_storage(
        self, item, data, metadata, overwrite, reload_items, **kwargs
    ):
        """Write data to parquet and update metadata."""
        dd.to_parquet(
            data,
            self.get_item_path(item, as_string=True),
            overwrite=overwrite,
            compression="snappy",
            engine="pyarrow",
            **kwargs,
        )

        utils.write_metadata(self.get_item_path(item), metadata)

        # update items — lock protects against concurrent modification from
        # write_batch's ThreadPoolExecutor or other threads.
        with self._items_lock:
            self.items.add(item)
        if reload_items:
            self._list_items_threaded()

    def write(
        self,
        item,
        data,
        metadata=None,
        npartitions=None,
        overwrite=False,
        epochdate=False,
        reload_items=False,
        **kwargs,
    ):
        """Write data to a PyStore item.

        Parameters
        ----------
        item : str
            Name of the item to write
        data : pd.DataFrame or Item
            Data to write
        metadata : dict
            Additional metadata to store
        npartitions : int, optional
            Number of partitions
        overwrite : bool, default False
            Whether to overwrite existing item
        epochdate : bool, default False
            Convert datetime to epoch
        reload_items : bool, default False
            Reload item list after write
        """
        if metadata is None:
            metadata = {}
        # Validate and prepare
        self._validate_write_item(item, overwrite)
        data = self._prepare_write_data(data)

        # Capture whether the index is datetime BEFORE transformations
        # convert it to int64 — needed for time-based partitioning below.
        _was_datetime_index = pd.api.types.is_datetime64_any_dtype(data.index)

        # Apply transformations
        data, metadata = self._apply_data_transformations(data, metadata)

        # Determine partitioning
        data, npartitions = self._determine_partitioning(
            data, npartitions, was_datetime_index=_was_datetime_index
        )

        # Convert datetime index to int64 *after* partitioning so that
        # time-based partitioning can still see the original DatetimeIndex.
        # Only convert when epochdate=True; parquet handles datetime natively.
        if epochdate:
            data = utils.datetime_to_int64(data)

        # Write to storage
        self._write_to_storage(item, data, metadata, overwrite, reload_items, **kwargs)

    def _validate_append_item(self, item):
        """Validate that item exists before appending."""
        if not utils.path_exists(self.get_item_path(item)):
            raise ItemNotFoundError(
                f"Item '{item}' does not exist. Use write() to create new items."
            )

    def _prepare_append_data(self, data):
        """Prepare data for appending by copying and setting index name."""
        data = data.copy()
        if data.index.name == "":
            data.index.name = "index"
        return data

    def _handle_schema_evolution(self, item, data, current_df):
        """Handle schema evolution if enabled for the item."""
        if item not in self._schema_evolutions:
            return None, data

        from .schema_evolution import Schema

        evolution = self._schema_evolutions[item]

        # Check and handle schema changes
        old_schema = Schema.from_dataframe(current_df)
        new_schema = Schema.from_dataframe(data)
        if evolution.validate_evolution(old_schema, new_schema):
            # Evolve the existing data to match new schema
            target_schema = evolution.get_target_schema(current_df, data)
            evolved_current_df = evolution.evolve_dataframe(current_df, target_schema)
            data = evolution.evolve_dataframe(data, target_schema)
            return evolved_current_df, data
        return None, data

    def _calculate_partitions(self, combined, npartitions):
        """Calculate optimal number of partitions based on memory usage."""
        if npartitions is None:
            memusage = combined.memory_usage(deep=True).sum()
            if isinstance(combined, dd.DataFrame):
                memusage = memusage.compute()
            npartitions = int(1 + memusage // config.PARTITION_SIZE)
        return npartitions

    def _write_temporary_item(
        self,
        item,
        combined,
        data,
        current,
        npartitions,
        threaded,
        epochdate,
        reload_items,
        **kwargs,
    ):
        """Write combined data to temporary item."""
        tmp_item = "__" + item
        write = self.write_threaded if threaded else self.write

        # Always copy metadata to avoid mutating the Item's internal state
        metadata = current.metadata.copy()
        # Use overwrite=True for temp items — a previous failed append may
        # have left the __-prefixed directory on disk.
        write(
            tmp_item,
            combined,
            npartitions=npartitions,
            metadata=metadata,
            overwrite=True,
            epochdate=epochdate,
            reload_items=reload_items,
            **kwargs,
        )
        return tmp_item

    def _replace_item_with_temporary(self, item, tmp_item):
        """Replace the original item with the temporary item."""
        try:
            self.delete_item(item=item, reload_items=False)
            shutil.move(self.get_item_path(tmp_item), self.get_item_path(item))
            self._list_items_threaded()
        except Exception as errn:
            raise StorageError(f"Failed to replace item '{item}': {errn}") from errn

    def append(
        self,
        item,
        data,
        npartitions=None,
        epochdate=False,
        threaded=False,
        reload_items=False,
        **kwargs,
    ):
        """Append data to an existing item.

        .. note::
            This method materializes the **entire** existing item into memory
            via ``to_pandas()`` in order to combine it with the new data.  For
            very large items this can be extremely memory-intensive.  Consider
            using :meth:`append_stream` for a chunked, memory-efficient
            alternative when dealing with large datasets.

        Parameters
        ----------
        item : str
            Name of the item to append to
        data : pandas.DataFrame
            Data to append
        npartitions : int, optional
            Number of partitions for the data
        epochdate : bool, default False
            Convert datetime to epoch
        threaded : bool, default False
            Use threaded write
        reload_items : bool, default False
            Reload item list after write
        """
        duplicate_handling = kwargs.pop("duplicate_handling", "keep_last")
        validate_schema = kwargs.pop("validate_schema", False)

        if duplicate_handling not in {"keep_last", "keep_first", "keep_all", "error"}:
            raise ValueError(
                f"Unknown duplicate handling strategy: {duplicate_handling}"
            )

        item = utils.validate_identifier(item, "Item")
        with with_lock(self, lock_name=f"append_{item}"):
            # Validate and prepare
            self._validate_append_item(item)
            data = self._prepare_append_data(data)
            if data.empty:
                logger.warning(f"No new data to append to item '{item}'")
                return

            current = self.item(item)
            current_df = current.to_pandas()

            # Validate/align MultiIndex and timezone handling before combining
            if isinstance(current_df.index, pd.MultiIndex) or isinstance(
                data.index, pd.MultiIndex
            ):
                MultiIndexHandler.validate_multiindex_append(current_df, data)

            if (
                getattr(current_df.index, "tz", None) is not None
                or getattr(data.index, "tz", None) is not None
            ):
                target_tz = str(
                    getattr(current_df.index, "tz", None)
                    or getattr(data.index, "tz", None)
                    or "UTC"
                )
                current_df, data = TimezoneHandler.align_timezones(
                    current_df, data, target_tz=target_tz
                )

            # Handle schema evolution
            evolved_current_df, data = self._handle_schema_evolution(
                item, data, current_df
            )

            if validate_schema and evolved_current_df is None:
                self._validate_schema_compatibility(current_df, data)

            base_df = (
                evolved_current_df if evolved_current_df is not None else current_df
            )

            base_has_default_index = (
                not isinstance(base_df.index, pd.MultiIndex)
                and pd.api.types.is_integer_dtype(base_df.index.dtype)
                and base_df.index.equals(
                    pd.Index(range(len(base_df)), name=base_df.index.name)
                )
            )
            data_has_default_index = (
                isinstance(data.index, pd.RangeIndex)
                and data.index.start == 0
                and data.index.step == 1
            )

            if base_has_default_index and data_has_default_index:
                data = data.copy()
                data.index = pd.RangeIndex(
                    start=len(base_df),
                    stop=len(base_df) + len(data),
                    step=1,
                    name=base_df.index.name,
                )

            overlapping_indices = base_df.index.intersection(data.index)

            if len(overlapping_indices) > 0:
                if duplicate_handling == "error":
                    raise DataIntegrityError(
                        f"Found {len(overlapping_indices)} duplicate indices. "
                        "Use duplicate_handling='keep_last' or 'keep_first' to handle them."
                    )
                if duplicate_handling == "keep_first":
                    data = data[~data.index.isin(overlapping_indices)]
                    if data.empty:
                        logger.warning(
                            f"No new data to append to item '{item}' after filtering duplicate indices"
                        )
                        return

            combined = pd.concat([base_df, data], axis=0)

            if duplicate_handling != "keep_all":
                keep = "first" if duplicate_handling == "keep_first" else "last"
                combined = combined[~combined.index.duplicated(keep=keep)]

            try:
                combined = combined.sort_index()
            except TypeError:
                logger.debug(
                    f"Skipping index sort for item '{item}' because the index is not sortable"
                )

            npartitions = self._calculate_partitions(combined, npartitions)

            # Write to temporary item and replace
            tmp_item = self._write_temporary_item(
                item,
                combined,
                data,
                current,
                npartitions,
                threaded,
                epochdate,
                reload_items,
                **kwargs,
            )
            self._replace_item_with_temporary(item, tmp_item)

    def create_snapshot(self, snapshot=None):
        if snapshot is not None:
            snapshot = utils.sanitize_snapshot_name(snapshot)
        else:
            snapshot = str(int(time.time() * 1000000))

        src = utils.make_path(self.datastore, self.collection)
        dst = utils.make_path(src, "_snapshots", snapshot)

        shutil.copytree(src, dst, ignore=shutil.ignore_patterns("_snapshots"))

        self.snapshots = self.list_snapshots()
        return True

    def list_snapshots(self):
        snapshots = utils.subdirs(
            utils.make_path(self.datastore, self.collection, "_snapshots")
        )
        return set(snapshots)

    def delete_snapshot(self, snapshot):
        snapshot_name = utils.validate_identifier(snapshot, "Snapshot")
        if snapshot_name not in self.snapshots:
            raise SnapshotNotFoundError(
                f"Snapshot '{snapshot_name}' doesn't exist"
            )

        shutil.rmtree(
            utils.make_path(
                self.datastore, self.collection, "_snapshots", snapshot_name
            )
        )
        self.snapshots = self.list_snapshots()
        return True

    def delete_snapshots(self):
        snapshots_path = utils.make_path(self.datastore, self.collection, "_snapshots")
        shutil.rmtree(snapshots_path)
        os.makedirs(snapshots_path)
        self.snapshots = self.list_snapshots()
        return True

    def _validate_schema_compatibility(
        self,
        existing_data: Union[pd.DataFrame, dd.DataFrame],
        new_data: Union[pd.DataFrame, dd.DataFrame],
    ) -> None:
        """Validate schema compatibility between existing and new data"""
        existing_columns = set(existing_data.columns)
        new_columns = set(new_data.columns)
        error_lines = ["Schema mismatch detected:"]
        has_mismatch = False

        if existing_columns != new_columns:
            missing_in_new = existing_columns - new_columns
            extra_in_new = new_columns - existing_columns

            if missing_in_new:
                error_lines.append(f"  Missing columns in new data: {missing_in_new}")
                has_mismatch = True
            if extra_in_new:
                error_lines.append(f"  Extra columns in new data: {extra_in_new}")
                has_mismatch = True

        for col in sorted(existing_columns & new_columns):
            existing_dtype = existing_data[col].dtype
            new_dtype = new_data[col].dtype
            if not are_dtypes_compatible(existing_dtype, new_dtype):
                error_lines.append(
                    f"  Dtype mismatch for column '{col}': "
                    f"existing {existing_dtype}, new {new_dtype}"
                )
                has_mismatch = True

        existing_index = existing_data.index
        new_index = new_data.index

        if isinstance(existing_index, pd.MultiIndex) or isinstance(
            new_index, pd.MultiIndex
        ):
            if isinstance(existing_index, pd.MultiIndex) != isinstance(
                new_index, pd.MultiIndex
            ):
                error_lines.append(
                    "  Dtype mismatch for index: existing and new data use "
                    "different index structures"
                )
                has_mismatch = True
            else:
                if existing_index.nlevels != new_index.nlevels:
                    error_lines.append(
                        f"  MultiIndex level count mismatch: "
                        f"existing has {existing_index.nlevels} levels, "
                        f"new has {new_index.nlevels} levels"
                    )
                    has_mismatch = True
                else:
                    existing_multiindex = cast(pd.MultiIndex, existing_index)
                    new_multiindex = cast(pd.MultiIndex, new_index)
                    for level, (existing_dtype, new_dtype) in enumerate(
                        zip(existing_multiindex.dtypes, new_multiindex.dtypes)
                    ):
                        if not are_dtypes_compatible(existing_dtype, new_dtype):
                            level_name = existing_multiindex.names[level]
                            error_lines.append(
                                f"  Dtype mismatch for index level {level_name!r}: "
                                f"existing {existing_dtype}, new {new_dtype}"
                            )
                            has_mismatch = True
        elif not are_dtypes_compatible(existing_index.dtype, new_index.dtype):
            error_lines.append(
                "  Dtype mismatch for index: "
                f"existing {existing_index.dtype}, new {new_index.dtype}"
            )
            has_mismatch = True

        if has_mismatch:
            raise ValidationError("\n".join(error_lines))

    def append_stream(
        self,
        item: str,
        data_iterator,
        chunk_size: int = 10000,
        epochdate: bool = False,
        duplicate_handling: str = "keep_last",
        validate_schema: bool = True,
        reload_items: bool = True,
        flush_every: int = 10,
        **kwargs,
    ) -> None:
        """
        Stream append data to an existing item for memory efficiency.

        Chunks are accumulated in memory and flushed to disk periodically
        (every ``flush_every`` chunks) instead of per-chunk, which avoids
        the O(M*N) materialisation overhead of the previous implementation.

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
        flush_every : int, default 10
            Number of chunks to accumulate before flushing to disk
        **kwargs
            Additional parameters for to_parquet
        """
        logger.info(f"Starting streaming append for item '{item}'")

        # Validate item exists
        if not utils.path_exists(self.get_item_path(item)):
            raise ItemNotFoundError(
                f"Item '{item}' does not exist. Use write() to create new items."
            )

        # Get current item for schema validation
        current_item = self.item(item)
        schema_validated = False
        total_rows_appended = 0
        buffer: list[pd.DataFrame] = []

        def _flush_buffer() -> None:
            """Concatenate buffered chunks and append once."""
            nonlocal buffer
            if not buffer:
                return
            combined = pd.concat(buffer, axis=0)
            self.append(
                item,
                combined,
                epochdate=epochdate,
                duplicate_handling=duplicate_handling,
                validate_schema=False,  # Already validated
                reload_items=False,  # Reload only at end
                **kwargs,
            )
            buffer = []

        try:
            for chunk_num, data_chunk in enumerate(data_iterator):
                if not isinstance(data_chunk, pd.DataFrame):
                    raise ValidationError(
                        f"Expected pandas DataFrame, got {type(data_chunk)}"
                    )

                if data_chunk.empty:
                    logger.debug(f"Skipping empty chunk {chunk_num}")
                    continue

                # Validate schema on first chunk — compare against the
                # *restored* pandas representation, not the raw Dask
                # DataFrame.  For MultiIndex or complex-type items the
                # on-disk columns differ from the pandas-visible columns.
                if validate_schema and not schema_validated:
                    logger.debug("Validating schema compatibility")
                    self._validate_schema_compatibility(
                        current_item.to_pandas(), data_chunk
                    )
                    schema_validated = True

                buffer.append(data_chunk)
                total_rows_appended += len(data_chunk)
                logger.debug(
                    f"Buffered chunk {chunk_num} with {len(data_chunk)} rows "
                    f"(total: {total_rows_appended})"
                )

                # Flush when buffer reaches the threshold
                if len(buffer) >= flush_every:
                    logger.debug(f"Flushing {len(buffer)} buffered chunks to disk")
                    _flush_buffer()

            # Flush any remaining buffered chunks
            _flush_buffer()

            if reload_items:
                self._list_items_threaded()

            logger.info(
                f"Successfully appended {total_rows_appended} total rows to item '{item}'"
            )

        except Exception as e:
            logger.error(f"Error during streaming append: {e}")
            raise

    def write_batch(
        self,
        items_data: dict,
        metadata: Optional[dict] = None,
        npartitions: Optional[dict] = None,
        overwrite: bool = False,
        epochdate: bool = False,
        parallel: bool = True,
        **kwargs,
    ) -> None:
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

        def write_single(item_name, data):
            item_metadata = metadata.get(item_name, {})
            item_npartitions = npartitions.get(item_name, None)
            self.write(
                item_name,
                data,
                metadata=item_metadata,
                npartitions=item_npartitions,
                overwrite=overwrite,
                epochdate=epochdate,
                reload_items=False,
                **kwargs,
            )

        failed_items: list[str] = []
        if parallel and items_data:
            max_workers = min(len(items_data), (os.cpu_count() or 1) + 4)
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=max_workers
            ) as executor:
                futures = {
                    executor.submit(write_single, item_name, data): item_name
                    for item_name, data in items_data.items()
                }
                for future in concurrent.futures.as_completed(futures):
                    item_name = futures[future]
                    try:
                        future.result()
                        logger.debug(f"Successfully wrote item '{item_name}'")
                    except Exception as e:
                        logger.error(f"Failed to write item '{item_name}': {e}")
                        failed_items.append(item_name)
        else:
            for item_name, data in items_data.items():
                try:
                    write_single(item_name, data)
                    logger.debug(f"Successfully wrote item '{item_name}'")
                except Exception as e:
                    logger.error(f"Failed to write item '{item_name}': {e}")
                    failed_items.append(item_name)

        # Reload items once at the end
        self._list_items_threaded()

        success_count = len(items_data) - len(failed_items)
        logger.info(
            f"Batch write completed: {success_count}/{len(items_data)} items written successfully"
        )

        if failed_items:
            raise StorageError(
                f"Batch write partially failed: {len(failed_items)}/{len(items_data)} items failed. "
                f"Failed items: {failed_items}"
            )

    def read_batch(
        self,
        items: list,
        columns: Optional[dict] = None,
        filters: Optional[dict] = None,
    ) -> dict:
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

                item = self.item(item_name, columns=item_columns, filters=item_filters)
                results[item_name] = item.to_pandas()
                logger.debug(f"Successfully read item '{item_name}'")
            except Exception as e:
                logger.error(f"Failed to read item '{item_name}': {e}")
                results[item_name] = None

        successful_reads = sum(1 for v in results.values() if v is not None)
        logger.info(
            f"Batch read completed: {successful_reads}/{len(items)} items read successfully"
        )

        return results
