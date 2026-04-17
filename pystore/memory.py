#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# PyStore: Flat-file datastore for timeseries data
# https://github.com/ranaroussi/pystore
#
# Copyright 2018-2025 Ran Aroussi
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

"""
Memory management utilities for PyStore
"""

import gc
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any, Optional

import dask
import numpy as np
import pandas as pd

from .logger import get_logger

logger = get_logger(__name__)

try:
    import psutil as _psutil

    _HAS_PSUTIL = True
except ImportError:  # pragma: no cover
    _HAS_PSUTIL = False
    _psutil = None

# Memory thresholds
MEMORY_WARNING_THRESHOLD = 0.8  # Warn when memory usage exceeds 80%
MEMORY_CRITICAL_THRESHOLD = 0.9  # Take action when memory usage exceeds 90%


def get_memory_info() -> dict:
    """Get current memory usage information.

    Returns an empty dict when ``psutil`` is not installed.
    """
    if not _HAS_PSUTIL:
        return {}

    memory = _psutil.virtual_memory()
    process = _psutil.Process()
    process_memory = process.memory_info()

    return {
        "total_gb": memory.total / (1024**3),
        "available_gb": memory.available / (1024**3),
        "used_percent": memory.percent / 100,
        "process_rss_gb": process_memory.rss / (1024**3),
        "process_vms_gb": process_memory.vms / (1024**3),
    }


def check_memory_usage() -> None:
    """Check memory usage and log warnings if needed.

    Silently returns when ``psutil`` is not installed.
    """
    info = get_memory_info()
    if not info:
        return

    if info["used_percent"] > MEMORY_CRITICAL_THRESHOLD:
        logger.warning(
            f"Critical memory usage: {info['used_percent']:.1%} "
            f"({info['available_gb']:.1f} GB available)"
        )
    elif info["used_percent"] > MEMORY_WARNING_THRESHOLD:
        logger.warning(
            f"High memory usage: {info['used_percent']:.1%} "
            f"({info['available_gb']:.1f} GB available)"
        )


@contextmanager
def memory_efficient_read(chunk_size: int = 50000):
    """Context manager for memory-efficient operations"""
    # Configure dask for memory efficiency
    original_chunk_size = dask.config.get("dataframe.chunk-size", default="128MB")

    try:
        # Set smaller chunk size for memory efficiency
        dask.config.set({"dataframe.chunk-size": f"{chunk_size} rows"})

        # Force garbage collection before operation
        gc.collect()

        yield

    finally:
        # Restore original settings
        dask.config.set({"dataframe.chunk-size": original_chunk_size})

        # Force garbage collection after operation
        gc.collect()


def read_in_chunks(
    collection, item: str, chunk_size: int = 100000, columns: Optional[list] = None
) -> Generator[pd.DataFrame, None, None]:
    """
    Read large items in chunks to manage memory

    Parameters
    ----------
    collection : Collection
        PyStore collection
    item : str
        Item name to read
    chunk_size : int, default 100000
        Number of rows per chunk
    columns : list, optional
        Columns to read

    Yields
    ------
    pd.DataFrame
        Chunks of the data
    """
    # Get item as Dask DataFrame (lazy – no full materialisation)
    item_obj = collection.item(item, columns=columns)
    dask_df = item_obj.data

    total_rows = len(dask_df)
    logger.info(f"Reading {total_rows:,} rows in chunks of {chunk_size:,}")

    # Iterate over the *existing* on-disk partitions without repartitioning.
    # Repartitioning before the loop and then computing each derived partition
    # independently would cause Dask to re-read the upstream source partition
    # once per yielded child partition (N re-reads for a single-partition item
    # split into N chunks).  By computing each original partition once and
    # then slicing the result in plain Python we guarantee that every source
    # file is read exactly once, regardless of chunk_size.
    rows_read = 0

    for partition_idx in range(dask_df.npartitions):
        check_memory_usage()

        # Compute only this partition; prior partitions are no longer referenced.
        partition_df = dask_df.get_partition(partition_idx).compute()

        # Slice within the partition to guarantee yielded chunks never exceed
        # chunk_size even if a single on-disk partition is larger.
        for start in range(0, len(partition_df), chunk_size):
            end = min(start + chunk_size, len(partition_df))
            chunk = partition_df.iloc[start:end]

            logger.debug(
                f"Read chunk {rows_read:,}-{rows_read + len(chunk):,} "
                f"({len(chunk):,} rows)"
            )

            yield chunk
            rows_read += len(chunk)
            del chunk

        del partition_df
        gc.collect()


def estimate_dataframe_memory(df: pd.DataFrame) -> float:
    """
    Estimate memory usage of a DataFrame in GB

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to estimate

    Returns
    -------
    float
        Estimated memory usage in GB
    """
    return float(df.memory_usage(deep=True).sum() / (1024**3))


def _optimize_integer_column(
    df: pd.DataFrame, col: str, c_min: float, c_max: float, *, unsigned: bool = False
) -> None:
    """Optimize integer column by downcasting to smallest possible type.

    Parameters
    ----------
    unsigned : bool, default False
        When True, downcast using unsigned integer types (uint8/16/32)
        instead of signed ones.
    """
    if unsigned:
        if c_min >= np.iinfo(np.uint8).min and c_max <= np.iinfo(np.uint8).max:
            df[col] = df[col].astype(np.uint8)
        elif c_min >= np.iinfo(np.uint16).min and c_max <= np.iinfo(np.uint16).max:
            df[col] = df[col].astype(np.uint16)
        elif c_min >= np.iinfo(np.uint32).min and c_max <= np.iinfo(np.uint32).max:
            df[col] = df[col].astype(np.uint32)
    else:
        if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
            df[col] = df[col].astype(np.int8)
        elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
            df[col] = df[col].astype(np.int16)
        elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
            df[col] = df[col].astype(np.int32)


def _optimize_float_column(
    df: pd.DataFrame, col: str, c_min: float, c_max: float, deep: bool
) -> None:
    """Optimize float column by downcasting or converting to category."""
    if deep and df[col].nunique() < 1000:
        # Consider converting to category if few unique values
        df[col] = pd.Categorical(df[col])
    elif c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
        df[col] = df[col].astype(np.float32)


def _optimize_object_column(df: pd.DataFrame, col: str) -> None:
    """Optimize object column by converting to category if appropriate."""
    num_unique = df[col].nunique()
    num_total = len(df[col])

    # Convert to category if less than 50% unique
    if num_unique / num_total < 0.5:
        df[col] = pd.Categorical(df[col])


def _optimize_numeric_column(
    df: pd.DataFrame, col: str, col_type: Any, deep: bool
) -> None:
    """Optimize a numeric column based on its type."""
    c_min = df[col].min()
    c_max = df[col].max()

    # Integer optimization (signed)
    if str(col_type)[:3] == "int":
        _optimize_integer_column(df, col, c_min, c_max)
    # Unsigned integer optimization
    elif str(col_type)[:4] == "uint":
        _optimize_integer_column(df, col, c_min, c_max, unsigned=True)
    # Float optimization
    elif str(col_type)[:5] == "float":
        _optimize_float_column(df, col, c_min, c_max, deep)


def optimize_dataframe_memory(df: pd.DataFrame, deep: bool = True) -> pd.DataFrame:
    """
    Optimize DataFrame memory usage by downcasting types

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to optimize
    deep : bool, default True
        Whether to do deep optimization (may be slower)

    Returns
    -------
    pd.DataFrame
        Optimized DataFrame
    """
    original_memory = estimate_dataframe_memory(df)

    # Optimize each column
    for col in df.columns:
        col_type = df[col].dtype

        if col_type != "object":
            _optimize_numeric_column(df, col, col_type, deep)
        elif deep and col_type == "object":
            _optimize_object_column(df, col)

    optimized_memory = estimate_dataframe_memory(df)
    reduction_pct = (1 - optimized_memory / original_memory) * 100

    logger.info(
        f"Memory optimization: {original_memory:.2f} GB -> "
        f"{optimized_memory:.2f} GB ({reduction_pct:.1f}% reduction)"
    )

    return df


class MemoryMonitor:
    """Monitor memory usage during operations"""

    def __init__(self, warn_threshold: float = MEMORY_WARNING_THRESHOLD):
        self.warn_threshold = warn_threshold
        self.initial_memory: Optional[dict[str, float]] = None

    def __enter__(self):
        self.initial_memory = get_memory_info()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        final_memory = get_memory_info()
        if not self.initial_memory or not final_memory:
            gc.collect()
            return

        memory_increase = (
            final_memory["process_rss_gb"] - self.initial_memory["process_rss_gb"]
        )

        if memory_increase > 0.1:  # More than 100 MB increase
            logger.info(
                f"Memory increased by {memory_increase:.2f} GB during operation"
            )

        if final_memory["used_percent"] > self.warn_threshold:
            logger.warning(
                f"High memory usage after operation: {final_memory['used_percent']:.1%}"
            )

        # Force garbage collection
        gc.collect()


_dask_memory_config_applied = False


def _has_distributed_client() -> bool:
    """Check whether a distributed.Client is currently active."""
    try:
        from distributed import get_client

        get_client()
        return True
    except (ImportError, ValueError):
        return False


def apply_dask_memory_config() -> None:
    """Apply Dask memory management configuration lazily.

    This avoids mutating global Dask configuration at import time, which can
    break downstream code.  Distributed-worker settings are only applied when
    a ``distributed.Client`` is actually active, so they won't interfere with
    a cluster started later.

    Call this explicitly before operations that benefit from the tuned settings.
    """
    global _dask_memory_config_applied
    if _dask_memory_config_applied:
        return

    config: dict[str, object] = {
        "dataframe.query-planning": True,
        "dataframe.shuffle.method": "disk",  # Use disk for shuffles to save memory
    }

    # Only set distributed.worker.memory.* when a distributed cluster is
    # actually active.  Setting these without a cluster is harmless from
    # Dask's perspective but can surprise users who start a cluster later
    # with different memory policies.
    if _has_distributed_client():
        config.update(
            {
                "distributed.worker.memory.target": 0.8,
                "distributed.worker.memory.spill": 0.9,
                "distributed.worker.memory.pause": 0.95,
            }
        )
        logger.debug("Applied distributed worker memory configuration")

    dask.config.set(config)
    _dask_memory_config_applied = True
