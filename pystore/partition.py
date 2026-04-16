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
Partition optimization strategies for PyStore
"""

from typing import Union

import dask.dataframe as dd
import pandas as pd

from .logger import get_logger

logger = get_logger(__name__)

# Default partition size targets
DEFAULT_PARTITION_SIZE_MB = 128  # Target size per partition in MB
MIN_PARTITION_SIZE_MB = 32  # Minimum size to avoid too many small files
MAX_PARTITION_SIZE_MB = 512  # Maximum size to avoid memory issues


def calculate_optimal_partitions(
    data: Union[pd.DataFrame, dd.DataFrame],
    target_size_mb: float = DEFAULT_PARTITION_SIZE_MB,
    min_partitions: int = 1,
    max_partitions: int = 100,
) -> int:
    """
    Calculate optimal number of partitions based on data size

    Parameters
    ----------
    data : pd.DataFrame or dd.DataFrame
        Data to partition
    target_size_mb : float, default 128
        Target size per partition in MB
    min_partitions : int, default 1
        Minimum number of partitions
    max_partitions : int, default 100
        Maximum number of partitions

    Returns
    -------
    int
        Optimal number of partitions
    """
    # Get memory usage in bytes
    if isinstance(data, pd.DataFrame):
        memory_usage = data.memory_usage(deep=True).sum()
    else:
        # For dask dataframe, compute memory usage
        memory_usage = data.memory_usage(deep=True).sum().compute()

    # Convert to MB
    memory_usage_mb = memory_usage / (1024 * 1024)

    # Calculate optimal partitions
    optimal = max(1, int(memory_usage_mb / target_size_mb))

    # Apply constraints
    optimal = max(min_partitions, min(optimal, max_partitions))

    logger.debug(
        f"Data size: {memory_usage_mb:.1f} MB, "
        f"Optimal partitions: {optimal} "
        f"(target: {target_size_mb} MB/partition)"
    )

    return optimal


def optimize_time_series_partitions(
    data: Union[pd.DataFrame, dd.DataFrame],
    freq: str = "auto",
    min_partition_days: int = 30,
) -> tuple[dd.DataFrame, int]:
    """
    Optimize partitions for time series data

    Parameters
    ----------
    data : pd.DataFrame or dd.DataFrame
        Time series data with datetime index
    freq : str, default 'auto'
        Frequency for partitioning: 'auto', 'monthly', 'quarterly', 'yearly'
    min_partition_days : int, default 30
        Minimum days per partition when using auto

    Returns
    -------
    dd.DataFrame, int
        Repartitioned data and number of partitions
    """
    if isinstance(data, pd.DataFrame):
        dask_data = dd.from_pandas(data.sort_index(), npartitions=1)
    else:
        dask_data = data

    min_date = dask_data.index.min().compute()
    max_date = dask_data.index.max().compute()

    if pd.isna(min_date) or pd.isna(max_date):
        return dask_data, dask_data.npartitions

    date_range_days = max(1, (max_date - min_date).days)

    if freq == "auto":
        if date_range_days < 365:
            freq = "monthly"
        elif date_range_days < 365 * 3:
            freq = "quarterly"
        else:
            freq = "yearly"

    partition_days = {
        "monthly": 30,
        "quarterly": 90,
        "yearly": 365,
    }.get(freq, min_partition_days)

    n_partitions = max(2, min(100, int(date_range_days / max(1, partition_days)) + 1))

    try:
        repartitioned = dask_data.repartition(npartitions=n_partitions)
        logger.info(
            f"Repartitioned time series data into {n_partitions} {freq} partitions"
        )
        return repartitioned, n_partitions
    except Exception as e:
        logger.warning(
            f"Time-based repartitioning failed: {e}, "
            "falling back to size-based partitioning"
        )
        n_partitions = max(1, calculate_optimal_partitions(dask_data))
        return dask_data.repartition(npartitions=n_partitions), n_partitions


def rebalance_partitions(
    collection,
    item: str,
    target_size_mb: float = DEFAULT_PARTITION_SIZE_MB,
    time_based: bool = True,
) -> None:
    """
    Rebalance partitions for an existing item

    Parameters
    ----------
    collection : Collection
        PyStore collection
    item : str
        Item name to rebalance
    target_size_mb : float, default 128
        Target size per partition in MB
    time_based : bool, default True
        Whether to use time-based partitioning for time series data
    """
    logger.info(f"Starting partition rebalancing for item '{item}'")

    # Read current data
    item_obj = collection.item(item)
    data = item_obj.to_pandas()
    metadata = item_obj.metadata.copy()

    # Check if time series
    is_time_series = pd.api.types.is_datetime64_any_dtype(data.index)

    # Optimize partitions
    if is_time_series and time_based:
        _, n_partitions = optimize_time_series_partitions(data)
    else:
        n_partitions = calculate_optimal_partitions(data, target_size_mb)
        n_partitions = max(1, n_partitions)

    # Update metadata
    metadata["_partitions"] = n_partitions
    metadata["_partition_strategy"] = (
        "time_based" if (is_time_series and time_based) else "size_based"
    )

    # Rewrite with optimized partitions
    collection.write(
        item, data, metadata=metadata, overwrite=True, npartitions=n_partitions
    )

    logger.info(f"Successfully rebalanced '{item}' with {n_partitions} partitions")
