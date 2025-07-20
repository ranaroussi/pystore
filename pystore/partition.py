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

import pandas as pd
import dask.dataframe as dd
from typing import Union, Optional, Tuple
from .logger import get_logger

logger = get_logger(__name__)

# Default partition size targets
DEFAULT_PARTITION_SIZE_MB = 128  # Target size per partition in MB
MIN_PARTITION_SIZE_MB = 32       # Minimum size to avoid too many small files
MAX_PARTITION_SIZE_MB = 512      # Maximum size to avoid memory issues


def calculate_optimal_partitions(data: Union[pd.DataFrame, dd.DataFrame],
                                target_size_mb: float = DEFAULT_PARTITION_SIZE_MB,
                                min_partitions: int = 1,
                                max_partitions: int = 100) -> int:
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
    
    logger.debug(f"Data size: {memory_usage_mb:.1f} MB, "
                f"Optimal partitions: {optimal} "
                f"(target: {target_size_mb} MB/partition)")
    
    return optimal


def optimize_time_series_partitions(data: Union[pd.DataFrame, dd.DataFrame],
                                   freq: str = 'auto',
                                   min_partition_days: int = 30) -> Tuple[dd.DataFrame, int]:
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
    # Convert to dask if needed
    if isinstance(data, pd.DataFrame):
        # Start with basic partitioning
        n_partitions = calculate_optimal_partitions(data)
        dask_data = dd.from_pandas(data, npartitions=n_partitions)
    else:
        dask_data = data
    
    # Get date range
    min_date = dask_data.index.min().compute()
    max_date = dask_data.index.max().compute()
    date_range_days = (max_date - min_date).days
    
    if freq == 'auto':
        # Determine frequency based on data characteristics
        if date_range_days < 365:
            freq = 'monthly'
        elif date_range_days < 365 * 3:
            freq = 'quarterly'
        else:
            freq = 'yearly'
    
    # Repartition based on time boundaries
    if freq == 'monthly':
        # Create monthly partitions
        divisions = pd.date_range(start=min_date.replace(day=1),
                                 end=max_date + pd.Timedelta(days=31),
                                 freq='MS').tolist()
    elif freq == 'quarterly':
        # Create quarterly partitions
        divisions = pd.date_range(start=min_date.replace(day=1),
                                 end=max_date + pd.Timedelta(days=93),
                                 freq='QS').tolist()
    elif freq == 'yearly':
        # Create yearly partitions
        divisions = pd.date_range(start=min_date.replace(month=1, day=1),
                                 end=max_date + pd.Timedelta(days=366),
                                 freq='YS').tolist()
    else:
        # Fall back to size-based partitioning
        return dask_data, dask_data.npartitions
    
    # Ensure divisions cover the data range
    if divisions[0] > min_date:
        divisions.insert(0, min_date - pd.Timedelta(days=1))
    if divisions[-1] < max_date:
        divisions.append(max_date + pd.Timedelta(days=1))
    
    # Remove duplicates and sort
    divisions = sorted(list(set(divisions)))
    
    # Limit number of partitions
    if len(divisions) > 101:  # divisions includes boundaries, so n_partitions = len(divisions) - 1
        # Too many partitions, fall back to size-based
        logger.warning(f"Time-based partitioning would create {len(divisions)-1} partitions, "
                      "falling back to size-based partitioning")
        n_partitions = calculate_optimal_partitions(dask_data)
        return dask_data.repartition(npartitions=n_partitions), n_partitions
    
    try:
        # Repartition with time-based divisions
        repartitioned = dask_data.repartition(divisions=divisions[:-1])
        logger.info(f"Repartitioned time series data into {len(divisions)-1} {freq} partitions")
        return repartitioned, len(divisions) - 1
    except Exception as e:
        logger.warning(f"Time-based repartitioning failed: {e}, "
                      "falling back to size-based partitioning")
        n_partitions = calculate_optimal_partitions(dask_data)
        return dask_data.repartition(npartitions=n_partitions), n_partitions


def rebalance_partitions(collection, item: str, 
                        target_size_mb: float = DEFAULT_PARTITION_SIZE_MB,
                        time_based: bool = True) -> None:
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
    data = item_obj.data
    metadata = item_obj.metadata.copy()
    
    # Check if time series
    is_time_series = pd.api.types.is_datetime64_any_dtype(data.index)
    
    # Optimize partitions
    if is_time_series and time_based:
        optimized_data, n_partitions = optimize_time_series_partitions(data)
    else:
        n_partitions = calculate_optimal_partitions(data, target_size_mb)
        optimized_data = data.repartition(npartitions=n_partitions)
    
    # Update metadata
    metadata['_partitions'] = n_partitions
    metadata['_partition_strategy'] = 'time_based' if (is_time_series and time_based) else 'size_based'
    
    # Rewrite with optimized partitions
    collection.write(item, optimized_data, metadata=metadata, 
                    overwrite=True, npartitions=n_partitions)
    
    logger.info(f"Successfully rebalanced '{item}' with {n_partitions} partitions")