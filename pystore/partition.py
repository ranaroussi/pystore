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

import os
import shutil
import tempfile
from typing import Union

import dask.dataframe as dd
import pandas as pd

from . import utils
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

    # Build time-aligned divisions so partition boundaries coincide with
    # calendar period starts (month/quarter/year) rather than arbitrary row counts.
    # Dask requires:
    #   divisions[0]  == source.divisions[0]  (actual data min)
    #   divisions[-1] == source.divisions[-1] (actual data max)
    # Interior boundaries are calendar-aligned period starts.
    period_code = {"monthly": "M", "quarterly": "Q", "yearly": "Y"}.get(freq, "M")
    pandas_freq = {"monthly": "MS", "quarterly": "QS", "yearly": "YS"}.get(freq, "MS")

    try:
        # Strip timezone for Period arithmetic, restore afterwards if needed.
        tz = getattr(min_date, "tzinfo", None)
        min_naive = min_date.tz_localize(None) if tz else min_date
        max_naive = max_date.tz_localize(None) if tz else max_date

        # Period starts from the period containing min_date through the period
        # containing max_date.  These are the candidate interior boundaries.
        start_boundary = min_naive.to_period(period_code).to_timestamp()
        end_boundary = max_naive.to_period(period_code).to_timestamp()
        interior_candidates = pd.date_range(
            start=start_boundary, end=end_boundary, freq=pandas_freq
        )

        # Build the divisions list:
        #   [min_date] + [interior boundaries strictly between min and max] + [max_date]
        # This satisfies Dask's constraint that endpoints match the source.
        divisions_list: list = [min_date]
        for boundary in interior_candidates:
            b = boundary.tz_localize(tz) if tz else boundary
            if b > min_date and b < max_date:
                divisions_list.append(b)
        divisions_list.append(max_date)

        n_partitions = len(divisions_list) - 1

        if not dask_data.known_divisions:
            # repartition(divisions=...) requires known source divisions; fall
            # through to the npartitions fallback with the calendar-derived count.
            raise ValueError("source divisions unknown")

        repartitioned = dask_data.repartition(divisions=divisions_list)
        logger.info(
            f"Repartitioned time series data into {n_partitions} {freq} partitions "
            f"with time-aligned boundaries"
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

    # Read current item as a Dask DataFrame – avoids materialising the full
    # dataset as pandas and allows us to use the time-aligned divisions returned
    # by the optimiser directly.
    item_obj = collection.item(item)
    metadata = item_obj.metadata.copy()

    # Re-read with calculate_divisions=True so Dask knows the sorted boundaries
    # from the parquet row-group statistics.  This is required for
    # repartition(divisions=...) to work without a full data shuffle.
    item_path = collection.get_item_path(item, as_string=True)
    dask_data = dd.read_parquet(item_path, engine="pyarrow", calculate_divisions=True)

    # Check if time series using the Dask index dtype
    is_time_series = pd.api.types.is_datetime64_any_dtype(dask_data.index.dtype)

    # Optimise partitions and keep the returned Dask DataFrame with proper divisions
    if is_time_series and time_based:
        optimized_dask, n_partitions = optimize_time_series_partitions(dask_data)
    else:
        n_partitions = calculate_optimal_partitions(dask_data, target_size_mb)
        n_partitions = max(1, n_partitions)
        optimized_dask = dask_data.repartition(npartitions=n_partitions)

    # Update metadata
    metadata["_partitions"] = n_partitions
    metadata["_partition_strategy"] = (
        "time_based" if (is_time_series and time_based) else "size_based"
    )

    # Write the optimised Dask DataFrame to a temporary location in the same
    # filesystem first.  Dask prohibits reading and writing the same path in one
    # task graph, so we must stage the output and then atomically swap it in.
    collection_dir = str(utils.make_path(collection.datastore, collection.collection))
    final_path = utils.make_path(collection.datastore, collection.collection, item)
    backup_path = utils.make_path(collection.datastore, collection.collection, f"_rebalance_backup_{item}")

    tmp_dir = tempfile.mkdtemp(dir=collection_dir)
    tmp_data_path = os.path.join(tmp_dir, "data")
    try:
        # Write the optimised Dask DataFrame to the temp path
        dd.to_parquet(
            optimized_dask,
            tmp_data_path,
            compression="snappy",
            engine="pyarrow",
            write_metadata_file=True,
        )
        utils.write_metadata(utils.make_path(tmp_data_path), metadata)

        # Atomically swap: backup original → move temp → remove backup
        if utils.path_exists(backup_path):
            shutil.rmtree(str(backup_path))
        shutil.move(str(final_path), str(backup_path))
        shutil.move(tmp_data_path, str(final_path))
        shutil.rmtree(str(backup_path))
    except Exception:
        # Restore original from backup if the swap failed.
        # A partial shutil.move() may have created an incomplete final_path,
        # so always remove it before restoring the backup.
        if utils.path_exists(backup_path):
            try:
                if utils.path_exists(final_path):
                    shutil.rmtree(str(final_path))
            except OSError:
                logger.error(f"Failed to remove incomplete final_path: {final_path}")
            shutil.move(str(backup_path), str(final_path))
        raise
    finally:
        if os.path.exists(tmp_dir):
            shutil.rmtree(tmp_dir)

    logger.info(f"Successfully rebalanced '{item}' with {n_partitions} partitions")
