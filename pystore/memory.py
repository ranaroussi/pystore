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
import psutil
import pandas as pd
import numpy as np
import dask
from contextlib import contextmanager
from typing import Generator, Optional
from .logger import get_logger

logger = get_logger(__name__)

# Memory thresholds
MEMORY_WARNING_THRESHOLD = 0.8  # Warn when memory usage exceeds 80%
MEMORY_CRITICAL_THRESHOLD = 0.9  # Take action when memory usage exceeds 90%


def get_memory_info() -> dict:
    """Get current memory usage information"""
    memory = psutil.virtual_memory()
    process = psutil.Process()
    process_memory = process.memory_info()
    
    return {
        'total_gb': memory.total / (1024**3),
        'available_gb': memory.available / (1024**3),
        'used_percent': memory.percent / 100,
        'process_rss_gb': process_memory.rss / (1024**3),
        'process_vms_gb': process_memory.vms / (1024**3)
    }


def check_memory_usage() -> None:
    """Check memory usage and log warnings if needed"""
    info = get_memory_info()
    
    if info['used_percent'] > MEMORY_CRITICAL_THRESHOLD:
        logger.warning(f"Critical memory usage: {info['used_percent']:.1%} "
                      f"({info['available_gb']:.1f} GB available)")
    elif info['used_percent'] > MEMORY_WARNING_THRESHOLD:
        logger.warning(f"High memory usage: {info['used_percent']:.1%} "
                      f"({info['available_gb']:.1f} GB available)")


@contextmanager
def memory_efficient_read(chunk_size: int = 50000):
    """Context manager for memory-efficient operations"""
    # Configure dask for memory efficiency
    original_chunk_size = dask.config.get('dataframe.chunk-size', default='128MB')
    
    try:
        # Set smaller chunk size for memory efficiency
        dask.config.set({'dataframe.chunk-size': f'{chunk_size} rows'})
        
        # Force garbage collection before operation
        gc.collect()
        
        yield
        
    finally:
        # Restore original settings
        dask.config.set({'dataframe.chunk-size': original_chunk_size})
        
        # Force garbage collection after operation
        gc.collect()


def read_in_chunks(collection, item: str, chunk_size: int = 100000,
                  columns: Optional[list] = None) -> Generator[pd.DataFrame, None, None]:
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
    # Get item
    item_obj = collection.item(item, columns=columns)
    dask_df = item_obj.data
    
    # Get total rows
    total_rows = len(dask_df)
    logger.info(f"Reading {total_rows:,} rows in chunks of {chunk_size:,}")
    
    # Read in chunks
    for start_idx in range(0, total_rows, chunk_size):
        check_memory_usage()
        
        end_idx = min(start_idx + chunk_size, total_rows)
        
        # Get chunk
        chunk = dask_df.iloc[start_idx:end_idx].compute()
        
        logger.debug(f"Read chunk {start_idx:,}-{end_idx:,} "
                    f"({len(chunk):,} rows)")
        
        yield chunk
        
        # Explicitly delete chunk to free memory
        del chunk
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
    return df.memory_usage(deep=True).sum() / (1024**3)


def optimize_dataframe_memory(df: pd.DataFrame, 
                             deep: bool = True) -> pd.DataFrame:
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
    
    # Optimize numeric columns
    for col in df.columns:
        col_type = df[col].dtype
        
        if col_type != 'object':
            c_min = df[col].min()
            c_max = df[col].max()
            
            # Integer optimization
            if str(col_type)[:3] == 'int':
                if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                    df[col] = df[col].astype(np.int8)
                elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                    df[col] = df[col].astype(np.int16)
                elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                    df[col] = df[col].astype(np.int32)
            
            # Float optimization
            elif str(col_type)[:5] == 'float':
                if deep and df[col].nunique() < 1000:
                    # Consider converting to category if few unique values
                    df[col] = pd.Categorical(df[col])
                elif c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
                    df[col] = df[col].astype(np.float32)
        
        # Object/string optimization
        elif deep and col_type == 'object':
            num_unique = df[col].nunique()
            num_total = len(df[col])
            
            # Convert to category if less than 50% unique
            if num_unique / num_total < 0.5:
                df[col] = pd.Categorical(df[col])
    
    optimized_memory = estimate_dataframe_memory(df)
    reduction_pct = (1 - optimized_memory / original_memory) * 100
    
    logger.info(f"Memory optimization: {original_memory:.2f} GB -> "
                f"{optimized_memory:.2f} GB ({reduction_pct:.1f}% reduction)")
    
    return df


class MemoryMonitor:
    """Monitor memory usage during operations"""
    
    def __init__(self, warn_threshold: float = MEMORY_WARNING_THRESHOLD):
        self.warn_threshold = warn_threshold
        self.initial_memory = None
    
    def __enter__(self):
        self.initial_memory = get_memory_info()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        final_memory = get_memory_info()
        
        memory_increase = (final_memory['process_rss_gb'] - 
                          self.initial_memory['process_rss_gb'])
        
        if memory_increase > 0.1:  # More than 100 MB increase
            logger.info(f"Memory increased by {memory_increase:.2f} GB during operation")
        
        if final_memory['used_percent'] > self.warn_threshold:
            logger.warning(f"High memory usage after operation: "
                          f"{final_memory['used_percent']:.1%}")
        
        # Force garbage collection
        gc.collect()


# Configure Dask for better memory management
dask.config.set({
    'dataframe.query-planning': True,
    'dataframe.shuffle.method': 'disk',  # Use disk for shuffles to save memory
    'distributed.worker.memory.target': 0.8,  # Spill to disk at 80% memory
    'distributed.worker.memory.spill': 0.9,   # Spill to disk at 90% memory
    'distributed.worker.memory.pause': 0.95,  # Pause at 95% memory
})