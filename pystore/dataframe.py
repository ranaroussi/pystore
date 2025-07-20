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
Advanced DataFrame handling for PyStore including MultiIndex support
"""

import pandas as pd
import numpy as np
import dask.dataframe as dd
import pyarrow as pa
import pyarrow.parquet as pq
from typing import Union, List, Tuple, Optional
import json

from .logger import get_logger
from .exceptions import ValidationError, StorageError

logger = get_logger(__name__)


def prepare_dataframe_for_storage(df: pd.DataFrame) -> Tuple[pd.DataFrame, dict]:
    """
    Prepare DataFrame for storage, handling MultiIndex and special types
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to prepare
    
    Returns
    -------
    pd.DataFrame, dict
        Prepared DataFrame and metadata about transformations
    """
    metadata = {
        'has_multiindex': False,
        'index_names': None,
        'index_dtypes': None,
        'original_columns': list(df.columns),
        'complex_columns': {}
    }
    
    # Handle MultiIndex
    if isinstance(df.index, pd.MultiIndex):
        logger.debug("Converting MultiIndex to columns for storage")
        metadata['has_multiindex'] = True
        metadata['index_names'] = list(df.index.names)
        metadata['index_dtypes'] = [str(df.index.get_level_values(i).dtype) 
                                   for i in range(df.index.nlevels)]
        
        # Reset index to convert MultiIndex to columns
        df = df.reset_index()
    else:
        # Store single index info
        metadata['index_names'] = [df.index.name or 'index']
        metadata['index_dtypes'] = [str(df.index.dtype)]
    
    # Handle complex data types
    for col in df.columns:
        if df[col].dtype == 'object':
            # Check for nested structures
            sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
            
            if sample is not None:
                if isinstance(sample, (list, dict, set)):
                    logger.debug(f"Converting complex column '{col}' to JSON")
                    metadata['complex_columns'][col] = 'json'
                    df[col] = df[col].apply(lambda x: json.dumps(x) if pd.notna(x) else None)
                elif isinstance(sample, pd.DataFrame):
                    logger.debug(f"Converting nested DataFrame column '{col}' to JSON")
                    metadata['complex_columns'][col] = 'dataframe'
                    df[col] = df[col].apply(
                        lambda x: x.to_json() if pd.notna(x) and isinstance(x, pd.DataFrame) else None
                    )
    
    return df, metadata


def restore_dataframe_from_storage(df: pd.DataFrame, metadata: dict) -> pd.DataFrame:
    """
    Restore DataFrame to original structure after reading from storage
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame read from storage
    metadata : dict
        Metadata about transformations
    
    Returns
    -------
    pd.DataFrame
        Restored DataFrame with original structure
    """
    # Restore complex columns
    for col, col_type in metadata.get('complex_columns', {}).items():
        if col in df.columns:
            if col_type == 'json':
                df[col] = df[col].apply(lambda x: json.loads(x) if pd.notna(x) else None)
            elif col_type == 'dataframe':
                df[col] = df[col].apply(
                    lambda x: pd.read_json(x) if pd.notna(x) else None
                )
    
    # Restore MultiIndex if needed
    if metadata.get('has_multiindex', False):
        index_names = metadata['index_names']
        
        # Set MultiIndex
        df = df.set_index(index_names)
        
        # Restore index dtypes if possible
        # Note: Some dtype conversions may not be reversible
    
    return df


class MultiIndexHandler:
    """Handler for MultiIndex operations"""
    
    @staticmethod
    def validate_multiindex_append(existing_df: Union[pd.DataFrame, dd.DataFrame],
                                 new_df: pd.DataFrame) -> None:
        """Validate that MultiIndex structures are compatible for append"""
        if isinstance(existing_df.index, pd.MultiIndex) != isinstance(new_df.index, pd.MultiIndex):
            raise ValidationError("Cannot append single index data to MultiIndex data or vice versa")
        
        if isinstance(existing_df.index, pd.MultiIndex):
            if existing_df.index.nlevels != new_df.index.nlevels:
                raise ValidationError(
                    f"MultiIndex level mismatch: existing has {existing_df.index.nlevels} levels, "
                    f"new has {new_df.index.nlevels} levels"
                )
            
            # Check level names match
            existing_names = existing_df.index.names
            new_names = new_df.index.names
            if existing_names != new_names:
                raise ValidationError(
                    f"MultiIndex names mismatch: existing {existing_names}, new {new_names}"
                )
    
    @staticmethod
    def handle_multiindex_duplicates(df: pd.DataFrame, strategy: str = 'keep_last') -> pd.DataFrame:
        """Handle duplicates in MultiIndex DataFrames"""
        if strategy == 'keep_last':
            return df[~df.index.duplicated(keep='last')]
        elif strategy == 'keep_first':
            return df[~df.index.duplicated(keep='first')]
        elif strategy == 'keep_all':
            return df
        else:
            raise ValueError(f"Unknown duplicate strategy: {strategy}")


class DataTypeHandler:
    """Handler for complex data types"""
    
    # Supported complex types
    COMPLEX_TYPES = {
        'timedelta': pd.Timedelta,
        'period': pd.Period,
        'interval': pd.Interval,
        'category': pd.Categorical
    }
    
    @staticmethod
    def serialize_complex_types(df: pd.DataFrame) -> Tuple[pd.DataFrame, dict]:
        """Serialize complex pandas types for storage"""
        type_info = {}
        df = df.copy()
        
        for col in df.columns:
            dtype = df[col].dtype
            
            if pd.api.types.is_timedelta64_dtype(dtype):
                # Convert timedelta to nanoseconds
                type_info[col] = {'type': 'timedelta', 'unit': 'ns'}
                df[col] = df[col].astype('int64')
                
            elif pd.api.types.is_period_dtype(dtype):
                # Convert period to string representation
                type_info[col] = {'type': 'period', 'freq': dtype.freq.name}
                df[col] = df[col].astype(str)
                
            elif pd.api.types.is_interval_dtype(dtype):
                # Split interval into left/right columns
                type_info[col] = {'type': 'interval', 'closed': dtype.closed}
                df[f'{col}_left'] = df[col].apply(lambda x: x.left if pd.notna(x) else np.nan)
                df[f'{col}_right'] = df[col].apply(lambda x: x.right if pd.notna(x) else np.nan)
                df = df.drop(columns=[col])
                
            elif pd.api.types.is_categorical_dtype(dtype):
                # Store categories separately
                type_info[col] = {
                    'type': 'category',
                    'categories': df[col].cat.categories.tolist(),
                    'ordered': df[col].cat.ordered
                }
                df[col] = df[col].cat.codes
        
        return df, type_info
    
    @staticmethod
    def deserialize_complex_types(df: pd.DataFrame, type_info: dict) -> pd.DataFrame:
        """Deserialize complex pandas types after reading"""
        df = df.copy()
        
        for col, info in type_info.items():
            if info['type'] == 'timedelta':
                df[col] = pd.to_timedelta(df[col], unit=info['unit'])
                
            elif info['type'] == 'period':
                df[col] = pd.PeriodIndex(df[col], freq=info['freq'])
                
            elif info['type'] == 'interval':
                # Reconstruct interval from left/right columns
                left_col = f'{col}_left'
                right_col = f'{col}_right'
                if left_col in df.columns and right_col in df.columns:
                    df[col] = pd.IntervalIndex.from_arrays(
                        df[left_col], df[right_col], closed=info['closed']
                    )
                    df = df.drop(columns=[left_col, right_col])
                    
            elif info['type'] == 'category':
                df[col] = pd.Categorical.from_codes(
                    df[col], categories=info['categories'], ordered=info['ordered']
                )
        
        return df


class TimezoneHandler:
    """Handler for timezone-aware datetime operations"""
    
    @staticmethod
    def prepare_timezone_data(df: pd.DataFrame, target_tz: str = 'UTC') -> Tuple[pd.DataFrame, dict]:
        """
        Prepare timezone-aware data for storage
        
        Parameters
        ----------
        df : pd.DataFrame
            DataFrame with potential timezone-aware columns
        target_tz : str, default 'UTC'
            Target timezone for conversion
        
        Returns
        -------
        pd.DataFrame, dict
            DataFrame with converted times and timezone metadata
        """
        tz_info = {}
        df = df.copy()
        
        # Handle timezone-aware index
        if hasattr(df.index, 'tz') and df.index.tz is not None:
            original_tz = str(df.index.tz)
            tz_info['index_tz'] = original_tz
            logger.debug(f"Converting index from {original_tz} to {target_tz}")
            df.index = df.index.tz_convert(target_tz)
        
        # Handle timezone-aware columns
        for col in df.columns:
            if pd.api.types.is_datetime64tz_dtype(df[col]):
                original_tz = str(df[col].dt.tz)
                tz_info[f'column_{col}_tz'] = original_tz
                logger.debug(f"Converting column '{col}' from {original_tz} to {target_tz}")
                df[col] = df[col].dt.tz_convert(target_tz)
        
        return df, tz_info
    
    @staticmethod
    def restore_timezone_data(df: pd.DataFrame, tz_info: dict) -> pd.DataFrame:
        """
        Restore timezone information after reading
        
        Parameters
        ----------
        df : pd.DataFrame
            DataFrame to restore timezones to
        tz_info : dict
            Timezone metadata
        
        Returns
        -------
        pd.DataFrame
            DataFrame with restored timezone information
        """
        df = df.copy()
        
        # Restore index timezone
        if 'index_tz' in tz_info and pd.api.types.is_datetime64_any_dtype(df.index):
            logger.debug(f"Restoring index timezone to {tz_info['index_tz']}")
            df.index = pd.to_datetime(df.index).tz_localize('UTC').tz_convert(tz_info['index_tz'])
        
        # Restore column timezones
        for key, tz in tz_info.items():
            if key.startswith('column_') and key.endswith('_tz'):
                col = key[7:-3]  # Extract column name
                if col in df.columns and pd.api.types.is_datetime64_any_dtype(df[col]):
                    logger.debug(f"Restoring column '{col}' timezone to {tz}")
                    df[col] = pd.to_datetime(df[col]).dt.tz_localize('UTC').dt.tz_convert(tz)
        
        return df
    
    @staticmethod
    def align_timezones(df1: pd.DataFrame, df2: pd.DataFrame, target_tz: str = 'UTC') -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Align timezones between two DataFrames for operations like append
        
        Parameters
        ----------
        df1 : pd.DataFrame
            First DataFrame
        df2 : pd.DataFrame
            Second DataFrame
        target_tz : str, default 'UTC'
            Target timezone for alignment
        
        Returns
        -------
        pd.DataFrame, pd.DataFrame
            Both DataFrames with aligned timezones
        """
        df1 = df1.copy()
        df2 = df2.copy()
        
        # Align index timezones
        if hasattr(df1.index, 'tz') and hasattr(df2.index, 'tz'):
            if df1.index.tz != df2.index.tz:
                logger.debug(f"Aligning index timezones to {target_tz}")
                if df1.index.tz is not None:
                    df1.index = df1.index.tz_convert(target_tz)
                else:
                    df1.index = df1.index.tz_localize(target_tz)
                    
                if df2.index.tz is not None:
                    df2.index = df2.index.tz_convert(target_tz)
                else:
                    df2.index = df2.index.tz_localize(target_tz)
        
        # Align column timezones
        for col in df1.columns:
            if col in df2.columns:
                if pd.api.types.is_datetime64tz_dtype(df1[col]) or pd.api.types.is_datetime64tz_dtype(df2[col]):
                    logger.debug(f"Aligning column '{col}' timezones to {target_tz}")
                    
                    # Convert df1 column
                    if pd.api.types.is_datetime64tz_dtype(df1[col]):
                        df1[col] = df1[col].dt.tz_convert(target_tz)
                    elif pd.api.types.is_datetime64_any_dtype(df1[col]):
                        df1[col] = pd.to_datetime(df1[col]).dt.tz_localize(target_tz)
                    
                    # Convert df2 column
                    if pd.api.types.is_datetime64tz_dtype(df2[col]):
                        df2[col] = df2[col].dt.tz_convert(target_tz)
                    elif pd.api.types.is_datetime64_any_dtype(df2[col]):
                        df2[col] = pd.to_datetime(df2[col]).dt.tz_localize(target_tz)
        
        return df1, df2


def validate_dataframe_for_storage(df: pd.DataFrame) -> None:
    """
    Validate DataFrame is suitable for storage
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to validate
    
    Raises
    ------
    ValidationError
        If DataFrame has issues that prevent storage
    """
    # Check for duplicate column names
    if df.columns.duplicated().any():
        duplicates = df.columns[df.columns.duplicated()].unique()
        raise ValidationError(f"Duplicate column names found: {list(duplicates)}")
    
    # Check for extremely nested MultiIndex (>5 levels)
    if isinstance(df.index, pd.MultiIndex) and df.index.nlevels > 5:
        logger.warning(f"DataFrame has {df.index.nlevels} index levels. "
                      "This may impact performance.")
    
    # Check for very wide DataFrames
    if len(df.columns) > 1000:
        logger.warning(f"DataFrame has {len(df.columns)} columns. "
                      "Consider using a different data structure.")
    
    # Check for mixed types in columns (can cause issues)
    for col in df.columns:
        if df[col].dtype == 'object':
            types = df[col].dropna().apply(type).unique()
            if len(types) > 1:
                logger.warning(f"Column '{col}' has mixed types: {types}. "
                             "This may cause storage issues.")