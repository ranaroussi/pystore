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
Schema evolution support for PyStore
"""

import pandas as pd
from typing import Dict, List, Optional, Union, Callable, Any
from enum import Enum
import json
from datetime import datetime

from . import utils
from .logger import get_logger
from .exceptions import ValidationError, SchemaError

logger = get_logger(__name__)


class EvolutionStrategy(Enum):
    """Schema evolution strategies"""
    STRICT = "strict"  # No schema changes allowed
    ADD_ONLY = "add_only"  # Only allow adding new columns
    COMPATIBLE = "compatible"  # Allow compatible changes (add columns, widen types)
    FLEXIBLE = "flexible"  # Allow most changes with automatic conversion


class SchemaChange:
    """Represents a single schema change"""
    
    def __init__(self, change_type: str, column: str, details: Dict[str, Any]):
        self.change_type = change_type
        self.column = column
        self.details = details
        self.timestamp = datetime.utcnow()
    
    def to_dict(self) -> dict:
        return {
            'change_type': self.change_type,
            'column': self.column,
            'details': self.details,
            'timestamp': self.timestamp.isoformat()
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'SchemaChange':
        change = cls(data['change_type'], data['column'], data['details'])
        change.timestamp = datetime.fromisoformat(data['timestamp'])
        return change


class Schema:
    """Represents a DataFrame schema"""
    
    def __init__(self, columns: List[str], dtypes: Dict[str, str],
                 index_dtype: str, version: int = 1):
        self.columns = columns
        self.dtypes = dtypes
        self.index_dtype = index_dtype
        self.version = version
        self.created_at = datetime.utcnow()
        self.changes = []
    
    def to_dict(self) -> dict:
        return {
            'columns': self.columns,
            'dtypes': self.dtypes,
            'index_dtype': self.index_dtype,
            'version': self.version,
            'created_at': self.created_at.isoformat(),
            'changes': [change.to_dict() for change in self.changes]
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Schema':
        schema = cls(
            data['columns'],
            data['dtypes'],
            data['index_dtype'],
            data['version']
        )
        schema.created_at = datetime.fromisoformat(data['created_at'])
        schema.changes = [SchemaChange.from_dict(c) for c in data.get('changes', [])]
        return schema
    
    @classmethod
    def from_dataframe(cls, df: pd.DataFrame, version: int = 1) -> 'Schema':
        """Create schema from DataFrame"""
        columns = df.columns.tolist()
        dtypes = {col: str(df[col].dtype) for col in columns}
        index_dtype = str(df.index.dtype)
        
        return cls(columns, dtypes, index_dtype, version)
    
    def detect_changes(self, other: 'Schema') -> List[SchemaChange]:
        """Detect changes between this schema and another"""
        changes = []
        
        # Check for added columns
        added_cols = set(other.columns) - set(self.columns)
        for col in added_cols:
            changes.append(SchemaChange(
                'column_added', col,
                {'dtype': other.dtypes[col]}
            ))
        
        # Check for removed columns
        removed_cols = set(self.columns) - set(other.columns)
        for col in removed_cols:
            changes.append(SchemaChange(
                'column_removed', col,
                {'dtype': self.dtypes[col]}
            ))
        
        # Check for type changes
        common_cols = set(self.columns) & set(other.columns)
        for col in common_cols:
            if self.dtypes[col] != other.dtypes[col]:
                changes.append(SchemaChange(
                    'type_changed', col,
                    {'old_dtype': self.dtypes[col], 'new_dtype': other.dtypes[col]}
                ))
        
        # Check for column reordering
        common_cols_list = [col for col in other.columns if col in self.columns]
        self_common_cols = [col for col in self.columns if col in other.columns]
        if common_cols_list != self_common_cols:
            changes.append(SchemaChange(
                'columns_reordered', '',
                {'old_order': self_common_cols, 'new_order': common_cols_list}
            ))
        
        # Check index type change
        if self.index_dtype != other.index_dtype:
            changes.append(SchemaChange(
                'index_type_changed', '',
                {'old_dtype': self.index_dtype, 'new_dtype': other.index_dtype}
            ))
        
        return changes


class SchemaEvolution:
    """Manages schema evolution for a collection item"""
    
    def __init__(self, strategy: EvolutionStrategy = EvolutionStrategy.COMPATIBLE):
        self.strategy = strategy
        self.schemas = {}  # version -> Schema
        self.current_version = 0
        self.migration_functions = {}  # (from_version, to_version) -> function
    
    def register_schema(self, df: pd.DataFrame) -> int:
        """Register a new schema version from DataFrame"""
        self.current_version += 1
        schema = Schema.from_dataframe(df, self.current_version)
        self.schemas[self.current_version] = schema
        logger.info(f"Registered schema version {self.current_version}")
        return self.current_version
    
    def validate_evolution(self, current_schema: Schema, new_schema: Schema) -> bool:
        """Validate if schema evolution is allowed based on strategy"""
        changes = current_schema.detect_changes(new_schema)
        
        if self.strategy == EvolutionStrategy.STRICT:
            return len(changes) == 0
        
        elif self.strategy == EvolutionStrategy.ADD_ONLY:
            # Only allow adding columns
            for change in changes:
                if change.change_type != 'column_added':
                    return False
            return True
        
        elif self.strategy == EvolutionStrategy.COMPATIBLE:
            # Allow adding columns and compatible type changes
            for change in changes:
                if change.change_type == 'column_removed':
                    return False
                elif change.change_type == 'type_changed':
                    # Check if type change is compatible
                    if not self._is_compatible_type_change(
                        change.details['old_dtype'],
                        change.details['new_dtype']
                    ):
                        return False
            return True
        
        elif self.strategy == EvolutionStrategy.FLEXIBLE:
            # Allow most changes
            return True
        
        return False
    
    def _is_compatible_type_change(self, old_dtype: str, new_dtype: str) -> bool:
        """Check if a type change is compatible"""
        compatible_changes = {
            ('int32', 'int64'),
            ('int16', 'int32'),
            ('int16', 'int64'),
            ('float32', 'float64'),
            ('int32', 'float64'),
            ('int64', 'float64'),
        }
        
        # Allow any type to object
        for dtype in ['int32', 'int64', 'float32', 'float64', 'bool']:
            compatible_changes.add((dtype, 'object'))
        
        return (old_dtype, new_dtype) in compatible_changes
    
    def evolve_dataframe(self, df: pd.DataFrame, target_schema: Schema) -> pd.DataFrame:
        """Evolve DataFrame to match target schema"""
        df = df.copy()
        
        # Add missing columns with default values
        for col in target_schema.columns:
            if col not in df.columns:
                # Determine default value based on dtype
                dtype_str = target_schema.dtypes[col]
                if 'int' in dtype_str:
                    df[col] = 0
                elif 'float' in dtype_str:
                    df[col] = 0.0
                elif 'bool' in dtype_str:
                    df[col] = False
                elif 'datetime' in dtype_str:
                    df[col] = pd.NaT
                else:
                    df[col] = None
                
                logger.debug(f"Added column '{col}' with default values")
        
        # Remove extra columns if needed
        extra_cols = set(df.columns) - set(target_schema.columns)
        if extra_cols:
            df = df.drop(columns=list(extra_cols))
            logger.debug(f"Removed columns: {extra_cols}")
        
        # Reorder columns to match schema
        df = df[target_schema.columns]
        
        # Convert types
        for col, dtype_str in target_schema.dtypes.items():
            if col in df.columns:
                try:
                    if 'int' in dtype_str:
                        df[col] = pd.to_numeric(df[col], errors='coerce').astype(dtype_str)
                    elif 'float' in dtype_str:
                        df[col] = pd.to_numeric(df[col], errors='coerce').astype(dtype_str)
                    elif 'datetime' in dtype_str:
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                    elif 'bool' in dtype_str:
                        df[col] = df[col].astype(bool)
                except Exception as e:
                    logger.warning(f"Failed to convert column '{col}' to {dtype_str}: {e}")
        
        return df
    
    def register_migration(self, from_version: int, to_version: int,
                         migration_func: Callable[[pd.DataFrame], pd.DataFrame]):
        """Register a custom migration function between versions"""
        self.migration_functions[(from_version, to_version)] = migration_func
        logger.info(f"Registered migration from version {from_version} to {to_version}")
    
    def migrate(self, df: pd.DataFrame, from_version: int, to_version: int) -> pd.DataFrame:
        """Migrate DataFrame from one version to another"""
        if from_version == to_version:
            return df
        
        # Check for direct migration
        if (from_version, to_version) in self.migration_functions:
            logger.info(f"Applying direct migration from {from_version} to {to_version}")
            return self.migration_functions[(from_version, to_version)](df)
        
        # Try step-by-step migration
        current_version = from_version
        result = df.copy()
        
        while current_version < to_version:
            next_version = current_version + 1
            
            if (current_version, next_version) in self.migration_functions:
                logger.info(f"Applying migration from {current_version} to {next_version}")
                result = self.migration_functions[(current_version, next_version)](result)
            else:
                # Use automatic evolution
                if next_version in self.schemas:
                    result = self.evolve_dataframe(result, self.schemas[next_version])
                else:
                    raise SchemaError(f"No migration path from version {current_version} to {next_version}")
            
            current_version = next_version
        
        return result
    
    def to_dict(self) -> dict:
        """Convert to dictionary for persistence"""
        return {
            'strategy': self.strategy.value,
            'current_version': self.current_version,
            'schemas': {
                str(v): schema.to_dict() for v, schema in self.schemas.items()
            }
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'SchemaEvolution':
        """Create from dictionary"""
        evolution = cls(EvolutionStrategy(data['strategy']))
        evolution.current_version = data['current_version']
        evolution.schemas = {
            int(v): Schema.from_dict(schema_data)
            for v, schema_data in data['schemas'].items()
        }
        return evolution


# Integration functions

def add_schema_evolution_to_collection(collection_class):
    """Add schema evolution support to Collection class"""
    
    def get_item_evolution(self, item: str) -> Optional[SchemaEvolution]:
        """Get schema evolution for an item"""
        metadata_path = utils.make_path(self.datastore, self.collection, item)
        metadata = utils.read_metadata(metadata_path)
        
        if '_schema_evolution' in metadata:
            return SchemaEvolution.from_dict(metadata['_schema_evolution'])
        
        return None
    
    def set_item_evolution(self, item: str, evolution: SchemaEvolution):
        """Set schema evolution for an item"""
        metadata_path = utils.make_path(self.datastore, self.collection, item)
        metadata = utils.read_metadata(metadata_path)
        metadata['_schema_evolution'] = evolution.to_dict()
        utils.write_metadata(metadata_path, metadata)
    
    def enable_schema_evolution(self, item: str, 
                               strategy: EvolutionStrategy = EvolutionStrategy.COMPATIBLE):
        """Enable schema evolution for an item"""
        evolution = SchemaEvolution(strategy)
        
        # Register current schema
        current_df = self.item(item).to_pandas()
        evolution.register_schema(current_df)
        
        self.set_item_evolution(item, evolution)
        logger.info(f"Enabled schema evolution for item '{item}' with strategy '{strategy.value}'")
    
    # Add methods to collection class
    collection_class.get_item_evolution = get_item_evolution
    collection_class.set_item_evolution = set_item_evolution
    collection_class.enable_schema_evolution = enable_schema_evolution
    
    return collection_class


# Example migration functions

def add_calculated_column(df: pd.DataFrame, column_name: str,
                         calculation_func: Callable) -> pd.DataFrame:
    """Migration function to add a calculated column"""
    df[column_name] = calculation_func(df)
    return df


def rename_columns(df: pd.DataFrame, rename_map: Dict[str, str]) -> pd.DataFrame:
    """Migration function to rename columns"""
    return df.rename(columns=rename_map)


def convert_index_to_datetime(df: pd.DataFrame) -> pd.DataFrame:
    """Migration function to convert index to datetime"""
    df.index = pd.to_datetime(df.index)
    return df