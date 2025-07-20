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
Data validation hooks for PyStore
"""

import pandas as pd
from typing import Callable, List, Dict, Any, Optional, Union
from functools import wraps

from .logger import get_logger
from .exceptions import ValidationError

logger = get_logger(__name__)


class ValidationRule:
    """Base class for validation rules"""
    
    def __init__(self, name: str, error_message: str = None):
        self.name = name
        self.error_message = error_message or f"Validation rule '{name}' failed"
    
    def validate(self, df: pd.DataFrame) -> bool:
        """Validate DataFrame. Return True if valid, False otherwise."""
        raise NotImplementedError
    
    def get_details(self, df: pd.DataFrame) -> str:
        """Get detailed error information"""
        return self.error_message


class ColumnExistsRule(ValidationRule):
    """Validate that required columns exist"""
    
    def __init__(self, columns: List[str], name: str = "column_exists"):
        super().__init__(name)
        self.columns = columns
    
    def validate(self, df: pd.DataFrame) -> bool:
        missing = set(self.columns) - set(df.columns)
        return len(missing) == 0
    
    def get_details(self, df: pd.DataFrame) -> str:
        missing = set(self.columns) - set(df.columns)
        return f"Missing required columns: {missing}"


class DataTypeRule(ValidationRule):
    """Validate column data types"""
    
    def __init__(self, type_map: Dict[str, type], name: str = "data_type"):
        super().__init__(name)
        self.type_map = type_map
    
    def validate(self, df: pd.DataFrame) -> bool:
        for col, expected_type in self.type_map.items():
            if col not in df.columns:
                continue
            
            # Check numpy/pandas types
            if expected_type == float:
                if not pd.api.types.is_float_dtype(df[col]):
                    return False
            elif expected_type == int:
                if not pd.api.types.is_integer_dtype(df[col]):
                    return False
            elif expected_type == str:
                if not pd.api.types.is_string_dtype(df[col]):
                    return False
            elif expected_type == pd.Timestamp:
                if not pd.api.types.is_datetime64_any_dtype(df[col]):
                    return False
        
        return True
    
    def get_details(self, df: pd.DataFrame) -> str:
        errors = []
        for col, expected_type in self.type_map.items():
            if col in df.columns:
                actual_type = df[col].dtype
                errors.append(f"Column '{col}': expected {expected_type}, got {actual_type}")
        return "; ".join(errors)


class RangeRule(ValidationRule):
    """Validate values are within range"""
    
    def __init__(self, column: str, min_val: float = None, max_val: float = None,
                 name: str = "range"):
        super().__init__(name)
        self.column = column
        self.min_val = min_val
        self.max_val = max_val
    
    def validate(self, df: pd.DataFrame) -> bool:
        if self.column not in df.columns:
            return True
        
        values = df[self.column]
        
        if self.min_val is not None and (values < self.min_val).any():
            return False
        
        if self.max_val is not None and (values > self.max_val).any():
            return False
        
        return True
    
    def get_details(self, df: pd.DataFrame) -> str:
        if self.column not in df.columns:
            return f"Column '{self.column}' not found"
        
        values = df[self.column]
        details = []
        
        if self.min_val is not None:
            below = (values < self.min_val).sum()
            if below > 0:
                details.append(f"{below} values below minimum {self.min_val}")
        
        if self.max_val is not None:
            above = (values > self.max_val).sum()
            if above > 0:
                details.append(f"{above} values above maximum {self.max_val}")
        
        return f"Column '{self.column}': " + ", ".join(details)


class NoNullRule(ValidationRule):
    """Validate no null values in specified columns"""
    
    def __init__(self, columns: List[str], name: str = "no_null"):
        super().__init__(name)
        self.columns = columns
    
    def validate(self, df: pd.DataFrame) -> bool:
        for col in self.columns:
            if col in df.columns and df[col].isnull().any():
                return False
        return True
    
    def get_details(self, df: pd.DataFrame) -> str:
        null_counts = {}
        for col in self.columns:
            if col in df.columns:
                null_count = df[col].isnull().sum()
                if null_count > 0:
                    null_counts[col] = null_count
        
        if null_counts:
            details = [f"'{col}': {count} nulls" for col, count in null_counts.items()]
            return "Null values found in: " + ", ".join(details)
        return ""


class UniqueRule(ValidationRule):
    """Validate values are unique in specified columns"""
    
    def __init__(self, columns: List[str], name: str = "unique"):
        super().__init__(name)
        self.columns = columns
    
    def validate(self, df: pd.DataFrame) -> bool:
        for col in self.columns:
            if col in df.columns and df[col].duplicated().any():
                return False
        return True
    
    def get_details(self, df: pd.DataFrame) -> str:
        dup_counts = {}
        for col in self.columns:
            if col in df.columns:
                dup_count = df[col].duplicated().sum()
                if dup_count > 0:
                    dup_counts[col] = dup_count
        
        if dup_counts:
            details = [f"'{col}': {count} duplicates" for col, count in dup_counts.items()]
            return "Duplicate values found in: " + ", ".join(details)
        return ""


class CustomRule(ValidationRule):
    """Custom validation function"""
    
    def __init__(self, validate_func: Callable[[pd.DataFrame], bool],
                 name: str = "custom", error_message: str = None):
        super().__init__(name, error_message)
        self.validate_func = validate_func
    
    def validate(self, df: pd.DataFrame) -> bool:
        try:
            return self.validate_func(df)
        except Exception as e:
            logger.error(f"Custom validation error: {e}")
            return False


class DataValidator:
    """Manages validation rules for a collection or item"""
    
    def __init__(self):
        self.rules = []
        self.enabled = True
    
    def add_rule(self, rule: ValidationRule):
        """Add a validation rule"""
        self.rules.append(rule)
        logger.debug(f"Added validation rule: {rule.name}")
    
    def remove_rule(self, name: str):
        """Remove a validation rule by name"""
        self.rules = [r for r in self.rules if r.name != name]
        logger.debug(f"Removed validation rule: {name}")
    
    def validate(self, df: pd.DataFrame, raise_on_error: bool = True) -> bool:
        """Validate DataFrame against all rules"""
        if not self.enabled:
            return True
        
        errors = []
        
        for rule in self.rules:
            if not rule.validate(df):
                error_detail = rule.get_details(df)
                errors.append(error_detail)
                logger.warning(f"Validation failed: {error_detail}")
        
        if errors:
            if raise_on_error:
                raise ValidationError("Data validation failed:\n" + "\n".join(errors))
            return False
        
        return True
    
    def disable(self):
        """Temporarily disable validation"""
        self.enabled = False
    
    def enable(self):
        """Re-enable validation"""
        self.enabled = True


def create_validator() -> DataValidator:
    """Create a new data validator"""
    return DataValidator()


def with_validation(validator: DataValidator):
    """Decorator to add validation to collection methods
    
    Usage:
        validator = create_validator()
        validator.add_rule(ColumnExistsRule(['price', 'volume']))
        
        @with_validation(validator)
        def write_data(collection, item, data):
            collection.write(item, data)
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Extract DataFrame from args
            df = None
            for arg in args:
                if isinstance(arg, pd.DataFrame):
                    df = arg
                    break
            
            if df is not None:
                validator.validate(df)
            
            return func(*args, **kwargs)
        
        return wrapper
    return decorator


# Pre-built validators for common use cases

def create_timeseries_validator(value_columns: List[str], 
                              allow_nulls: bool = False) -> DataValidator:
    """Create validator for time series data"""
    validator = DataValidator()
    
    # Ensure required columns exist
    validator.add_rule(ColumnExistsRule(value_columns))
    
    # Ensure no nulls if required
    if not allow_nulls:
        validator.add_rule(NoNullRule(value_columns))
    
    # Ensure numeric types for value columns
    type_map = {col: float for col in value_columns}
    validator.add_rule(DataTypeRule(type_map))
    
    # Ensure index is sorted (custom rule)
    def check_sorted_index(df):
        return df.index.is_monotonic_increasing
    
    validator.add_rule(CustomRule(
        check_sorted_index,
        name="sorted_index",
        error_message="Index must be sorted in ascending order"
    ))
    
    return validator


def create_financial_validator(price_columns: List[str] = None,
                             volume_column: str = 'volume') -> DataValidator:
    """Create validator for financial data"""
    if price_columns is None:
        price_columns = ['open', 'high', 'low', 'close']
    
    validator = DataValidator()
    
    # Required columns
    all_columns = price_columns + [volume_column]
    validator.add_rule(ColumnExistsRule(all_columns))
    
    # No nulls allowed
    validator.add_rule(NoNullRule(all_columns))
    
    # Positive values for prices and volume
    for col in price_columns:
        validator.add_rule(RangeRule(col, min_val=0))
    validator.add_rule(RangeRule(volume_column, min_val=0))
    
    # OHLC relationship validation
    def check_ohlc_relationship(df):
        if all(col in df.columns for col in ['open', 'high', 'low', 'close']):
            # Combine all conditions into a single vectorized operation
            invalid_rows = (
                (df['high'] < df['low']) | 
                (df['high'] < df['open']) | 
                (df['high'] < df['close']) | 
                (df['low'] > df['open']) | 
                (df['low'] > df['close'])
            )
            if invalid_rows.any():
                return False
        return True
    
    validator.add_rule(CustomRule(
        check_ohlc_relationship,
        name="ohlc_relationship",
        error_message="Invalid OHLC relationship: High must be >= all prices, Low must be <= all prices"
    ))
    
    return validator


# Integration with Collection class
def add_validation_to_collection(collection_class):
    """Add validation support to Collection class"""
    
    # Store original methods
    original_write = collection_class.write
    original_append = collection_class.append
    
    def validated_write(self, item, data, metadata={}, validate=True, **kwargs):
        if validate and hasattr(self, '_validator') and self._validator:
            self._validator.validate(data)
        return original_write(self, item, data, metadata, **kwargs)
    
    def validated_append(self, item, data, validate=True, **kwargs):
        if validate and hasattr(self, '_validator') and self._validator:
            self._validator.validate(data)
        return original_append(self, item, data, **kwargs)
    
    def set_validator(self, validator: DataValidator):
        """Set validator for this collection"""
        self._validator = validator
    
    def get_validator(self) -> Optional[DataValidator]:
        """Get current validator"""
        return getattr(self, '_validator', None)
    
    # Monkey patch the methods
    collection_class.write = validated_write
    collection_class.append = validated_append
    collection_class.set_validator = set_validator
    collection_class.get_validator = get_validator
    
    return collection_class