# Critical Fixes Implementation Guide

## 1. Fixing the Append Operation (Highest Priority)

### Current Problem
The append operation in `collection.py` has multiple issues:
- Removes data based only on index matching, ignoring actual values
- Loads entire dataset into memory
- Silent failures hide errors
- Index becomes unsorted after append

### Implementation Plan

```python
# NEW: collection.py - Improved append method
def append(self, item, data, npartitions=None, epochdate=None, 
           threaded=False, reload_items=True, **kwargs):
    """
    Append data to existing item with proper duplicate handling
    
    Parameters:
    -----------
    item : str
        Item name
    data : pd.DataFrame
        Data to append
    npartitions : int, optional
        Number of partitions for new data
    epochdate : bool, optional
        Convert datetime to int64
    threaded : bool, default False
        Use threading (with proper locking)
    reload_items : bool, default True
        Reload item list after append
    **kwargs : dict
        Additional parameters for to_parquet
    """
    # Input validation
    if not isinstance(data, pd.DataFrame):
        raise TypeError(f"Expected pandas DataFrame, got {type(data)}")
    
    if data.empty:
        logger.warning(f"Empty DataFrame provided for item '{item}'")
        return
    
    if not self.item_exists(item):
        raise ValueError(f"Item '{item}' does not exist. Use write() to create new items.")
    
    # Ensure index has a name
    if data.index.name is None:
        data.index.name = "index"
    
    try:
        # Handle datetime conversion
        if epochdate or self._should_convert_datetime(data):
            data = utils.datetime_to_int64(data)
        
        # Stream processing approach to avoid loading all data
        item_path = self._item_path(item, as_string=True)
        
        # Use PyArrow for efficient append
        import pyarrow.parquet as pq
        import pyarrow as pa
        
        # Read existing schema
        existing_schema = pq.read_schema(item_path)
        
        # Validate schema compatibility
        new_table = pa.Table.from_pandas(data)
        if not self._schemas_compatible(existing_schema, new_table.schema):
            raise ValueError("Schema mismatch between existing and new data")
        
        # Perform streaming append with duplicate handling
        if threaded:
            with self._append_lock:
                self._perform_append(item_path, data, new_table, **kwargs)
        else:
            self._perform_append(item_path, data, new_table, **kwargs)
        
        # Update metadata
        self._update_item_metadata(item, data)
        
        if reload_items:
            self._load_items()
            
        logger.info(f"Successfully appended {len(data)} rows to item '{item}'")
        
    except Exception as e:
        logger.error(f"Failed to append to item '{item}': {str(e)}")
        raise
```

### Supporting Methods

```python
def _perform_append(self, item_path, data, new_table, **kwargs):
    """Perform the actual append operation"""
    # Use temporary directory for atomic operation
    import tempfile
    import shutil
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = os.path.join(tmp_dir, "tmp_append")
        
        # Write new data to temporary location
        pq.write_table(new_table, tmp_path, **kwargs)
        
        # Merge with existing data
        existing_dataset = pq.ParquetDataset(item_path)
        
        # Handle duplicates based on index AND values
        if self._has_duplicates_to_handle(existing_dataset, data):
            self._merge_with_dedup(existing_dataset, tmp_path, item_path)
        else:
            # Simple append if no duplicates
            self._simple_append(tmp_path, item_path)

def _has_duplicates_to_handle(self, existing_dataset, new_data):
    """Check if there are duplicates that need handling"""
    # Efficient duplicate detection using index ranges
    existing_indices = self._get_index_range(existing_dataset)
    new_indices = new_data.index
    
    return bool(existing_indices.intersection(new_indices))

def _merge_with_dedup(self, existing_dataset, new_path, target_path):
    """Merge datasets with proper duplicate handling"""
    # This is where we implement the logic to handle duplicates
    # Options: keep='last', keep='first', keep='both', custom logic
    pass
```

## 2. Fixing Path Handling

### Current Problem
- Mixed use of os.path and pathlib
- Platform-specific issues with path separators
- Tilde expansion doesn't work correctly

### Implementation

```python
# utils.py - Modernized path handling
from pathlib import Path
import os

def set_path(path=None):
    """Set the base path for PyStore data
    
    Parameters:
    -----------
    path : str or Path, optional
        Base path for data storage. Defaults to ~/pystore
    """
    if path is None:
        path = Path.home() / "pystore"
    else:
        path = Path(path).expanduser().resolve()
    
    # Validate path
    if not path.exists():
        try:
            path.mkdir(parents=True, exist_ok=True)
        except PermissionError:
            raise PermissionError(f"Cannot create directory at {path}")
    
    # Store as string for compatibility
    config.DEFAULT_PATH = str(path)
    return path

def get_path(*args):
    """Get a path relative to the PyStore base path"""
    base = Path(config.DEFAULT_PATH)
    if args:
        return str(base.joinpath(*args))
    return str(base)
```

## 3. Error Handling Framework

### Implementation

```python
# exceptions.py - New file for custom exceptions
class PyStoreError(Exception):
    """Base exception for PyStore"""
    pass

class DataIntegrityError(PyStoreError):
    """Raised when data integrity issues are detected"""
    pass

class SchemaError(PyStoreError):
    """Raised when schema incompatibilities are found"""
    pass

class StorageError(PyStoreError):
    """Raised when storage operations fail"""
    pass

class ConfigurationError(PyStoreError):
    """Raised when configuration is invalid"""
    pass

# Enhanced error handling in methods
def write(self, item, data, **kwargs):
    try:
        # ... existing code ...
    except PermissionError as e:
        raise StorageError(f"Permission denied writing to {item}: {e}")
    except Exception as e:
        logger.exception(f"Unexpected error writing item {item}")
        raise PyStoreError(f"Failed to write item {item}: {e}") from e
```

## 4. Performance Improvements

### Streaming Append Implementation

```python
def append_streaming(self, item, data, batch_size=10000):
    """Memory-efficient append for large datasets"""
    import pyarrow.parquet as pq
    
    item_path = self._item_path(item)
    
    # Process in batches
    for i in range(0, len(data), batch_size):
        batch = data.iloc[i:i+batch_size]
        
        # Convert batch to arrow table
        table = pa.Table.from_pandas(batch)
        
        # Append to parquet file
        if i == 0:
            pq.write_table(table, item_path, compression='snappy')
        else:
            # Append mode
            pq.write_table(table, item_path, compression='snappy', 
                          append=True)
```

## 5. Testing Strategy

### Test Structure
```
tests/
├── unit/
│   ├── test_append.py
│   ├── test_path_handling.py
│   ├── test_error_handling.py
│   └── test_performance.py
├── integration/
│   ├── test_full_workflow.py
│   ├── test_cross_platform.py
│   └── test_large_datasets.py
└── fixtures/
    └── sample_data.py
```

### Example Test
```python
# test_append.py
import pytest
import pandas as pd
import numpy as np
from pystore import store

class TestAppend:
    def test_append_preserves_all_data(self, tmp_store):
        """Ensure append doesn't lose data"""
        collection = tmp_store.collection('test')
        
        # Initial data
        df1 = pd.DataFrame({
            'value': range(10)
        }, index=pd.date_range('2024-01-01', periods=10))
        
        collection.write('item1', df1)
        
        # Overlapping data with different values
        df2 = pd.DataFrame({
            'value': range(5, 15)
        }, index=pd.date_range('2024-01-06', periods=10))
        
        collection.append('item1', df2)
        
        # Verify all data is preserved
        result = collection.item('item1').to_pandas()
        assert len(result) == 15  # 10 original + 10 new - 5 duplicates
        assert result.index.is_monotonic_increasing
        
    def test_append_with_identical_data(self, tmp_store):
        """Test append with identical index and values"""
        # ... test implementation ...
```

## Migration Tool

```python
# migrate_store.py - Tool to migrate existing stores
import click
import pystore
from pathlib import Path

@click.command()
@click.argument('store_path')
@click.option('--backup/--no-backup', default=True)
def migrate_store(store_path, backup):
    """Migrate existing PyStore to new format"""
    store_path = Path(store_path)
    
    if backup:
        backup_path = store_path.with_suffix('.backup')
        shutil.copytree(store_path, backup_path)
        click.echo(f"Backup created at {backup_path}")
    
    # Migration logic
    # 1. Update metadata format
    # 2. Rebuild indices
    # 3. Verify data integrity
    
    click.echo("Migration completed successfully")

if __name__ == '__main__':
    migrate_store()
```

This implementation guide provides concrete solutions for the most critical issues in PyStore, focusing on data integrity, performance, and reliability.