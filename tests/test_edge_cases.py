"""
Tests for edge cases and regression tests
"""

import pytest
import pandas as pd
import numpy as np
import pystore
from pathlib import Path


class TestEdgeCases:
    """Test edge cases and specific bug fixes"""
    
    def test_path_with_spaces(self, temp_store_path):
        """Test handling paths with spaces (Windows issue)"""
        # Create path with spaces
        path_with_spaces = temp_store_path / "path with spaces"
        pystore.set_path(path_with_spaces)
        
        # Should work without issues
        store = pystore.store('test_store')
        collection = store.collection('test_collection')
        
        data = pd.DataFrame({'value': [1, 2, 3]})
        collection.write('test_item', data)
        
        # Verify read works
        item = collection.item('test_item')
        assert len(item.to_pandas()) == 3
    
    def test_tilde_expansion(self):
        """Test tilde path expansion (issue #68)"""
        # Set path with tilde
        path = pystore.set_path("~/pystore_test")
        
        # Should expand to home directory
        assert str(path).startswith(str(Path.home()))
        assert "~" not in str(path)
    
    def test_metadata_timestamp_format(self, test_collection, sample_data):
        """Test metadata timestamp uses correct format (issue #64)"""
        test_collection.write('test_item', sample_data)
        
        # Read metadata
        item = test_collection.item('test_item')
        timestamp = item.metadata.get('_updated')
        
        # Should be in correct format: YYYY-MM-DD HH:MM:SS.ffffff
        # Not HH:II which was the bug
        assert timestamp is not None
        parts = timestamp.split(' ')
        assert len(parts) == 2
        time_parts = parts[1].split(':')
        assert len(time_parts) == 3
        # Minutes should be 00-59, not hours in 12-hour format
        minutes = int(time_parts[1])
        assert 0 <= minutes <= 59
    
    def test_empty_dataframe_operations(self, test_collection):
        """Test operations with empty DataFrames"""
        # Write empty DataFrame
        empty_df = pd.DataFrame()
        
        # Should handle empty DataFrame gracefully
        with pytest.raises(Exception):  # May raise various exceptions
            test_collection.write('empty_item', empty_df)
    
    def test_large_column_names(self, test_collection):
        """Test handling of very long column names"""
        # Create DataFrame with very long column names
        long_name = 'a' * 1000
        data = pd.DataFrame({long_name: [1, 2, 3]})
        
        # Should work without issues
        test_collection.write('test_item', data)
        item = test_collection.item('test_item')
        df_read = item.to_pandas()
        
        assert long_name in df_read.columns
    
    def test_special_characters_in_item_name(self, test_collection, sample_data):
        """Test special characters in item names"""
        # Some characters might cause issues on certain filesystems
        special_names = [
            'item_with_underscore',
            'item-with-dash',
            'item.with.dots',
            'UPPERCASE_ITEM',
            '123_numeric_start'
        ]
        
        for name in special_names:
            test_collection.write(name, sample_data)
            item = test_collection.item(name)
            assert len(item.to_pandas()) == len(sample_data)
    
    def test_concurrent_append_simulation(self, test_collection):
        """Simulate concurrent append scenario (not true concurrency)"""
        # Initial data
        initial_data = pd.DataFrame({'value': range(10)})
        test_collection.write('test_item', initial_data)
        
        # Simulate multiple appends that might happen concurrently
        for i in range(5):
            new_data = pd.DataFrame({'value': [100 + i]})
            test_collection.append('test_item', new_data)
        
        # Should have all data
        item = test_collection.item('test_item')
        df_read = item.to_pandas()
        assert len(df_read) == 15
    
    def test_index_name_preservation(self, test_collection):
        """Test that custom index names are preserved"""
        # Create data with custom index name
        data = pd.DataFrame({'value': range(10)})
        data.index.name = 'custom_index_name'
        
        test_collection.write('test_item', data)
        
        # Read back
        item = test_collection.item('test_item')
        df_read = item.to_pandas()
        
        # Index name should be preserved
        assert df_read.index.name == 'custom_index_name'
    
    def test_multicolumn_with_mixed_types(self, test_collection):
        """Test handling of mixed data types across columns"""
        # Create DataFrame with various data types
        data = pd.DataFrame({
            'int_col': [1, 2, 3],
            'float_col': [1.1, 2.2, 3.3],
            'str_col': ['a', 'b', 'c'],
            'bool_col': [True, False, True],
            'datetime_col': pd.date_range('2024-01-01', periods=3)
        })
        
        test_collection.write('test_item', data)
        
        # Read back and verify types are preserved
        item = test_collection.item('test_item')
        df_read = item.to_pandas()
        
        assert df_read['int_col'].dtype in ['int64', 'Int64']
        assert df_read['float_col'].dtype == 'float64'
        assert df_read['str_col'].dtype == 'object'
        assert df_read['bool_col'].dtype == 'bool'
        assert pd.api.types.is_datetime64_any_dtype(df_read['datetime_col'])
    
    def test_append_after_delete_and_recreate(self, test_collection, sample_data):
        """Test append after deleting and recreating an item"""
        item_name = 'test_item'
        
        # Create, delete, recreate
        test_collection.write(item_name, sample_data[:50])
        test_collection.delete_item(item_name)
        test_collection.write(item_name, sample_data[:30])
        
        # Append should work on recreated item
        test_collection.append(item_name, sample_data[50:70])
        
        item = test_collection.item(item_name)
        df_read = item.to_pandas()
        # Should have 30 + 20 = 50 rows
        assert len(df_read) == 50