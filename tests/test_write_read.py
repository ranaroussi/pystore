"""
Tests for PyStore write and read operations
"""

import pytest
import pandas as pd
import numpy as np
import dask.dataframe as dd
import pystore


class TestWriteRead:
    """Test data write and read operations"""
    
    def test_write_read_basic(self, test_collection, sample_data):
        """Test basic write and read functionality"""
        item_name = 'test_item'
        
        # Write data
        test_collection.write(item_name, sample_data, metadata={'source': 'test'})
        
        # Read data back
        item = test_collection.item(item_name)
        df_read = item.to_pandas()
        
        # Verify data integrity
        pd.testing.assert_frame_equal(df_read, sample_data)
        assert item.metadata['source'] == 'test'
    
    def test_write_overwrite_error(self, test_collection, sample_data):
        """Test error when writing to existing item without overwrite"""
        item_name = 'test_item'
        test_collection.write(item_name, sample_data)
        
        with pytest.raises(pystore.ItemExistsError):
            test_collection.write(item_name, sample_data, overwrite=False)
    
    def test_write_overwrite(self, test_collection, sample_data):
        """Test overwriting an existing item"""
        item_name = 'test_item'
        
        # Write initial data
        test_collection.write(item_name, sample_data)
        
        # Overwrite with new data
        new_data = sample_data * 2
        test_collection.write(item_name, new_data, overwrite=True)
        
        # Verify new data
        item = test_collection.item(item_name)
        df_read = item.to_pandas()
        pd.testing.assert_frame_equal(df_read, new_data)
    
    def test_write_dask_dataframe(self, test_collection, sample_data):
        """Test writing a Dask DataFrame"""
        item_name = 'test_item'
        
        # Convert to Dask DataFrame
        dask_df = dd.from_pandas(sample_data, npartitions=2)
        
        # Write Dask DataFrame
        test_collection.write(item_name, dask_df)
        
        # Read and verify
        item = test_collection.item(item_name)
        df_read = item.to_pandas()
        pd.testing.assert_frame_equal(df_read, sample_data)
    
    def test_write_with_epochdate(self, test_collection):
        """Test writing with epochdate conversion"""
        dates = pd.date_range('2024-01-01', periods=10, freq='D')
        data = pd.DataFrame({'value': range(10)}, index=dates)
        
        test_collection.write('test_item', data, epochdate=True)
        
        # Read back
        item = test_collection.item('test_item')
        df_read = item.to_pandas()
        
        # Index should be datetime after reading
        assert isinstance(df_read.index, pd.DatetimeIndex)
    
    def test_write_nanosecond_precision(self, test_collection, sample_data_nanosecond):
        """Test writing data with nanosecond precision timestamps"""
        # This previously caused TypeError with 'times' parameter
        test_collection.write('test_item', sample_data_nanosecond)
        
        # Read back and verify
        item = test_collection.item('test_item')
        df_read = item.to_pandas()
        
        # Verify data integrity
        assert len(df_read) == len(sample_data_nanosecond)
        assert isinstance(df_read.index, pd.DatetimeIndex)
    
    def test_read_nonexistent_item(self, test_collection):
        """Test reading an item that doesn't exist"""
        with pytest.raises(pystore.ItemNotFoundError):
            test_collection.item('nonexistent')
    
    def test_read_with_filters(self, test_collection):
        """Test reading with filters"""
        # Create data with multiple values
        dates = pd.date_range('2024-01-01', periods=100, freq='D')
        data = pd.DataFrame({
            'value': range(100),
            'category': ['A'] * 50 + ['B'] * 50
        }, index=dates)
        
        test_collection.write('test_item', data)
        
        # Read with filter
        item = test_collection.item('test_item', filters=[('category', '==', 'A')])
        df_filtered = item.to_pandas()
        
        assert len(df_filtered) == 50
        assert all(df_filtered['category'] == 'A')
    
    def test_read_with_columns(self, test_collection, sample_data):
        """Test reading specific columns"""
        test_collection.write('test_item', sample_data)
        
        # Read only specific columns
        item = test_collection.item('test_item', columns=['value1'])
        df_read = item.to_pandas()
        
        assert list(df_read.columns) == ['value1']
        assert len(df_read) == len(sample_data)