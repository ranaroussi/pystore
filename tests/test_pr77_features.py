#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Tests to verify PR #77 features including:
- MultiIndex support
- Complex data types support
- Nested DataFrame support
- Enhanced data validation
- Async operations
- Transactions
- Schema evolution
"""

import os
import shutil
import tempfile
import pytest
import pandas as pd
import numpy as np
import asyncio
import pystore
from datetime import datetime, timedelta


class TestPR77Features:
    """Test all new features from PR #77"""
    
    @pytest.fixture(autouse=True)
    def setup_and_teardown(self):
        """Setup test environment and cleanup after tests"""
        self.test_dir = tempfile.mkdtemp()
        pystore.set_path(self.test_dir)
        self.store = pystore.store('test_store')
        self.collection = self.store.collection('test_collection')
        
        yield
        
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
    
    def test_multiindex_support(self):
        """Test MultiIndex DataFrame support"""
        # Create DataFrame with MultiIndex
        index = pd.MultiIndex.from_product(
            [['A', 'B', 'C'], pd.date_range('2020-01-01', periods=5)],
            names=['category', 'date']
        )
        df = pd.DataFrame({'value': np.random.randn(15)}, index=index)
        
        # Write and read MultiIndex data
        self.collection.write('multiindex_item', df)
        result = self.collection.item('multiindex_item').to_pandas()
        
        # Verify MultiIndex is preserved
        assert isinstance(result.index, pd.MultiIndex)
        assert result.index.names == ['category', 'date']
        assert len(result) == 15
        pd.testing.assert_frame_equal(df, result)
    
    def test_complex_data_types(self):
        """Test support for complex pandas data types"""
        # Create DataFrame with various complex types
        df = pd.DataFrame({
            'duration': pd.to_timedelta(['1 days', '2 days', '3 days']),
            'category': pd.Categorical(['A', 'B', 'A'], categories=['A', 'B', 'C'], ordered=True),
            'period': pd.period_range('2020-01', periods=3, freq='M'),
            'interval': pd.IntervalIndex.from_breaks([0, 1, 3, 5]).to_numpy(),
            'lists': [[1, 2], [3, 4], [5, 6]],
            'dicts': [{'a': 1}, {'b': 2}, {'c': 3}]
        })
        
        # Write and read complex types
        self.collection.write('complex_types', df)
        result = self.collection.item('complex_types').to_pandas()
        
        # Verify timedelta
        assert result['duration'].dtype == 'timedelta64[ns]'
        pd.testing.assert_series_equal(df['duration'], result['duration'])
        
        # Verify categorical
        assert isinstance(result['category'].dtype, pd.CategoricalDtype)
        assert result['category'].cat.ordered == True
        assert list(result['category'].cat.categories) == ['A', 'B', 'C']
        
        # Verify lists and dicts
        assert result['lists'].tolist() == df['lists'].tolist()
        assert result['dicts'].tolist() == df['dicts'].tolist()
    
    def test_nested_dataframe_support(self):
        """Test support for nested DataFrames in columns"""
        # Create DataFrame with nested DataFrames
        nested_df1 = pd.DataFrame({'x': [1, 2], 'y': [3, 4]})
        nested_df2 = pd.DataFrame({'x': [5, 6], 'y': [7, 8]})
        nested_df3 = pd.DataFrame({'x': [9, 10], 'y': [11, 12]})
        
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'nested_data': [nested_df1, nested_df2, nested_df3],
            'value': [100, 200, 300]
        })
        
        # Write and read nested DataFrames
        self.collection.write('nested_df', df)
        result = self.collection.item('nested_df').to_pandas()
        
        # Verify nested DataFrames are preserved
        assert len(result) == 3
        assert result['id'].tolist() == [1, 2, 3]
        assert result['value'].tolist() == [100, 200, 300]
        
        # Check each nested DataFrame
        for i in range(3):
            original_nested = df.iloc[i]['nested_data']
            result_nested = result.iloc[i]['nested_data']
            pd.testing.assert_frame_equal(original_nested, result_nested)
    
    def test_timezone_aware_data(self):
        """Test timezone-aware datetime handling"""
        # Create timezone-aware data
        dates = pd.date_range('2020-01-01', periods=10, tz='US/Eastern')
        df = pd.DataFrame({
            'value': np.random.randn(10),
            'timestamp': dates
        }, index=dates)
        
        # Write and read timezone-aware data
        self.collection.write('tz_aware', df)
        result = self.collection.item('tz_aware').to_pandas()
        
        # Verify timezone is preserved
        assert result.index.tz is not None
        assert str(result.index.tz) == 'US/Eastern'
        assert str(result['timestamp'].dt.tz) == 'US/Eastern'
    
    def test_append_with_duplicate_handling(self):
        """Test enhanced append with duplicate handling"""
        # Create initial data
        dates1 = pd.date_range('2020-01-01', periods=10)
        df1 = pd.DataFrame({'value': range(10)}, index=dates1)
        
        # Write initial data
        self.collection.write('append_test', df1)
        
        # Create overlapping data
        dates2 = pd.date_range('2020-01-08', periods=10)  # Overlaps last 3 days
        df2 = pd.DataFrame({'value': range(100, 110)}, index=dates2)
        
        # Append with duplicate handling
        self.collection.append('append_test', df2)
        
        # Read result
        result = self.collection.item('append_test').to_pandas()
        
        # Verify duplicates were handled (should keep latest values)
        assert len(result) == 17  # 10 original + 10 new - 3 duplicates
        assert result.loc['2020-01-08']['value'] == 100  # Updated value
    
    def test_data_validation(self):
        """Test data validation features"""
        # Test duplicate column names (should raise error)
        df_dup_cols = pd.DataFrame({
            'value': [1, 2, 3],
            'value': [4, 5, 6]  # Duplicate column name
        })
        
        with pytest.raises(Exception):  # Should raise validation error
            self.collection.write('invalid_dup_cols', df_dup_cols)
        
        # Test very wide DataFrame (should work but might warn)
        wide_df = pd.DataFrame(
            np.random.randn(10, 1500),  # 1500 columns
            columns=[f'col_{i}' for i in range(1500)]
        )
        
        # This should succeed but might log a warning
        self.collection.write('wide_df', wide_df)
        result = self.collection.item('wide_df').to_pandas()
        assert result.shape == (10, 1500)
    
    def test_multiindex_with_duplicates(self):
        """Test MultiIndex with duplicate handling"""
        # Create MultiIndex with potential duplicates
        arrays = [
            ['A', 'A', 'B', 'B'],
            pd.date_range('2020-01-01', periods=4)
        ]
        index = pd.MultiIndex.from_arrays(arrays, names=['category', 'date'])
        df = pd.DataFrame({'value': [1, 2, 3, 4]}, index=index)
        
        # Write data
        self.collection.write('multiindex_dup', df)
        
        # Append data with overlapping MultiIndex
        arrays2 = [
            ['A', 'B', 'C'],
            pd.date_range('2020-01-03', periods=3)
        ]
        index2 = pd.MultiIndex.from_arrays(arrays2, names=['category', 'date'])
        df2 = pd.DataFrame({'value': [10, 20, 30]}, index=index2)
        
        self.collection.append('multiindex_dup', df2)
        
        result = self.collection.item('multiindex_dup').to_pandas()
        assert isinstance(result.index, pd.MultiIndex)
    
    def test_mixed_type_columns(self):
        """Test handling of mixed type columns"""
        # Create DataFrame with mixed types in object column
        df = pd.DataFrame({
            'mixed': [1, 'string', 3.14, True, None, {'key': 'value'}, [1, 2, 3]],
            'regular': range(7)
        })
        
        # Write and read
        self.collection.write('mixed_types', df)
        result = self.collection.item('mixed_types').to_pandas()
        
        # Verify mixed types are preserved
        assert result['mixed'].tolist()[1] == 'string'
        assert result['mixed'].tolist()[2] == 3.14
        assert result['mixed'].tolist()[3] == True
        assert result['mixed'].tolist()[5] == {'key': 'value'}
        assert result['mixed'].tolist()[6] == [1, 2, 3]
    
    def test_empty_dataframe(self):
        """Test handling of empty DataFrames"""
        # Empty DataFrame with columns
        df_empty = pd.DataFrame(columns=['A', 'B', 'C'])
        
        self.collection.write('empty_df', df_empty)
        result = self.collection.item('empty_df').to_pandas()
        
        assert len(result) == 0
        assert list(result.columns) == ['A', 'B', 'C']
    
    def test_sparse_data(self):
        """Test handling of sparse data"""
        # Create sparse DataFrame
        df = pd.DataFrame({
            'dense': [1, 2, 3, 4, 5],
            'sparse': pd.arrays.SparseArray([0, 0, 1, 0, 0])
        })
        
        self.collection.write('sparse_data', df)
        result = self.collection.item('sparse_data').to_pandas()
        
        # Verify data is preserved (sparse may be converted to dense)
        assert result['dense'].tolist() == [1, 2, 3, 4, 5]
        assert result['sparse'].tolist() == [0, 0, 1, 0, 0]
    
    @pytest.mark.asyncio
    async def test_async_operations(self):
        """Test async operations support"""
        from pystore.async_operations import AsyncCollection
        
        # Create async collection wrapper
        async_collection = AsyncCollection(self.collection)
        
        # Create test data
        df1 = pd.DataFrame({'value': range(10)}, index=pd.date_range('2020-01-01', periods=10))
        df2 = pd.DataFrame({'value': range(10, 20)}, index=pd.date_range('2020-01-11', periods=10))
        df3 = pd.DataFrame({'value': range(20, 30)}, index=pd.date_range('2020-01-21', periods=10))
        
        # Test async write operations
        await async_collection.write_async('async_item1', df1)
        await async_collection.write_async('async_item2', df2)
        await async_collection.write_async('async_item3', df3)
        
        # Test async read operations
        result1 = await async_collection.read_async('async_item1')
        result2 = await async_collection.read_async('async_item2')
        result3 = await async_collection.read_async('async_item3')
        
        # Verify results
        pd.testing.assert_frame_equal(df1, result1)
        pd.testing.assert_frame_equal(df2, result2)
        pd.testing.assert_frame_equal(df3, result3)
        
        # Test batch async operations
        items = ['async_item1', 'async_item2', 'async_item3']
        results = await async_collection.read_many_async(items)
        assert len(results) == 3
    
    def test_transactions(self):
        """Test transaction support"""
        from pystore.transactions import Transaction
        
        # Create initial data
        df1 = pd.DataFrame({'value': range(10)}, index=pd.date_range('2020-01-01', periods=10))
        self.collection.write('trans_item', df1)
        
        # Test successful transaction
        with Transaction(self.collection) as txn:
            # Write multiple items in transaction
            df2 = pd.DataFrame({'value': range(100, 110)}, index=pd.date_range('2020-01-11', periods=10))
            df3 = pd.DataFrame({'value': range(200, 210)}, index=pd.date_range('2020-01-21', periods=10))
            
            txn.write('trans_item2', df2)
            txn.write('trans_item3', df3)
            txn.append('trans_item', df2)
        
        # Verify all operations were committed
        assert 'trans_item2' in self.collection.list_items()
        assert 'trans_item3' in self.collection.list_items()
        result = self.collection.item('trans_item').to_pandas()
        assert len(result) == 20  # Original 10 + appended 10
        
        # Test failed transaction (rollback)
        try:
            with Transaction(self.collection) as txn:
                df4 = pd.DataFrame({'value': range(300, 310)}, index=pd.date_range('2020-02-01', periods=10))
                txn.write('trans_item4', df4)
                # Simulate error
                raise ValueError("Simulated error")
        except ValueError:
            pass
        
        # Verify transaction was rolled back
        assert 'trans_item4' not in self.collection.list_items()
    
    def test_schema_evolution(self):
        """Test schema evolution support"""
        from pystore.schema_evolution import SchemaEvolution
        
        # Create initial schema
        df_v1 = pd.DataFrame({
            'col1': range(10),
            'col2': range(10, 20)
        }, index=pd.date_range('2020-01-01', periods=10))
        
        self.collection.write('evolving_item', df_v1)
        
        # Create evolved schema (add column)
        df_v2 = pd.DataFrame({
            'col1': range(10, 20),
            'col2': range(20, 30),
            'col3': range(30, 40)  # New column
        }, index=pd.date_range('2020-01-11', periods=10))
        
        # Append with schema evolution
        self.collection.append('evolving_item', df_v2)
        
        # Read and verify schema evolution
        result = self.collection.item('evolving_item').to_pandas()
        assert 'col3' in result.columns
        assert pd.isna(result.iloc[:10]['col3']).all()  # First 10 rows should have NaN for col3
        assert result.iloc[10:]['col3'].tolist() == list(range(30, 40))  # Last 10 rows should have values
    
    def test_partitioning_optimization(self):
        """Test partitioning and optimization features"""
        # Create large dataset
        dates = pd.date_range('2020-01-01', periods=1000, freq='H')
        df_large = pd.DataFrame({
            'value': np.random.randn(1000),
            'category': np.random.choice(['A', 'B', 'C'], 1000)
        }, index=dates)
        
        # Write with partitioning
        self.collection.write('partitioned_item', df_large, partition_on='category')
        
        # Read with partition filtering
        item = self.collection.item('partitioned_item')
        result_filtered = item.to_pandas(filters=[('category', '==', 'A')])
        
        # Verify filtering worked
        assert (result_filtered['category'] == 'A').all()
        assert len(result_filtered) < len(df_large)
    
    def test_memory_management(self):
        """Test memory management features"""
        from pystore.memory import MemoryManager
        
        # Create memory manager
        mem_manager = MemoryManager()
        
        # Set memory limit (in MB)
        mem_manager.set_limit(100)
        
        # Create data that might exceed limit
        large_df = pd.DataFrame(
            np.random.randn(10000, 100),
            columns=[f'col_{i}' for i in range(100)]
        )
        
        # Write with memory management
        with mem_manager.monitor():
            self.collection.write('memory_test', large_df)
        
        # Verify write succeeded
        assert 'memory_test' in self.collection.list_items()
    
    def test_streaming_append(self):
        """Test streaming append functionality"""
        # Create initial data
        df1 = pd.DataFrame({'value': range(10)}, index=pd.date_range('2020-01-01', periods=10))
        self.collection.write('streaming_item', df1)
        
        # Create streaming data chunks
        chunks = []
        for i in range(5):
            start_date = pd.Timestamp('2020-01-11') + pd.Timedelta(days=i*10)
            chunk = pd.DataFrame({
                'value': range(i*10, (i+1)*10)
            }, index=pd.date_range(start_date, periods=10))
            chunks.append(chunk)
        
        # Stream append chunks
        for chunk in chunks:
            self.collection.append('streaming_item', chunk, mode='streaming')
        
        # Verify all data is present
        result = self.collection.item('streaming_item').to_pandas()
        assert len(result) == 60  # 10 initial + 5 chunks of 10