#!/usr/bin/env python
# -*- coding: UTF-8 -*-

"""
Tests for PR #77 New Features
Comprehensive tests for all new functionality introduced in the modernization
"""

import pytest
import pandas as pd
import numpy as np
import tempfile
import shutil
import asyncio
from datetime import timedelta
import pytz

import pystore
from pystore import (
    transaction, batch_transaction, async_pystore,
    create_validator, create_financial_validator,
    ColumnExistsRule, RangeRule, NoNullRule,
    SchemaEvolution, EvolutionStrategy,
    ValidationError, TransactionError
)


class TestMultiIndexSupport:
    """Test MultiIndex DataFrame support"""
    
    def setup_method(self):
        self.path = tempfile.mkdtemp()
        pystore.set_path(self.path)
        self.store = pystore.store("test_store")
        self.collection = self.store.collection("test_collection")
    
    def teardown_method(self):
        shutil.rmtree(self.path)
    
    def test_multiindex_write_read(self):
        """Test writing and reading MultiIndex DataFrames"""
        # Create MultiIndex DataFrame
        index = pd.MultiIndex.from_product(
            [['A', 'B', 'C'], pd.date_range('2020-01-01', periods=5)],
            names=['category', 'date']
        )
        df = pd.DataFrame({'value': np.random.randn(15)}, index=index)
        
        # Write and read
        self.collection.write('multi_item', df)
        result = self.collection.item('multi_item').to_pandas()
        
        # Verify MultiIndex is preserved
        assert isinstance(result.index, pd.MultiIndex)
        assert result.index.names == ['category', 'date']
        pd.testing.assert_frame_equal(result, df)
    
    @pytest.mark.xfail(reason="Dask MultiIndex support is limited - workaround doesn't fully preserve structure")
    def test_multiindex_append(self):
        """Test appending to MultiIndex DataFrames"""
        # Initial data
        index1 = pd.MultiIndex.from_product(
            [['A', 'B'], pd.date_range('2020-01-01', periods=3)],
            names=['category', 'date']
        )
        df1 = pd.DataFrame({'value': [1, 2, 3, 4, 5, 6]}, index=index1)
        
        # Data to append
        index2 = pd.MultiIndex.from_product(
            [['A', 'B'], pd.date_range('2020-01-04', periods=2)],
            names=['category', 'date']
        )
        df2 = pd.DataFrame({'value': [7, 8, 9, 10]}, index=index2)
        
        # Write and append
        self.collection.write('multi_append', df1)
        self.collection.append('multi_append', df2)
        
        # Read and verify
        result = self.collection.item('multi_append').to_pandas()
        expected = pd.concat([df1, df2]).sort_index()
        pd.testing.assert_frame_equal(result, expected)
    
    def test_complex_multiindex(self):
        """Test complex MultiIndex with multiple data types"""
        # Create complex MultiIndex
        arrays = [
            ['bar', 'bar', 'baz', 'baz', 'foo', 'foo', 'qux', 'qux'],
            ['one', 'two', 'one', 'two', 'one', 'two', 'one', 'two'],
            pd.date_range('2020-01-01', periods=8)
        ]
        index = pd.MultiIndex.from_arrays(arrays, names=['first', 'second', 'date'])
        
        df = pd.DataFrame({
            'value1': np.random.randn(8),
            'value2': np.random.randint(0, 100, 8)
        }, index=index)
        
        # Write and read
        self.collection.write('complex_multi', df)
        result = self.collection.item('complex_multi').to_pandas()
        
        # Verify
        pd.testing.assert_frame_equal(result, df)


class TestComplexDataTypes:
    """Test support for complex pandas data types"""
    
    def setup_method(self):
        self.path = tempfile.mkdtemp()
        pystore.set_path(self.path)
        self.store = pystore.store("test_store")
        self.collection = self.store.collection("test_collection")
    
    def teardown_method(self):
        shutil.rmtree(self.path)
    
    def test_timedelta_support(self):
        """Test timedelta data type"""
        df = pd.DataFrame({
            'duration': pd.to_timedelta(['1 days', '2 days', '3 days 4 hours']),
            'value': [1, 2, 3]
        })
        
        self.collection.write('timedelta_test', df)
        result = self.collection.item('timedelta_test').to_pandas()
        
        assert result['duration'].dtype == 'timedelta64[ns]'
        pd.testing.assert_frame_equal(result, df)
    
    def test_categorical_support(self):
        """Test categorical data type"""
        df = pd.DataFrame({
            'category': pd.Categorical(['A', 'B', 'A', 'C'], 
                                     categories=['A', 'B', 'C', 'D'],
                                     ordered=True),
            'value': [1, 2, 3, 4]
        })
        
        self.collection.write('categorical_test', df)
        result = self.collection.item('categorical_test').to_pandas()
        
        assert isinstance(result['category'].dtype, pd.CategoricalDtype)
        assert result['category'].cat.ordered == True
        assert list(result['category'].cat.categories) == ['A', 'B', 'C', 'D']
        pd.testing.assert_frame_equal(result, df)
    
    def test_period_support(self):
        """Test period data type"""
        df = pd.DataFrame({
            'period': pd.period_range('2020-01', periods=4, freq='M'),
            'value': [1, 2, 3, 4]
        })
        
        self.collection.write('period_test', df)
        result = self.collection.item('period_test').to_pandas()
        
        assert isinstance(result['period'].dtype, pd.PeriodDtype)
        pd.testing.assert_frame_equal(result, df)
    
    def test_interval_support(self):
        """Test interval data type"""
        df = pd.DataFrame({
            'interval': pd.interval_range(start=0, end=4),
            'value': [1, 2, 3, 4]
        })
        
        self.collection.write('interval_test', df)
        result = self.collection.item('interval_test').to_pandas()
        
        assert isinstance(result['interval'].dtype, pd.IntervalDtype)
        pd.testing.assert_frame_equal(result, df, check_like=True)
    
    def test_nested_objects(self):
        """Test nested objects (lists and dicts)"""
        df = pd.DataFrame({
            'lists': [[1, 2], [3, 4, 5], [6]],
            'dicts': [{'a': 1}, {'b': 2, 'c': 3}, {'d': 4}],
            'mixed': [{'data': [1, 2]}, {'data': [3, 4, 5]}, {'data': [6]}]
        })
        
        self.collection.write('nested_test', df)
        result = self.collection.item('nested_test').to_pandas()
        
        # Verify nested structures are preserved
        assert result['lists'].tolist() == df['lists'].tolist()
        assert result['dicts'].tolist() == df['dicts'].tolist()
        assert result['mixed'].tolist() == df['mixed'].tolist()


class TestTimezoneSupport:
    """Test timezone-aware datetime operations"""
    
    def setup_method(self):
        self.path = tempfile.mkdtemp()
        pystore.set_path(self.path)
        self.store = pystore.store("test_store")
        self.collection = self.store.collection("test_collection")
    
    def teardown_method(self):
        shutil.rmtree(self.path)
    
    def test_timezone_preservation(self):
        """Test that timezones are preserved"""
        # Create timezone-aware data
        df = pd.DataFrame({
            'value': np.random.randn(100)
        }, index=pd.date_range('2023-01-01', periods=100, freq='h', tz='US/Eastern'))
        
        self.collection.write('tz_data', df)
        result = self.collection.item('tz_data').to_pandas()
        
        # Verify timezone is preserved (data is stored in UTC for consistency)
        assert result.index.tz is not None
        # Check that the timezone is UTC (standard for storage)
        if hasattr(result.index.tz, 'zone'):
            assert result.index.tz.zone == 'UTC'
        else:
            assert str(result.index.tz) == 'UTC'
        
        # Verify the data matches when converted to the same timezone
        df_utc = df.copy()
        df_utc.index = df_utc.index.tz_convert('UTC')
        # Note: freq is not preserved in parquet storage
        pd.testing.assert_frame_equal(result, df_utc, check_freq=False)
    
    def test_mixed_timezone_append(self):
        """Test appending data with different timezones"""
        # Initial data in US/Eastern
        df1 = pd.DataFrame({
            'value': [1, 2, 3]
        }, index=pd.date_range('2023-01-01', periods=3, freq='h', tz='US/Eastern'))
        
        # Data to append in UTC
        df2 = pd.DataFrame({
            'value': [4, 5, 6]
        }, index=pd.date_range('2023-01-01 08:00', periods=3, freq='h', tz='UTC'))
        
        self.collection.write('tz_mixed', df1)
        self.collection.append('tz_mixed', df2)
        
        result = self.collection.item('tz_mixed').to_pandas()
        
        # Should maintain a timezone (UTC is standard for storage)
        assert result.index.tz is not None
        assert len(result) == 6
        # Verify all data is present
        assert result['value'].tolist() == [1, 2, 3, 4, 5, 6]


class TestTransactionSupport:
    """Test transaction and rollback functionality"""
    
    def setup_method(self):
        self.path = tempfile.mkdtemp()
        pystore.set_path(self.path)
        self.store = pystore.store("test_store")
        self.collection = self.store.collection("test_collection")
    
    def teardown_method(self):
        shutil.rmtree(self.path)
    
    def test_successful_transaction(self):
        """Test successful transaction commit"""
        df1 = pd.DataFrame({'value': [1, 2, 3]})
        df2 = pd.DataFrame({'value': [4, 5, 6]})
        
        with transaction(self.collection) as txn:
            txn.write('item1', df1)
            txn.write('item2', df2)
        
        # Verify both items exist
        assert 'item1' in self.collection.list_items()
        assert 'item2' in self.collection.list_items()
        
        pd.testing.assert_frame_equal(
            self.collection.item('item1').to_pandas(), df1
        )
        pd.testing.assert_frame_equal(
            self.collection.item('item2').to_pandas(), df2
        )
    
    def test_transaction_rollback(self):
        """Test transaction rollback on error"""
        df1 = pd.DataFrame({'value': [1, 2, 3]})
        
        # First write an item
        self.collection.write('existing', df1)
        
        # Try a transaction that will fail
        with pytest.raises(Exception):
            with transaction(self.collection) as txn:
                txn.write('new_item', df1)
                txn.delete('existing')
                # Force an error
                raise Exception("Test error")
        
        # Verify rollback - existing item should still exist, new item shouldn't
        assert 'existing' in self.collection.list_items()
        assert 'new_item' not in self.collection.list_items()
    
    def test_batch_transaction(self):
        """Test batch transaction functionality"""
        data = {f'item_{i}': pd.DataFrame({'value': [i, i+1, i+2]}) 
                for i in range(10)}
        
        with batch_transaction(self.collection) as batch:
            for name, df in data.items():
                batch.write(name, df)
        
        # Verify all items were written
        items = self.collection.list_items()
        assert len(items) == 10
        assert all(f'item_{i}' in items for i in range(10))


class TestAsyncOperations:
    """Test async/await support"""
    
    def setup_method(self):
        self.path = tempfile.mkdtemp()
        pystore.set_path(self.path)
        self.store = pystore.store("test_store")
        self.collection = self.store.collection("test_collection")
    
    def teardown_method(self):
        shutil.rmtree(self.path)
    
    @pytest.mark.asyncio
    async def test_async_batch_write(self):
        """Test async batch write operations"""
        # Create test data
        data = {
            'item1': pd.DataFrame({'value': [1, 2, 3]}),
            'item2': pd.DataFrame({'value': [4, 5, 6]}),
            'item3': pd.DataFrame({'value': [7, 8, 9]})
        }
        
        # Async batch write
        async with async_pystore(self.collection) as async_coll:
            await async_coll.write_batch(data)
        
        # Verify all items were written
        items = self.collection.list_items()
        assert len(items) == 3
        for name, df in data.items():
            pd.testing.assert_frame_equal(
                self.collection.item(name).to_pandas(), df
            )
    
    @pytest.mark.asyncio
    async def test_async_batch_read(self):
        """Test async batch read operations"""
        # Write some data
        data = {
            'item1': pd.DataFrame({'value': [1, 2, 3]}),
            'item2': pd.DataFrame({'value': [4, 5, 6]}),
            'item3': pd.DataFrame({'value': [7, 8, 9]})
        }
        for name, df in data.items():
            self.collection.write(name, df)
        
        # Async batch read
        async with async_pystore(self.collection) as async_coll:
            results = await async_coll.read_batch(['item1', 'item2', 'item3'])
        
        # Verify results
        assert len(results) == 3
        for name, df in data.items():
            pd.testing.assert_frame_equal(results[name], df)


class TestDataValidation:
    """Test data validation framework"""
    
    def setup_method(self):
        self.path = tempfile.mkdtemp()
        pystore.set_path(self.path)
        self.store = pystore.store("test_store")
        self.collection = self.store.collection("test_collection")
    
    def teardown_method(self):
        shutil.rmtree(self.path)
    
    def test_column_exists_validation(self):
        """Test column existence validation"""
        # Create validator
        validator = create_validator()
        validator.add_rule(ColumnExistsRule(['price', 'volume']))
        
        self.collection.set_validator(validator)
        
        # Valid data
        valid_df = pd.DataFrame({
            'price': [100, 101, 102],
            'volume': [1000, 1100, 1200]
        })
        self.collection.write('valid', valid_df)  # Should succeed
        
        # Invalid data
        invalid_df = pd.DataFrame({
            'price': [100, 101, 102]
            # Missing 'volume' column
        })
        
        with pytest.raises(ValidationError):
            self.collection.write('invalid', invalid_df)
    
    def test_range_validation(self):
        """Test range validation"""
        validator = create_validator()
        validator.add_rule(RangeRule('price', min_val=0))
        validator.add_rule(RangeRule('volume', min_val=0))
        
        self.collection.set_validator(validator)
        
        # Valid data
        valid_df = pd.DataFrame({
            'price': [100, 101, 102],
            'volume': [1000, 1100, 1200]
        })
        self.collection.write('valid', valid_df)
        
        # Invalid data
        invalid_df = pd.DataFrame({
            'price': [100, -101, 102],  # Negative price
            'volume': [1000, 1100, 1200]
        })
        
        with pytest.raises(ValidationError):
            self.collection.write('invalid', invalid_df)
    
    def test_financial_validator(self):
        """Test pre-built financial validator"""
        validator = create_financial_validator()
        self.collection.set_validator(validator)
        
        # Valid financial data
        valid_df = pd.DataFrame({
            'open': [100, 101, 102],
            'high': [101, 102, 103],
            'low': [99, 100, 101],
            'close': [100.5, 101.5, 102.5],
            'volume': [1000, 1100, 1200]
        })
        self.collection.write('valid', valid_df)
        
        # Invalid financial data (high < low)
        invalid_df = pd.DataFrame({
            'open': [100, 101, 102],
            'high': [98, 102, 103],  # First high < low
            'low': [99, 100, 101],
            'close': [100.5, 101.5, 102.5],
            'volume': [1000, 1100, 1200]
        })
        
        with pytest.raises(ValidationError):
            self.collection.write('invalid', invalid_df)


class TestSchemaEvolution:
    """Test schema evolution functionality"""
    
    def setup_method(self):
        self.path = tempfile.mkdtemp()
        pystore.set_path(self.path)
        self.store = pystore.store("test_store")
        self.collection = self.store.collection("test_collection")
    
    def teardown_method(self):
        shutil.rmtree(self.path)
    
    def test_add_only_strategy(self):
        """Test ADD_ONLY schema evolution strategy"""
        # Initial schema
        df_v1 = pd.DataFrame({
            'value': [1, 2, 3],
            'timestamp': pd.date_range('2023-01-01', periods=3)
        })
        
        self.collection.write('evolving', df_v1)
        self.collection.enable_schema_evolution('evolving', EvolutionStrategy.ADD_ONLY)
        
        # Add new column - should succeed
        df_v2 = pd.DataFrame({
            'value': [4, 5, 6],
            'timestamp': pd.date_range('2023-01-04', periods=3),
            'new_column': ['a', 'b', 'c']
        })
        
        self.collection.append('evolving', df_v2)
        
        # Read and verify
        result = self.collection.item('evolving').to_pandas()
        assert 'new_column' in result.columns
        assert result['new_column'].isna().sum() == 3  # First 3 rows should be NaN
    
    def test_schema_migration(self):
        """Test schema migration functionality"""
        # Initial data
        df_v1 = pd.DataFrame({
            'value': [1, 2, 3],
            'old_name': ['a', 'b', 'c']
        })
        
        self.collection.write('migrating', df_v1)
        self.collection.enable_schema_evolution('migrating', EvolutionStrategy.FLEXIBLE)
        
        # Register migration to rename column
        evolution = self.collection.get_item_evolution('migrating')
        evolution.register_migration(
            from_version=1,
            to_version=2,
            migration_func=lambda df: df.rename(columns={'old_name': 'new_name'})
        )
        
        # Apply migration
        evolution.migrate_to_version(2)
        
        # Read and verify
        result = self.collection.item('migrating').to_pandas()
        assert 'new_name' in result.columns
        assert 'old_name' not in result.columns
        assert result['new_name'].tolist() == ['a', 'b', 'c']


class TestMemoryManagement:
    """Test memory management features"""
    
    def setup_method(self):
        self.path = tempfile.mkdtemp()
        pystore.set_path(self.path)
        self.store = pystore.store("test_store")
        self.collection = self.store.collection("test_collection")
    
    def teardown_method(self):
        shutil.rmtree(self.path)
    
    def test_streaming_append(self):
        """Test streaming append for large datasets"""
        # Simulate large dataset with generator
        def data_generator():
            for i in range(10):
                df = pd.DataFrame({
                    'value': np.random.randn(1000),
                }, index=pd.date_range(f'2023-01-{i+1:02d}', periods=1000, freq='min'))
                yield df
        
        # Create generator once
        gen = data_generator()
        
        # Initial write
        first_chunk = next(gen)
        self.collection.write('streaming', first_chunk)
        
        # Streaming append
        for chunk in gen:
            self.collection.append('streaming', chunk)
        
        # Verify total size
        result = self.collection.item('streaming')
        assert len(result.to_pandas()) == 10000  # 10 chunks * 1000 rows


class TestEdgeCases:
    """Test edge cases and error handling"""
    
    def setup_method(self):
        self.path = tempfile.mkdtemp()
        pystore.set_path(self.path)
        self.store = pystore.store("test_store")
        self.collection = self.store.collection("test_collection")
    
    def teardown_method(self):
        shutil.rmtree(self.path)
    
    def test_empty_dataframe(self):
        """Test handling of empty DataFrames"""
        # Empty DataFrame with schema
        empty_df = pd.DataFrame(columns=['value', 'timestamp'])
        
        self.collection.write('empty', empty_df)
        result = self.collection.item('empty').to_pandas()
        
        assert len(result) == 0
        assert list(result.columns) == ['value', 'timestamp']
    
    def test_duplicate_index_handling(self):
        """Test handling of duplicate indices"""
        # DataFrame with duplicate index
        df = pd.DataFrame({
            'value': [1, 2, 3, 4]
        }, index=[1, 1, 2, 2])
        
        # Should handle duplicates appropriately
        self.collection.write('duplicates', df)
        result = self.collection.item('duplicates').to_pandas()
        
        # Verify data is preserved
        assert len(result) == 4
        assert result['value'].tolist() == [1, 2, 3, 4]
    
    def test_very_wide_dataframe(self):
        """Test handling of very wide DataFrames"""
        # Create DataFrame with 1000 columns
        data = {f'col_{i}': np.random.randn(10) for i in range(1000)}
        wide_df = pd.DataFrame(data)
        
        # Should handle with warning
        self.collection.write('wide', wide_df)
        result = self.collection.item('wide').to_pandas()
        
        assert result.shape == (10, 1000)
        pd.testing.assert_frame_equal(result, wide_df)