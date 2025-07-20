#!/usr/bin/env python
# -*- coding: UTF-8 -*-

"""
Tests for PyStore Phase 4 Feature Enhancements
"""

import pytest
import pandas as pd
import numpy as np
import tempfile
import shutil
import asyncio
from datetime import datetime

import pystore
from pystore import (
    transaction, batch_transaction, async_pystore,
    create_validator, create_timeseries_validator,
    ValidationRule, ColumnExistsRule, RangeRule,
    SchemaEvolution, EvolutionStrategy
)
from pystore.exceptions import ValidationError, TransactionError


class TestAsyncOperations:
    def setup_method(self):
        self.path = tempfile.mkdtemp()
        pystore.set_path(self.path)
        self.store = pystore.store("test_store")
        self.collection = self.store.collection("test_collection")
    
    def teardown_method(self):
        shutil.rmtree(self.path)
    
    @pytest.mark.asyncio
    async def test_async_write_read(self):
        # Create test data
        df = pd.DataFrame({
            'value': np.random.randn(100),
            'timestamp': pd.date_range('2023-01-01', periods=100)
        })
        
        # Async write and read
        async with async_pystore(self.collection) as async_coll:
            await async_coll.write('async_test', df)
            result = await async_coll.read('async_test')
        
        # Verify
        pd.testing.assert_frame_equal(result, df)
    
    @pytest.mark.asyncio
    async def test_async_batch_operations(self):
        # Create multiple DataFrames
        data = {
            f'item_{i}': pd.DataFrame({
                'value': np.random.randn(50),
                'id': i
            }) for i in range(5)
        }
        
        async with async_pystore(self.collection) as async_coll:
            # Batch write
            await async_coll.write_batch(data)
            
            # Batch read
            items = list(data.keys())
            results = await async_coll.read_batch(items)
        
        # Verify all items
        for item, df in data.items():
            assert item in results
            pd.testing.assert_frame_equal(results[item], df)


class TestTransactions:
    def setup_method(self):
        self.path = tempfile.mkdtemp()
        pystore.set_path(self.path)
        self.store = pystore.store("test_store")
        self.collection = self.store.collection("test_collection")
    
    def teardown_method(self):
        shutil.rmtree(self.path)
    
    def test_transaction_commit(self):
        # Create test data
        df1 = pd.DataFrame({'value': [1, 2, 3]})
        df2 = pd.DataFrame({'value': [4, 5, 6]})
        
        # Use transaction
        with transaction(self.collection) as txn:
            txn.write('item1', df1)
            txn.write('item2', df2)
        
        # Verify both writes succeeded
        result1 = self.collection.item('item1').to_pandas()
        result2 = self.collection.item('item2').to_pandas()
        
        pd.testing.assert_frame_equal(result1, df1)
        pd.testing.assert_frame_equal(result2, df2)
    
    def test_transaction_rollback(self):
        # Create initial data
        df_initial = pd.DataFrame({'value': [1, 2, 3]})
        self.collection.write('item_rollback', df_initial)
        
        # Attempt transaction that will fail
        df_new = pd.DataFrame({'value': [4, 5, 6]})
        
        try:
            with transaction(self.collection) as txn:
                txn.write('item_rollback', df_new, overwrite=True)
                # Force an error
                raise ValueError("Simulated error")
        except ValueError:
            pass
        
        # Verify original data is preserved
        result = self.collection.item('item_rollback').to_pandas()
        pd.testing.assert_frame_equal(result, df_initial)
    
    def test_batch_transaction(self):
        # Create multiple items
        data = {f'batch_{i}': pd.DataFrame({'value': [i]}) for i in range(10)}
        
        with batch_transaction(self.collection) as batch:
            for item, df in data.items():
                batch.write(item, df)
        
        # Verify all items written
        for item, df in data.items():
            result = self.collection.item(item).to_pandas()
            pd.testing.assert_frame_equal(result, df)


class TestValidation:
    def setup_method(self):
        self.path = tempfile.mkdtemp()
        pystore.set_path(self.path)
        self.store = pystore.store("test_store")
        self.collection = self.store.collection("test_collection")
    
    def teardown_method(self):
        shutil.rmtree(self.path)
    
    def test_column_validation(self):
        # Create validator
        validator = create_validator()
        validator.add_rule(ColumnExistsRule(['price', 'volume']))
        
        # Valid data
        df_valid = pd.DataFrame({
            'price': [100, 101, 102],
            'volume': [1000, 2000, 3000]
        })
        
        # Should pass
        assert validator.validate(df_valid, raise_on_error=False)
        
        # Invalid data (missing column)
        df_invalid = pd.DataFrame({
            'price': [100, 101, 102]
        })
        
        # Should fail
        assert not validator.validate(df_invalid, raise_on_error=False)
    
    def test_range_validation(self):
        validator = create_validator()
        validator.add_rule(RangeRule('price', min_val=0, max_val=1000))
        
        # Valid data
        df_valid = pd.DataFrame({'price': [100, 200, 300]})
        assert validator.validate(df_valid, raise_on_error=False)
        
        # Invalid data
        df_invalid = pd.DataFrame({'price': [100, 2000, 300]})
        assert not validator.validate(df_invalid, raise_on_error=False)
    
    def test_timeseries_validator(self):
        validator = create_timeseries_validator(['value1', 'value2'])
        
        # Valid time series
        df = pd.DataFrame({
            'value1': [1.0, 2.0, 3.0],
            'value2': [4.0, 5.0, 6.0]
        }, index=pd.date_range('2023-01-01', periods=3))
        
        assert validator.validate(df, raise_on_error=False)


class TestSchemaEvolution:
    def setup_method(self):
        self.path = tempfile.mkdtemp()
        pystore.set_path(self.path)
        self.store = pystore.store("test_store")
        self.collection = self.store.collection("test_collection")
    
    def teardown_method(self):
        shutil.rmtree(self.path)
    
    def test_schema_detection(self):
        # Create initial schema
        df1 = pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': ['a', 'b', 'c']
        })
        
        schema1 = pystore.schema_evolution.Schema.from_dataframe(df1)
        
        # Create modified schema
        df2 = pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': ['a', 'b', 'c'],
            'col3': [4.0, 5.0, 6.0]  # New column
        })
        
        schema2 = pystore.schema_evolution.Schema.from_dataframe(df2)
        
        # Detect changes
        changes = schema1.detect_changes(schema2)
        
        # Should detect one column addition
        assert len(changes) == 1
        assert changes[0].change_type == 'column_added'
        assert changes[0].column == 'col3'
    
    def test_compatible_evolution(self):
        evolution = SchemaEvolution(EvolutionStrategy.COMPATIBLE)
        
        # Initial schema
        df1 = pd.DataFrame({'value': [1, 2, 3]})
        schema1 = pystore.schema_evolution.Schema.from_dataframe(df1)
        
        # Compatible change (add column)
        df2 = pd.DataFrame({
            'value': [1, 2, 3],
            'new_col': [4, 5, 6]
        })
        schema2 = pystore.schema_evolution.Schema.from_dataframe(df2)
        
        # Should be allowed
        assert evolution.validate_evolution(schema1, schema2)
        
        # Incompatible change (remove column)
        df3 = pd.DataFrame({'new_col': [4, 5, 6]})
        schema3 = pystore.schema_evolution.Schema.from_dataframe(df3)
        
        # Should not be allowed
        assert not evolution.validate_evolution(schema1, schema3)
    
    def test_dataframe_evolution(self):
        evolution = SchemaEvolution()
        
        # Target schema
        target_df = pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': ['a', 'b', 'c'],
            'col3': [1.0, 2.0, 3.0]
        })
        target_schema = pystore.schema_evolution.Schema.from_dataframe(target_df)
        
        # DataFrame missing columns
        df = pd.DataFrame({
            'col1': [4, 5, 6]
        })
        
        # Evolve to match target
        evolved_df = evolution.evolve_dataframe(df, target_schema)
        
        # Should have all columns
        assert set(evolved_df.columns) == set(target_schema.columns)
        assert len(evolved_df) == len(df)


class TestTimezoneOperations:
    def setup_method(self):
        self.path = tempfile.mkdtemp()
        pystore.set_path(self.path)
        self.store = pystore.store("test_store")
        self.collection = self.store.collection("test_collection")
    
    def teardown_method(self):
        shutil.rmtree(self.path)
    
    def test_timezone_preservation(self):
        # Create timezone-aware data
        tz = 'US/Eastern'
        df = pd.DataFrame({
            'value': np.random.randn(24)
        }, index=pd.date_range('2023-01-01', periods=24, freq='h', tz=tz))
        
        # Write and read
        self.collection.write('tz_test', df)
        result = self.collection.item('tz_test').to_pandas()
        
        # Verify timezone preserved
        assert result.index.tz is not None
        assert str(result.index.tz) == tz
    
    def test_timezone_aware_columns(self):
        # DataFrame with timezone-aware column
        df = pd.DataFrame({
            'value': [1, 2, 3],
            'timestamp': pd.date_range('2023-01-01', periods=3, tz='UTC')
        })
        
        # Write and read
        self.collection.write('tz_col_test', df)
        result = self.collection.item('tz_col_test').to_pandas()
        
        # Verify column timezone preserved
        assert isinstance(result['timestamp'].dtype, pd.DatetimeTZDtype)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])