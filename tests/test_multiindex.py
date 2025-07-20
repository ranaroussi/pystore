#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import pytest
import pandas as pd
import numpy as np
import tempfile
import shutil
import os

import pystore


class TestMultiIndex:
    def setup_method(self):
        self.path = tempfile.mkdtemp()
        pystore.set_path(self.path)
        self.store = pystore.store("test_store")
        self.collection = self.store.collection("test_collection")
    
    def teardown_method(self):
        shutil.rmtree(self.path)
    
    def test_write_read_multiindex(self):
        # Create DataFrame with MultiIndex
        index = pd.MultiIndex.from_product(
            [['A', 'B', 'C'], pd.date_range('2020-01-01', periods=5)],
            names=['category', 'date']
        )
        df = pd.DataFrame({
            'value1': np.random.randn(15),
            'value2': np.random.randn(15)
        }, index=index)
        
        # Write to store
        self.collection.write('multiindex_item', df)
        
        # Read back
        item = self.collection.item('multiindex_item')
        result = item.to_pandas()
        
        # Verify structure is preserved
        assert isinstance(result.index, pd.MultiIndex)
        assert result.index.names == ['category', 'date']
        assert result.index.nlevels == 2
        pd.testing.assert_frame_equal(result, df)
    
    def test_append_multiindex(self):
        # Create initial DataFrame with MultiIndex
        index1 = pd.MultiIndex.from_product(
            [['A', 'B'], pd.date_range('2020-01-01', periods=3)],
            names=['category', 'date']
        )
        df1 = pd.DataFrame({
            'value': np.random.randn(6)
        }, index=index1)
        
        # Write initial data
        self.collection.write('append_test', df1)
        
        # Create new data with same MultiIndex structure
        index2 = pd.MultiIndex.from_product(
            [['A', 'B'], pd.date_range('2020-01-04', periods=3)],
            names=['category', 'date']
        )
        df2 = pd.DataFrame({
            'value': np.random.randn(6)
        }, index=index2)
        
        # Append new data
        self.collection.append('append_test', df2)
        
        # Read back and verify
        result = self.collection.item('append_test').to_pandas()
        assert isinstance(result.index, pd.MultiIndex)
        assert len(result) == 12  # 6 + 6 rows
        assert result.index.names == ['category', 'date']
    
    def test_multiindex_with_duplicates(self):
        # Create DataFrame with MultiIndex and duplicates
        index = pd.MultiIndex.from_tuples([
            ('A', '2020-01-01'),
            ('A', '2020-01-02'),
            ('B', '2020-01-01'),
            ('B', '2020-01-01'),  # Duplicate
            ('B', '2020-01-02')
        ], names=['category', 'date'])
        
        df = pd.DataFrame({
            'value': [1, 2, 3, 4, 5]
        }, index=index)
        
        # Write with duplicates
        self.collection.write('dup_test', df)
        
        # Read back
        result = self.collection.item('dup_test').to_pandas()
        assert isinstance(result.index, pd.MultiIndex)
        assert len(result) == 5  # All rows preserved
    
    def test_multiindex_3_levels(self):
        # Create DataFrame with 3-level MultiIndex
        index = pd.MultiIndex.from_product(
            [['X', 'Y'], ['A', 'B'], pd.date_range('2020-01-01', periods=2)],
            names=['group', 'category', 'date']
        )
        df = pd.DataFrame({
            'value': np.random.randn(8)
        }, index=index)
        
        # Write and read
        self.collection.write('three_level', df)
        result = self.collection.item('three_level').to_pandas()
        
        # Verify
        assert result.index.nlevels == 3
        assert result.index.names == ['group', 'category', 'date']
        pd.testing.assert_frame_equal(result, df)
    
    def test_mixed_type_multiindex(self):
        # Create MultiIndex with mixed types
        index = pd.MultiIndex.from_tuples([
            (1, 'A', pd.Timestamp('2020-01-01')),
            (1, 'B', pd.Timestamp('2020-01-02')),
            (2, 'A', pd.Timestamp('2020-01-01')),
            (2, 'B', pd.Timestamp('2020-01-02'))
        ], names=['id', 'category', 'date'])
        
        df = pd.DataFrame({
            'value': [10, 20, 30, 40]
        }, index=index)
        
        # Write and read
        self.collection.write('mixed_types', df)
        result = self.collection.item('mixed_types').to_pandas()
        
        # Verify structure
        assert isinstance(result.index, pd.MultiIndex)
        assert result.index.names == ['id', 'category', 'date']
        pd.testing.assert_frame_equal(result, df)


class TestComplexDataTypes:
    def setup_method(self):
        self.path = tempfile.mkdtemp()
        pystore.set_path(self.path)
        self.store = pystore.store("test_store")
        self.collection = self.store.collection("test_collection")
    
    def teardown_method(self):
        shutil.rmtree(self.path)
    
    def test_timedelta_column(self):
        # Create DataFrame with timedelta column
        df = pd.DataFrame({
            'duration': pd.to_timedelta(['1 days', '2 days', '3 days']),
            'value': [1, 2, 3]
        })
        
        # Write and read
        self.collection.write('timedelta_test', df)
        result = self.collection.item('timedelta_test').to_pandas()
        
        # Verify timedelta is preserved
        assert pd.api.types.is_timedelta64_dtype(result['duration'])
        pd.testing.assert_frame_equal(result, df)
    
    def test_categorical_column(self):
        # Create DataFrame with categorical column
        df = pd.DataFrame({
            'category': pd.Categorical(['A', 'B', 'A', 'C'], categories=['A', 'B', 'C'], ordered=True),
            'value': [1, 2, 3, 4]
        })
        
        # Write and read
        self.collection.write('categorical_test', df)
        result = self.collection.item('categorical_test').to_pandas()
        
        # Verify categorical is preserved
        assert pd.api.types.is_categorical_dtype(result['category'])
        assert result['category'].cat.categories.tolist() == ['A', 'B', 'C']
        assert result['category'].cat.ordered == True
        pd.testing.assert_frame_equal(result, df)
    
    def test_interval_column(self):
        # Create DataFrame with interval column
        intervals = pd.IntervalIndex.from_tuples([(0, 1), (1, 2), (2, 3)], closed='right')
        df = pd.DataFrame({
            'interval': intervals,
            'value': [10, 20, 30]
        })
        
        # Write and read
        self.collection.write('interval_test', df)
        result = self.collection.item('interval_test').to_pandas()
        
        # Verify interval is preserved
        assert pd.api.types.is_interval_dtype(result['interval'])
        assert all(result['interval'] == df['interval'])
    
    def test_period_column(self):
        # Create DataFrame with period column
        df = pd.DataFrame({
            'period': pd.period_range('2020-01', periods=3, freq='M'),
            'value': [100, 200, 300]
        })
        
        # Write and read
        self.collection.write('period_test', df)
        result = self.collection.item('period_test').to_pandas()
        
        # Verify period is preserved
        assert pd.api.types.is_period_dtype(result['period'])
        assert all(result['period'] == df['period'])
    
    def test_mixed_complex_types(self):
        # Create DataFrame with multiple complex types
        df = pd.DataFrame({
            'timedelta': pd.to_timedelta(['1 days', '2 days']),
            'category': pd.Categorical(['X', 'Y']),
            'value': [1.1, 2.2]
        })
        
        # Write and read
        self.collection.write('mixed_complex', df)
        result = self.collection.item('mixed_complex').to_pandas()
        
        # Verify all types preserved
        assert pd.api.types.is_timedelta64_dtype(result['timedelta'])
        assert pd.api.types.is_categorical_dtype(result['category'])
        pd.testing.assert_frame_equal(result, df)
    
    def test_complex_object_columns(self):
        # Create DataFrame with list and dict columns
        df = pd.DataFrame({
            'lists': [[1, 2, 3], [4, 5], [6, 7, 8]],
            'dicts': [{'a': 1}, {'b': 2}, {'c': 3}],
            'value': [10, 20, 30]
        })
        
        # Write and read
        self.collection.write('complex_objects', df)
        result = self.collection.item('complex_objects').to_pandas()
        
        # Verify complex objects are preserved
        assert result['lists'].tolist() == df['lists'].tolist()
        assert result['dicts'].tolist() == df['dicts'].tolist()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])