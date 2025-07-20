#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import pandas as pd
import numpy as np
import tempfile
import shutil
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pystore


def test_multiindex_basic():
    print("Testing basic MultiIndex support...")
    
    # Setup
    path = tempfile.mkdtemp()
    pystore.set_path(path)
    store = pystore.store("test_store")
    collection = store.collection("test_collection")
    
    try:
        # Create DataFrame with MultiIndex
        index = pd.MultiIndex.from_product(
            [['A', 'B', 'C'], pd.date_range('2020-01-01', periods=3)],
            names=['category', 'date']
        )
        df = pd.DataFrame({
            'value1': np.random.randn(9),
            'value2': np.random.randn(9)
        }, index=index)
        
        print(f"Original DataFrame shape: {df.shape}")
        print(f"Original index type: {type(df.index)}")
        print(f"Original index names: {df.index.names}")
        
        # Write to store
        collection.write('multiindex_item', df)
        print("✓ Write successful")
        
        # Read back
        item = collection.item('multiindex_item')
        result = item.to_pandas()
        
        print(f"Result DataFrame shape: {result.shape}")
        print(f"Result index type: {type(result.index)}")
        print(f"Result index names: {result.index.names}")
        
        # Verify structure is preserved
        assert isinstance(result.index, pd.MultiIndex), "Index should be MultiIndex"
        assert result.index.names == ['category', 'date'], f"Names mismatch: {result.index.names}"
        assert result.index.nlevels == 2, f"Level count mismatch: {result.index.nlevels}"
        assert df.shape == result.shape, f"Shape mismatch: {df.shape} vs {result.shape}"
        
        print("✓ MultiIndex structure preserved")
        print("✓ Test passed!")
        
    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        shutil.rmtree(path)


def test_complex_types():
    print("\nTesting complex data types...")
    
    # Setup
    path = tempfile.mkdtemp()
    pystore.set_path(path)
    store = pystore.store("test_store")
    collection = store.collection("test_collection")
    
    try:
        # Create DataFrame with complex types
        df = pd.DataFrame({
            'timedelta': pd.to_timedelta(['1 days', '2 days', '3 days']),
            'category': pd.Categorical(['A', 'B', 'A'], categories=['A', 'B', 'C'], ordered=True),
            'value': [1, 2, 3]
        })
        
        print("Original dtypes:")
        for col, dtype in df.dtypes.items():
            print(f"  {col}: {dtype}")
        
        # Write and read
        collection.write('complex_test', df)
        print("✓ Write successful")
        
        result = collection.item('complex_test').to_pandas()
        
        print("Result dtypes:")
        for col, dtype in result.dtypes.items():
            print(f"  {col}: {dtype}")
        
        # Verify types preserved
        assert pd.api.types.is_timedelta64_dtype(result['timedelta']), "Timedelta type not preserved"
        assert pd.api.types.is_categorical_dtype(result['category']), "Categorical type not preserved"
        assert result['category'].cat.ordered == True, "Categorical ordering not preserved"
        
        print("✓ Complex types preserved")
        print("✓ Test passed!")
        
    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        shutil.rmtree(path)


def test_nested_objects():
    print("\nTesting nested objects (lists and dicts)...")
    
    # Setup
    path = tempfile.mkdtemp()
    pystore.set_path(path)
    store = pystore.store("test_store")
    collection = store.collection("test_collection")
    
    try:
        # Create DataFrame with nested objects
        df = pd.DataFrame({
            'lists': [[1, 2, 3], [4, 5], [6, 7, 8]],
            'dicts': [{'a': 1}, {'b': 2}, {'c': 3}],
            'value': [10, 20, 30]
        })
        
        print("Original data:")
        print(f"  lists[0]: {df['lists'].iloc[0]}")
        print(f"  dicts[0]: {df['dicts'].iloc[0]}")
        
        # Write and read
        collection.write('nested_test', df)
        print("✓ Write successful")
        
        result = collection.item('nested_test').to_pandas()
        
        print("Result data:")
        print(f"  lists[0]: {result['lists'].iloc[0]}")
        print(f"  dicts[0]: {result['dicts'].iloc[0]}")
        
        # Verify objects preserved
        assert result['lists'].tolist() == df['lists'].tolist(), "Lists not preserved"
        assert result['dicts'].tolist() == df['dicts'].tolist(), "Dicts not preserved"
        
        print("✓ Nested objects preserved")
        print("✓ Test passed!")
        
    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        shutil.rmtree(path)


if __name__ == "__main__":
    print("Running PyStore MultiIndex and Complex Type Tests")
    print("=" * 50)
    
    test_multiindex_basic()
    test_complex_types()
    test_nested_objects()
    
    print("\nAll tests completed!")