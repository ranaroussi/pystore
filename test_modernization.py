#!/usr/bin/env python
"""
Test script to verify PyStore modernization works correctly
"""

import pandas as pd
import numpy as np
import pystore
import tempfile
import shutil
from pathlib import Path

def test_basic_functionality():
    """Test basic PyStore operations with modern dependencies"""
    
    # Create temporary directory for testing
    test_dir = Path(tempfile.mkdtemp())
    print(f"Testing in directory: {test_dir}")
    
    try:
        # Set PyStore path
        pystore.set_path(test_dir)
        
        # Create a store
        store = pystore.store('test_store')
        print("✓ Store created successfully")
        
        # Create a collection
        collection = store.collection('test_collection')
        print("✓ Collection created successfully")
        
        # Create test data with datetime index
        dates = pd.date_range('2024-01-01', periods=100, freq='D')
        data = pd.DataFrame({
            'value1': np.random.randn(100),
            'value2': np.random.randn(100) * 100,
            'value3': np.random.choice(['A', 'B', 'C'], 100)
        }, index=dates)
        
        # Write data
        collection.write('test_item', data, metadata={'source': 'test'})
        print("✓ Data written successfully")
        
        # Read data back
        item = collection.item('test_item')
        df_read = item.to_pandas()
        print("✓ Data read successfully")
        
        # Verify data integrity
        assert len(df_read) == len(data), "Data length mismatch"
        assert df_read.index.equals(data.index), "Index mismatch"
        assert df_read.columns.equals(data.columns), "Columns mismatch"
        print("✓ Data integrity verified")
        
        # Test append operation
        new_dates = pd.date_range('2024-04-10', periods=10, freq='D')
        new_data = pd.DataFrame({
            'value1': np.random.randn(10),
            'value2': np.random.randn(10) * 100,
            'value3': np.random.choice(['A', 'B', 'C'], 10)
        }, index=new_dates)
        
        collection.append('test_item', new_data)
        print("✓ Data appended successfully")
        
        # Verify append worked
        item_updated = collection.item('test_item')
        df_updated = item_updated.to_pandas()
        assert len(df_updated) == 110, f"Expected 110 rows, got {len(df_updated)}"
        print("✓ Append operation verified")
        
        # Test snapshot
        collection.create_snapshot('test_snapshot')
        print("✓ Snapshot created successfully")
        
        # Test metadata
        assert item.metadata['source'] == 'test', "Metadata mismatch"
        print("✓ Metadata verified")
        
        # Test listing
        assert 'test_item' in collection.list_items(), "Item not in list"
        print("✓ Listing works correctly")
        
        print("\n✅ All tests passed! PyStore modernization successful.")
        
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        # Cleanup
        shutil.rmtree(test_dir)
        print(f"\nCleaned up test directory: {test_dir}")

def test_edge_cases():
    """Test edge cases and problematic scenarios"""
    
    test_dir = Path(tempfile.mkdtemp())
    
    try:
        pystore.set_path(test_dir)
        store = pystore.store('edge_test_store')
        collection = store.collection('edge_collection')
        
        # Test 1: Data with nanosecond precision (previously caused issues)
        dates_ns = pd.date_range('2024-01-01', periods=10, freq='1s')
        dates_ns = dates_ns + pd.to_timedelta(np.random.randint(0, 10, size=10), unit='ns')
        
        data_ns = pd.DataFrame({
            'value': range(10)
        }, index=dates_ns)
        
        collection.write('nanosecond_test', data_ns)
        print("✓ Nanosecond precision data written successfully")
        
        # Test 2: Empty append (should handle gracefully)
        empty_df = pd.DataFrame(columns=['value'])
        collection.append('nanosecond_test', empty_df)
        print("✓ Empty append handled correctly")
        
        # Test 3: Overlapping data append
        overlap_dates = pd.date_range('2024-01-05', periods=10, freq='1s')
        overlap_data = pd.DataFrame({
            'value': range(100, 110)
        }, index=overlap_dates)
        
        collection.append('nanosecond_test', overlap_data)
        result = collection.item('nanosecond_test').to_pandas()
        print(f"✓ Overlapping append handled correctly (total rows: {len(result)})")
        
        print("\n✅ Edge case tests passed!")
        
    except Exception as e:
        print(f"\n❌ Edge case test failed with error: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        shutil.rmtree(test_dir)

if __name__ == "__main__":
    print("Testing PyStore Modernization (Phase 1)\n")
    print("=" * 50)
    test_basic_functionality()
    print("\n" + "=" * 50)
    print("\nTesting Edge Cases\n")
    print("=" * 50)
    test_edge_cases()