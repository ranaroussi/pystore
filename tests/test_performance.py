"""
Tests for PyStore performance optimizations
"""

import pytest
import pandas as pd
import numpy as np
import pystore
from pystore.memory import MemoryMonitor, optimize_dataframe_memory, read_in_chunks
from pystore.partition import calculate_optimal_partitions, rebalance_partitions


class TestPerformanceOptimizations:
    """Test performance optimization features"""
    
    def test_streaming_append(self, test_collection):
        """Test streaming append functionality"""
        # Create initial data
        initial_data = pd.DataFrame({
            'value': range(1000)
        }, index=pd.date_range('2024-01-01', periods=1000, freq='1h'))
        
        test_collection.write('stream_test', initial_data)
        
        # Create data iterator
        def data_generator():
            for i in range(5):
                chunk_data = pd.DataFrame({
                    'value': range(i * 100, (i + 1) * 100)
                }, index=pd.date_range('2024-02-01', periods=100, freq=f'{i+1}h'))
                yield chunk_data
        
        # Use streaming append
        test_collection.append_stream('stream_test', data_generator(), chunk_size=100)
        
        # Verify all data was appended
        item = test_collection.item('stream_test')
        df = item.to_pandas()
        assert len(df) == 1500  # 1000 initial + 500 from stream
    
    def test_batch_write(self, test_collection):
        """Test batch write functionality"""
        # Prepare multiple datasets
        items_data = {}
        metadata = {}
        
        for i in range(10):
            data = pd.DataFrame({
                'value': np.random.randn(1000),
                'category': np.random.choice(['A', 'B', 'C'], 1000)
            }, index=pd.date_range('2024-01-01', periods=1000, freq='1h'))
            
            items_data[f'batch_item_{i}'] = data
            metadata[f'batch_item_{i}'] = {'batch_index': i}
        
        # Batch write
        test_collection.write_batch(items_data, metadata=metadata, parallel=True)
        
        # Verify all items were written
        for i in range(10):
            assert f'batch_item_{i}' in test_collection.list_items()
            item_meta = test_collection.get_item_metadata(f'batch_item_{i}')
            assert item_meta['batch_index'] == i
    
    def test_batch_read(self, test_collection, sample_data):
        """Test batch read functionality"""
        # Write multiple items
        for i in range(5):
            test_collection.write(f'read_item_{i}', sample_data)
        
        # Batch read
        items_to_read = [f'read_item_{i}' for i in range(5)]
        results = test_collection.read_batch(items_to_read)
        
        # Verify results
        assert len(results) == 5
        for i in range(5):
            assert f'read_item_{i}' in results
            pd.testing.assert_frame_equal(results[f'read_item_{i}'], sample_data)
    
    def test_optimized_partitioning(self, test_collection):
        """Test optimized partitioning strategies"""
        # Create large time series data
        large_data = pd.DataFrame({
            'value': np.random.randn(100_000),
            'volume': np.random.randint(1000, 10000, 100_000)
        }, index=pd.date_range('2020-01-01', periods=100_000, freq='15min'))
        
        # Write with automatic partitioning
        test_collection.write('partitioned_item', large_data)
        
        # Check that partitioning was applied
        item = test_collection.item('partitioned_item')
        assert item.data.npartitions > 1  # Should have multiple partitions
        
        # Read back and verify
        df_read = item.to_pandas()
        assert len(df_read) == len(large_data)
    
    def test_partition_rebalancing(self, test_collection):
        """Test partition rebalancing"""
        # Create poorly partitioned data (single partition)
        data = pd.DataFrame({
            'value': np.random.randn(50_000)
        }, index=pd.date_range('2023-01-01', periods=50_000, freq='1min'))
        
        test_collection.write('unbalanced_item', data, npartitions=1)
        
        # Rebalance partitions
        from pystore.partition import rebalance_partitions
        rebalance_partitions(test_collection, 'unbalanced_item')
        
        # Check improved partitioning
        item = test_collection.item('unbalanced_item')
        assert item.data.npartitions > 1
        
        # Verify data integrity
        df_read = item.to_pandas()
        pd.testing.assert_frame_equal(df_read, data)
    
    def test_memory_optimization(self):
        """Test memory optimization utilities"""
        # Create DataFrame with inefficient types
        df = pd.DataFrame({
            'small_int': np.array([1, 2, 3, 4, 5] * 1000, dtype=np.int64),
            'small_float': np.array([1.1, 2.2, 3.3, 4.4, 5.5] * 1000, dtype=np.float64),
            'category': ['A', 'B', 'C'] * 1667,  # Repeated values
            'unique_values': range(5000)  # Many unique values
        })
        
        original_memory = df.memory_usage(deep=True).sum()
        
        # Optimize memory
        optimized_df = optimize_dataframe_memory(df, deep=True)
        
        optimized_memory = optimized_df.memory_usage(deep=True).sum()
        
        # Should use less memory
        assert optimized_memory < original_memory
        
        # Verify data integrity
        assert len(optimized_df) == len(df)
        assert optimized_df['small_int'].sum() == df['small_int'].sum()
    
    def test_memory_monitoring(self, test_collection, sample_data):
        """Test memory monitoring functionality"""
        # Use memory monitor
        with MemoryMonitor() as monitor:
            # Perform memory-intensive operation
            for i in range(5):
                test_collection.write(f'memory_test_{i}', sample_data, overwrite=True)
        
        # Monitor should have tracked memory usage (no assertion, just ensure no errors)
    
    def test_chunked_reading(self, test_collection):
        """Test chunked reading for large datasets"""
        # Create large dataset
        large_data = pd.DataFrame({
            'value': np.random.randn(100_000),
            'category': np.random.choice(['A', 'B', 'C', 'D'], 100_000)
        }, index=pd.date_range('2024-01-01', periods=100_000, freq='1min'))
        
        test_collection.write('large_item', large_data)
        
        # Read in chunks
        chunks = list(read_in_chunks(test_collection, 'large_item', chunk_size=10_000))
        
        # Should have 10 chunks
        assert len(chunks) == 10
        
        # Verify each chunk
        total_rows = 0
        for chunk in chunks:
            assert len(chunk) <= 10_000
            total_rows += len(chunk)
        
        assert total_rows == 100_000
    
    def test_metadata_caching(self, test_collection, sample_data):
        """Test metadata caching functionality"""
        # Write item with metadata
        metadata = {'source': 'test', 'version': '1.0'}
        test_collection.write('cached_item', sample_data, metadata=metadata)
        
        # First read - from disk
        meta1 = test_collection.get_item_metadata('cached_item')
        assert meta1['source'] == 'test'
        
        # Second read - should be from cache
        meta2 = test_collection.get_item_metadata('cached_item')
        assert meta2['source'] == 'test'
        
        # Clear cache
        test_collection.clear_metadata_cache('cached_item')
        
        # Next read should be from disk again
        meta3 = test_collection.get_item_metadata('cached_item')
        assert meta3['source'] == 'test'
    
    def test_column_selection_performance(self, test_collection):
        """Test that column selection improves read performance"""
        # Create wide DataFrame
        data = pd.DataFrame({
            f'col_{i}': np.random.randn(10_000) 
            for i in range(50)
        })
        data.index = pd.date_range('2024-01-01', periods=10_000, freq='1min')
        
        test_collection.write('wide_data', data)
        
        # Read all columns
        item_all = test_collection.item('wide_data')
        df_all = item_all.to_pandas()
        assert df_all.shape == (10_000, 50)
        
        # Read subset of columns
        item_subset = test_collection.item('wide_data', columns=['col_0', 'col_1', 'col_2'])
        df_subset = item_subset.to_pandas()
        assert df_subset.shape == (10_000, 3)
        
        # Memory usage should be much less for subset
        assert df_subset.memory_usage(deep=True).sum() < df_all.memory_usage(deep=True).sum() / 10
    
    def test_filter_pushdown(self, test_collection):
        """Test that filters are pushed down to parquet level"""
        # Create data with categories
        data = pd.DataFrame({
            'value': np.random.randn(10_000),
            'category': np.random.choice(['A', 'B', 'C', 'D'], 10_000),
            'flag': np.random.choice([True, False], 10_000)
        }, index=pd.date_range('2024-01-01', periods=10_000, freq='1min'))
        
        test_collection.write('filter_test', data)
        
        # Read with filter
        item = test_collection.item('filter_test', 
                                   filters=[('category', '==', 'A')])
        df_filtered = item.to_pandas()
        
        # Should have fewer rows
        assert len(df_filtered) < len(data)
        assert all(df_filtered['category'] == 'A')
        
        # Multiple filters
        item2 = test_collection.item('filter_test',
                                    filters=[('category', '==', 'B'), 
                                           ('flag', '==', True)])
        df_filtered2 = item2.to_pandas()
        
        assert all(df_filtered2['category'] == 'B')
        assert all(df_filtered2['flag'] == True)