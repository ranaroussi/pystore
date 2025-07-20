#!/usr/bin/env python
# -*- coding: UTF-8 -*-

"""
Tests for PyStore README Examples
Verifies all functionality shown in the README works correctly
"""

import pytest
import pandas as pd
import numpy as np
import tempfile
import shutil
import os
from unittest.mock import patch, MagicMock

import pystore


class TestReadmeExamples:
    """Test all examples from the README"""
    
    def setup_method(self):
        """Setup test environment"""
        self.path = tempfile.mkdtemp()
        pystore.set_path(self.path)
    
    def teardown_method(self):
        """Cleanup test environment"""
        shutil.rmtree(self.path)
    
    def test_basic_workflow_with_yfinance(self):
        """Test the basic workflow example from README with yfinance"""
        # Mock yfinance since we don't want to make real API calls
        with patch('yfinance.download') as mock_download:
            # Create mock data that yfinance would return
            dates = pd.date_range('2023-01-01', periods=200, freq='D')
            mock_data = pd.DataFrame({
                'Open': np.random.randn(200) * 10 + 150,
                'High': np.random.randn(200) * 10 + 152,
                'Low': np.random.randn(200) * 10 + 148,
                'Close': np.random.randn(200) * 10 + 150,
                'Volume': np.random.randint(1000000, 10000000, 200),
                'Adj Close': np.random.randn(200) * 10 + 150
            }, index=dates)
            mock_download.return_value = mock_data
            
            # Now run the README example
            import yfinance as yf
            
            # List stores (should be empty initially)
            stores = pystore.list_stores()
            assert isinstance(stores, list)
            
            # Connect to datastore (create it if not exist)
            store = pystore.store('mydatastore')
            # Store objects may not have a 'name' attribute in this implementation
            
            # List existing collections (should be empty)
            collections = store.list_collections()
            assert collections == []
            
            # Access a collection (create it if not exist)
            collection = store.collection('NASDAQ')
            # Collection objects may not have a 'name' attribute in this implementation
            
            # List items in collection (should be empty)
            items = collection.list_items()
            assert len(items) == 0
            
            # Load some data from yfinance
            aapl = yf.download("AAPL", multi_level_index=False)
            assert len(aapl) == 200
            
            # Store the first 100 rows of the data in the collection under "AAPL"
            collection.write('AAPL', aapl[:100], metadata={'source': 'yfinance'})
            
            # Reading the item's data
            item = collection.item('AAPL')
            data = item.data  # <-- Dask dataframe
            metadata = item.metadata
            df = item.to_pandas()
            
            # Verify data
            assert len(df) == 100
            assert metadata['source'] == 'yfinance'
            pd.testing.assert_frame_equal(df, aapl[:100], check_names=False, check_freq=False)
            
            # Append the rest of the rows to the "AAPL" item
            collection.append('AAPL', aapl[100:])
            
            # Reading the updated item's data
            item = collection.item('AAPL')
            data = item.data
            metadata = item.metadata
            df = item.to_pandas()
            
            # Verify appended data
            assert len(df) == 200
            pd.testing.assert_frame_equal(df, aapl, check_names=False, check_freq=False)
    
    def test_query_functionality(self):
        """Test metadata-based query functionality"""
        store = pystore.store('test_store')
        collection = store.collection('test_collection')
        
        # Create items with different metadata
        df1 = pd.DataFrame({'value': [1, 2, 3]})
        df2 = pd.DataFrame({'value': [4, 5, 6]})
        df3 = pd.DataFrame({'value': [7, 8, 9]})
        
        collection.write('item1', df1, metadata={'source': 'api', 'type': 'raw'})
        collection.write('item2', df2, metadata={'source': 'file', 'type': 'raw'})
        collection.write('item3', df3, metadata={'source': 'api', 'type': 'processed'})
        
        # Query by metadata
        api_items = collection.list_items(source='api')
        assert set(api_items) == {'item1', 'item3'}
        
        raw_items = collection.list_items(type='raw')
        assert set(raw_items) == {'item1', 'item2'}
        
        # Query with multiple criteria
        api_raw_items = collection.list_items(source='api', type='raw')
        assert set(api_raw_items) == {'item1'}
    
    def test_snapshot_functionality(self):
        """Test snapshot functionality"""
        store = pystore.store('test_store')
        collection = store.collection('test_collection')
        
        # Create some items
        df1 = pd.DataFrame({'value': [1, 2, 3]})
        df2 = pd.DataFrame({'value': [4, 5, 6]})
        
        collection.write('item1', df1)
        collection.write('item2', df2)
        
        # Create a snapshot
        collection.create_snapshot('v1.0')
        
        # List available snapshots
        snapshots = collection.list_snapshots()
        assert 'v1.0' in snapshots
        
        # Modify an item
        df1_modified = pd.DataFrame({'value': [10, 20, 30]})
        collection.write('item1', df1_modified, overwrite=True)
        
        # Get current version
        current_item = collection.item('item1')
        pd.testing.assert_frame_equal(current_item.to_pandas(), df1_modified, check_names=False)
        
        # Get snapshot version
        snapshot_item = collection.item('item1', snapshot='v1.0')
        pd.testing.assert_frame_equal(snapshot_item.to_pandas(), df1, check_names=False)
        
        # Delete snapshot
        collection.delete_snapshot('v1.0')
        assert 'v1.0' not in collection.list_snapshots()
    
    def test_delete_operations(self):
        """Test delete operations"""
        store = pystore.store('test_store')
        collection = store.collection('test_collection')
        
        # Create an item
        df = pd.DataFrame({'value': [1, 2, 3]})
        collection.write('test_item', df)
        
        # Verify item exists
        assert 'test_item' in collection.list_items()
        
        # Delete the item
        collection.delete_item('test_item')
        
        # Verify item is deleted
        assert 'test_item' not in collection.list_items()
        
        # Delete the collection
        store.delete_collection('test_collection')
        
        # Verify collection is deleted
        assert 'test_collection' not in store.list_collections()
    
    def test_dask_scheduler_configuration(self):
        """Test Dask scheduler configuration"""
        # Test setting a local cluster
        from dask.distributed import LocalCluster
        
        # Create a local cluster with minimal resources for testing
        cluster = LocalCluster(n_workers=1, threads_per_worker=1, 
                              processes=False, silence_logs=60)
        try:
            pystore.set_client(cluster)
            client = pystore.get_client()
            assert client is not None
        finally:
            cluster.close()
        
        # Reset client
        pystore.set_client(None)
    
    def test_path_configuration(self):
        """Test storage path configuration"""
        # Test default path
        default_path = pystore.get_path()
        assert default_path is not None
        
        # Test setting custom path
        custom_path = tempfile.mkdtemp()
        try:
            pystore.set_path(custom_path)
            # Path object might be returned instead of string, and macOS might resolve symlinks
            path1 = os.path.realpath(str(pystore.get_path()))
            path2 = os.path.realpath(str(custom_path))
            assert path1 == path2
            
            # Create a store in custom path
            store = pystore.store('custom_store')
            assert os.path.exists(os.path.join(custom_path, 'custom_store'))
        finally:
            shutil.rmtree(custom_path)
    
    def test_collection_namespacing(self):
        """Test collection namespacing concepts from README"""
        store = pystore.store('market_data')
        
        # Create collections for different frequencies
        eod_collection = store.collection('EOD')
        minute_collection = store.collection('ONEMINUTE')
        
        # Add data to different collections
        eod_data = pd.DataFrame({
            'close': [100, 101, 102],
            'volume': [1000, 1100, 1200]
        }, index=pd.date_range('2023-01-01', periods=3, freq='D'))
        
        minute_data = pd.DataFrame({
            'close': np.random.randn(60) + 100,
            'volume': np.random.randint(10, 100, 60)
        }, index=pd.date_range('2023-01-01 09:30', periods=60, freq='1min'))
        
        eod_collection.write('AAPL', eod_data)
        minute_collection.write('AAPL', minute_data)
        
        # Verify data isolation
        eod_aapl = eod_collection.item('AAPL').to_pandas()
        minute_aapl = minute_collection.item('AAPL').to_pandas()
        
        assert len(eod_aapl) == 3
        assert len(minute_aapl) == 60
        # Note: Freq info might not be preserved through storage/retrieval
    
    def test_metadata_persistence(self):
        """Test that metadata persists across reads"""
        store = pystore.store('test_store')
        collection = store.collection('test_collection')
        
        # Write data with metadata
        df = pd.DataFrame({'value': [1, 2, 3]})
        metadata = {
            'source': 'yfinance',
            'last_updated': '2023-01-01',
            'version': 1.0,
            'tags': ['equity', 'US', 'tech']
        }
        
        collection.write('TEST', df, metadata=metadata)
        
        # Read back and verify metadata
        item = collection.item('TEST')
        # Check that user metadata is preserved (system may add additional metadata)
        for key, value in metadata.items():
            assert item.metadata.get(key) == value
        
        # Append data and verify metadata persists
        df2 = pd.DataFrame({'value': [4, 5, 6]})
        collection.append('TEST', df2)
        
        item = collection.item('TEST')
        # Check that user metadata is preserved (system may add additional metadata)
        for key, value in metadata.items():
            assert item.metadata.get(key) == value