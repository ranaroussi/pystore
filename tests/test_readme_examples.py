#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Tests to verify the functionality described in the README
"""

import os
import shutil
import tempfile
import pytest
import pandas as pd
import pystore

# Mock yfinance download function to avoid network calls during tests
def mock_yf_download(ticker, multi_level_index=False):
    """Create mock stock data similar to yfinance output"""
    # Use timestamps without nanoseconds to avoid issues with append
    dates = pd.date_range('2023-01-01', periods=200, freq='D')
    # Convert to timestamps without nanoseconds
    dates = pd.DatetimeIndex([pd.Timestamp(d.date()) for d in dates])
    
    # Create data with explicit float values
    data = pd.DataFrame({
        'Open': [150.0 + i * 0.1 for i in range(200)],
        'High': [152.0 + i * 0.1 for i in range(200)],
        'Low': [149.0 + i * 0.1 for i in range(200)],
        'Close': [151.0 + i * 0.1 for i in range(200)],
        'Adj Close': [151.0 + i * 0.1 for i in range(200)],
        'Volume': [1000000 + i * 1000 for i in range(200)]
    }, index=dates)
    data.index.name = 'Date'
    return data


class TestReadmeExamples:
    """Test all examples from the README"""
    
    @pytest.fixture(autouse=True)
    def setup_and_teardown(self):
        """Setup test environment and cleanup after tests"""
        # Create temporary directory for test data
        self.test_dir = tempfile.mkdtemp()
        pystore.set_path(self.test_dir)
        
        yield
        
        # Cleanup
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
    
    def test_basic_workflow(self):
        """Test the basic workflow from README"""
        # List stores (should be empty initially)
        stores = pystore.list_stores()
        assert isinstance(stores, list)
        assert len(stores) == 0
        
        # Connect to datastore (create it if not exist)
        store = pystore.store('mydatastore')
        assert store is not None
        # Check that the store path ends with 'mydatastore'
        assert str(store.datastore).endswith('mydatastore')
        
        # List stores again (should have one now)
        stores = pystore.list_stores()
        assert len(stores) == 1
        assert 'mydatastore' in stores
        
        # List existing collections (should be empty)
        collections = store.list_collections()
        assert isinstance(collections, list)
        assert len(collections) == 0
        
        # Access a collection (create it if not exist)
        collection = store.collection('NASDAQ')
        assert collection is not None
        assert collection.collection == 'NASDAQ'
        
        # List collections again (should have one now)
        collections = store.list_collections()
        assert len(collections) == 1
        assert 'NASDAQ' in collections
        
        # List items in collection (should be empty)
        items = collection.list_items()
        assert isinstance(items, set)
        assert len(items) == 0
    
    def test_data_storage_and_retrieval(self):
        """Test storing and retrieving data"""
        store = pystore.store('mydatastore')
        collection = store.collection('NASDAQ')
        
        # Load some mock data (simulating yfinance)
        aapl = mock_yf_download("AAPL", multi_level_index=False)
        
        # Store the first 100 rows of the data in the collection under "AAPL"
        collection.write('AAPL', aapl[:100], metadata={'source': 'Yahoo Finance'})
        
        # Verify item was stored
        items = collection.list_items()
        assert 'AAPL' in items
        
        # Reading the item's data
        item = collection.item('AAPL')
        assert item is not None
        
        # Check dask dataframe
        data = item.data
        assert data is not None
        assert hasattr(data, 'compute')  # Verify it's a dask dataframe
        
        # Check metadata
        metadata = item.metadata
        assert metadata is not None
        assert metadata.get('source') == 'Yahoo Finance'
        
        # Convert to pandas
        df = item.to_pandas()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 100
        assert list(df.columns) == ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
    
    def test_append_functionality(self):
        """Test appending data to existing items"""
        store = pystore.store('mydatastore')
        collection = store.collection('NASDAQ')
        
        # Load mock data
        aapl = mock_yf_download("AAPL", multi_level_index=False)
        
        # Store first 100 rows
        collection.write('AAPL', aapl[:100], metadata={'source': 'Yahoo Finance'})
        
        # Append the rest of the rows to the "AAPL" item
        collection.append('AAPL', aapl[100:])
        
        # Reading the item's data
        item = collection.item('AAPL')
        df = item.to_pandas()
        
        # Verify all data is present
        assert len(df) == 200
        assert df.index[0] == aapl.index[0]
        assert df.index[-1] == aapl.index[-1]
    
    def test_query_functionality(self):
        """Test querying items based on metadata"""
        store = pystore.store('mydatastore')
        collection = store.collection('NASDAQ')
        
        # Create multiple items with different metadata
        data1 = mock_yf_download("AAPL")[:50]
        data2 = mock_yf_download("MSFT")[:50]
        data3 = mock_yf_download("GOOGL")[:50]
        
        collection.write('AAPL', data1, metadata={'source': 'Yahoo Finance', 'sector': 'Technology'})
        collection.write('MSFT', data2, metadata={'source': 'Yahoo Finance', 'sector': 'Technology'})
        collection.write('GOOGL', data3, metadata={'source': 'Alpha Vantage', 'sector': 'Technology'})
        
        # Query all items
        all_items = collection.list_items()
        assert len(all_items) == 3
        
        # Query by single metadata key
        yahoo_items = collection.list_items(source='Yahoo Finance')
        assert len(yahoo_items) == 2
        assert 'AAPL' in yahoo_items
        assert 'MSFT' in yahoo_items
        assert 'GOOGL' not in yahoo_items
        
        # Query by multiple metadata keys
        tech_yahoo_items = collection.list_items(source='Yahoo Finance', sector='Technology')
        assert len(tech_yahoo_items) == 2
    
    def test_snapshot_functionality(self):
        """Test snapshot creation and usage"""
        store = pystore.store('mydatastore')
        collection = store.collection('NASDAQ')
        
        # Add some data
        data = mock_yf_download("AAPL")[:100]
        collection.write('AAPL', data, metadata={'source': 'Yahoo Finance'})
        
        # Create a snapshot
        collection.create_snapshot('snapshot_v1')
        
        # List available snapshots
        snapshots = collection.list_snapshots()
        assert 'snapshot_v1' in snapshots
        
        # Modify the data
        new_data = mock_yf_download("AAPL")[100:150]
        collection.append('AAPL', new_data)
        
        # Get current version
        current_item = collection.item('AAPL')
        current_df = current_item.to_pandas()
        assert len(current_df) == 150
        
        # Get snapshot version
        snapshot_item = collection.item('AAPL', snapshot='snapshot_v1')
        snapshot_df = snapshot_item.to_pandas()
        assert len(snapshot_df) == 100
        
        # Delete snapshot
        collection.delete_snapshot('snapshot_v1')
        snapshots_after = collection.list_snapshots()
        assert 'snapshot_v1' not in snapshots_after
    
    def test_delete_operations(self):
        """Test deleting items and collections"""
        store = pystore.store('mydatastore')
        collection = store.collection('NASDAQ')
        
        # Add data
        data = mock_yf_download("AAPL")[:100]
        collection.write('AAPL', data)
        
        # Verify item exists
        assert 'AAPL' in collection.list_items()
        
        # Delete the item
        collection.delete_item('AAPL')
        
        # Verify item is deleted
        assert 'AAPL' not in collection.list_items()
        
        # Delete the collection
        store.delete_collection('NASDAQ')
        
        # Verify collection is deleted
        assert 'NASDAQ' not in store.list_collections()
    
    def test_multiple_stores(self):
        """Test working with multiple stores"""
        # Create multiple stores
        store1 = pystore.store('datastore1')
        store2 = pystore.store('datastore2')
        
        # Add collections to each
        collection1 = store1.collection('NYSE')
        collection2 = store2.collection('NASDAQ')
        
        # Verify stores are separate
        assert 'NYSE' in store1.list_collections()
        assert 'NYSE' not in store2.list_collections()
        assert 'NASDAQ' in store2.list_collections()
        assert 'NASDAQ' not in store1.list_collections()
        
        # List all stores
        stores = pystore.list_stores()
        assert 'datastore1' in stores
        assert 'datastore2' in stores