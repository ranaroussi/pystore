"""
Tests for PyStore collection functionality
"""

import pytest
import pandas as pd
import numpy as np
import pystore


class TestCollection:
    """Test collection operations"""
    
    def test_collection_creation(self, test_store):
        """Test creating a new collection"""
        collection = test_store.collection('test_collection')
        assert 'test_collection' in test_store.list_collections()
    
    def test_collection_exists_error(self, test_store):
        """Test error when creating existing collection without overwrite"""
        test_store.collection('test_collection')
        
        with pytest.raises(pystore.CollectionExistsError):
            test_store.collection('test_collection', overwrite=False)
    
    def test_collection_overwrite(self, test_store):
        """Test overwriting an existing collection"""
        # Create collection with data
        collection1 = test_store.collection('test_collection')
        data = pd.DataFrame({'value': [1, 2, 3]})
        collection1.write('item1', data)
        
        # Overwrite collection
        collection2 = test_store.collection('test_collection', overwrite=True)
        assert 'item1' not in collection2.list_items()
    
    def test_delete_collection(self, test_store):
        """Test deleting a collection"""
        collection = test_store.collection('test_collection')
        assert 'test_collection' in test_store.list_collections()
        
        test_store.delete_collection('test_collection')
        assert 'test_collection' not in test_store.list_collections()
    
    def test_delete_nonexistent_collection(self, test_store):
        """Test deleting a collection that doesn't exist"""
        with pytest.raises(pystore.CollectionNotFoundError):
            test_store.delete_collection('nonexistent')
    
    def test_list_items(self, test_collection, sample_data):
        """Test listing items in a collection"""
        # Write multiple items
        for i in range(3):
            test_collection.write(f'item_{i}', sample_data)
        
        items = test_collection.list_items()
        assert len(items) == 3
        for i in range(3):
            assert f'item_{i}' in items
    
    def test_list_items_with_metadata_filter(self, test_collection, sample_data):
        """Test listing items with metadata filter"""
        # Write items with different metadata
        test_collection.write('item1', sample_data, metadata={'type': 'A'})
        test_collection.write('item2', sample_data, metadata={'type': 'B'})
        test_collection.write('item3', sample_data, metadata={'type': 'A'})
        
        # Filter by metadata
        items_a = test_collection.list_items(type='A')
        assert len(items_a) == 2
        assert 'item1' in items_a
        assert 'item3' in items_a
        assert 'item2' not in items_a