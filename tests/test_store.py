"""
Tests for PyStore store functionality
"""

import pytest
import pystore
from pathlib import Path


class TestStore:
    """Test store creation and management"""
    
    def test_store_creation(self, temp_store_path):
        """Test creating a new store"""
        pystore.set_path(temp_store_path)
        store = pystore.store('test_store')
        
        assert store.datastore == str(temp_store_path / 'test_store')
        assert 'test_store' in pystore.list_stores()
    
    def test_store_exists_detection(self, temp_store_path):
        """Test that store existence is properly detected"""
        pystore.set_path(temp_store_path)
        
        # Create store
        store1 = pystore.store('test_store')
        
        # Access existing store
        store2 = pystore.store('test_store')
        
        assert store1.datastore == store2.datastore
    
    def test_list_stores(self, temp_store_path):
        """Test listing multiple stores"""
        pystore.set_path(temp_store_path)
        
        # Create multiple stores
        stores = ['store1', 'store2', 'store3']
        for store_name in stores:
            pystore.store(store_name)
        
        listed_stores = pystore.list_stores()
        for store_name in stores:
            assert store_name in listed_stores
    
    def test_delete_store(self, temp_store_path):
        """Test deleting a store"""
        pystore.set_path(temp_store_path)
        
        # Create and delete store
        store_name = 'test_store'
        pystore.store(store_name)
        assert store_name in pystore.list_stores()
        
        pystore.delete_store(store_name)
        assert store_name not in pystore.list_stores()
    
    def test_delete_nonexistent_store(self, temp_store_path):
        """Test deleting a store that doesn't exist"""
        pystore.set_path(temp_store_path)
        
        with pytest.raises(ValueError, match="Store 'nonexistent' does not exist"):
            pystore.delete_store('nonexistent')
    
    def test_path_handling(self):
        """Test various path input formats"""
        # Test with string path
        path1 = pystore.set_path("/tmp/pystore_test1")
        assert isinstance(path1, Path)
        
        # Test with Path object
        path2 = pystore.set_path(Path("/tmp/pystore_test2"))
        assert isinstance(path2, Path)
        
        # Test with tilde expansion
        path3 = pystore.set_path("~/pystore_test3")
        assert isinstance(path3, Path)
        assert str(path3).startswith(str(Path.home()))
    
    def test_invalid_path(self):
        """Test handling of invalid paths"""
        with pytest.raises(ValueError, match="only works with local file system"):
            pystore.set_path("s3://bucket/path")