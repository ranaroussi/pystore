"""
Tests for PyStore custom exceptions
"""

import pytest
import pandas as pd
import pystore


class TestExceptions:
    """Test custom exception handling"""
    
    def test_item_not_found_error(self, test_collection):
        """Test ItemNotFoundError is raised correctly"""
        with pytest.raises(pystore.ItemNotFoundError) as exc_info:
            test_collection.item('nonexistent_item')
        
        assert "Item 'nonexistent_item' doesn't exist" in str(exc_info.value)
    
    def test_item_exists_error(self, test_collection, sample_data):
        """Test ItemExistsError is raised correctly"""
        test_collection.write('existing_item', sample_data)
        
        with pytest.raises(pystore.ItemExistsError) as exc_info:
            test_collection.write('existing_item', sample_data, overwrite=False)
        
        assert "Item 'existing_item' already exists" in str(exc_info.value)
    
    def test_collection_not_found_error(self, test_store):
        """Test CollectionNotFoundError is raised correctly"""
        with pytest.raises(pystore.CollectionNotFoundError) as exc_info:
            test_store.delete_collection('nonexistent_collection')
        
        assert "Collection 'nonexistent_collection' does not exist" in str(exc_info.value)
    
    def test_collection_exists_error(self, test_store):
        """Test CollectionExistsError is raised correctly"""
        test_store.collection('existing_collection')
        
        with pytest.raises(pystore.CollectionExistsError) as exc_info:
            test_store.collection('existing_collection', overwrite=False)
        
        assert "Collection 'existing_collection' already exists" in str(exc_info.value)
    
    def test_data_integrity_error(self, test_collection):
        """Test DataIntegrityError is raised correctly"""
        # Create initial data
        dates = pd.date_range('2024-01-01', periods=5, freq='D')
        data = pd.DataFrame({'value': range(5)}, index=dates)
        test_collection.write('test_item', data)
        
        # Try to append with duplicate index and error strategy
        duplicate_data = pd.DataFrame({'value': [99]}, index=[dates[2]])
        
        with pytest.raises(pystore.DataIntegrityError) as exc_info:
            test_collection.append('test_item', duplicate_data, duplicate_handling='error')
        
        assert "duplicate indices" in str(exc_info.value)
    
    def test_validation_error(self, test_collection):
        """Test ValidationError is raised correctly"""
        # Write initial data
        data = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
        test_collection.write('test_item', data)
        
        # Try to append with different schema
        bad_data = pd.DataFrame({'col1': [5], 'col3': [6]})
        
        with pytest.raises(pystore.ValidationError) as exc_info:
            test_collection.append('test_item', bad_data, validate_schema=True)
        
        assert "Schema mismatch" in str(exc_info.value)
    
    def test_snapshot_not_found_error(self, test_collection, sample_data):
        """Test SnapshotNotFoundError is raised correctly"""
        test_collection.write('test_item', sample_data)
        
        with pytest.raises(pystore.SnapshotNotFoundError) as exc_info:
            test_collection.item('test_item', snapshot='nonexistent_snapshot')
        
        assert "Snapshot 'nonexistent_snapshot' doesn't exist" in str(exc_info.value)
    
    def test_exception_inheritance(self):
        """Test that all custom exceptions inherit from PyStoreError"""
        # All custom exceptions should be subclasses of PyStoreError
        assert issubclass(pystore.ItemNotFoundError, pystore.PyStoreError)
        assert issubclass(pystore.ItemExistsError, pystore.PyStoreError)
        assert issubclass(pystore.CollectionNotFoundError, pystore.PyStoreError)
        assert issubclass(pystore.CollectionExistsError, pystore.PyStoreError)
        assert issubclass(pystore.DataIntegrityError, pystore.PyStoreError)
        assert issubclass(pystore.ValidationError, pystore.PyStoreError)
        assert issubclass(pystore.SnapshotNotFoundError, pystore.PyStoreError)
        assert issubclass(pystore.StorageError, pystore.PyStoreError)
        assert issubclass(pystore.SchemaError, pystore.PyStoreError)
        assert issubclass(pystore.ConfigurationError, pystore.PyStoreError)