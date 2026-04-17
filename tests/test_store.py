"""
Tests for PyStore store functionality
"""

import shutil
import tempfile
from pathlib import Path

import pytest

import pystore

INVALID_STORE_NAMES = [
    "",
    "   ",
    ".",
    "..",
    "../escape",
    "nested/name",
    r"nested\name",
    "/tmp/abs",
]


class TestStore:
    """Test store creation and management"""

    def test_store_creation(self, temp_store_path):
        """Test creating a new store"""
        pystore.set_path(temp_store_path)
        store = pystore.store("test_store")

        assert store.datastore == str(temp_store_path / "test_store")
        assert "test_store" in pystore.list_stores()

    def test_store_exists_detection(self, temp_store_path):
        """Test that store existence is properly detected"""
        pystore.set_path(temp_store_path)

        # Create store
        store1 = pystore.store("test_store")

        # Access existing store
        store2 = pystore.store("test_store")

        assert store1.datastore == store2.datastore

    def test_list_stores(self, temp_store_path):
        """Test listing multiple stores"""
        pystore.set_path(temp_store_path)

        # Create multiple stores
        stores = ["store1", "store2", "store3"]
        for store_name in stores:
            pystore.store(store_name)

        listed_stores = pystore.list_stores()
        for store_name in stores:
            assert store_name in listed_stores

    def test_delete_store(self, temp_store_path):
        """Test deleting a store"""
        pystore.set_path(temp_store_path)

        # Create and delete store
        store_name = "test_store"
        pystore.store(store_name)
        assert store_name in pystore.list_stores()

        pystore.delete_store(store_name)
        assert store_name not in pystore.list_stores()

    def test_delete_nonexistent_store(self, temp_store_path):
        """Test deleting a store that doesn't exist"""
        pystore.set_path(temp_store_path)

        with pytest.raises(ValueError, match="Store 'nonexistent' does not exist"):
            pystore.delete_store("nonexistent")

    def test_delete_stores(self, temp_store_path):
        """Test deleting all stores"""
        pystore.set_path(temp_store_path)

        # Create some stores
        pystore.store("store1")
        pystore.store("store2")
        assert len(pystore.list_stores()) >= 2

        # Delete all stores
        pystore.delete_stores()
        assert len(pystore.list_stores()) == 0

    def test_delete_stores_nonexistent_path_raises_valueerror(self, temp_store_path):
        """delete_stores() raises ValueError (not FileNotFoundError) when path doesn't exist"""
        # Use a completely separate temp path so we don't interfere with
        # the fixture's cleanup.
        import tempfile

        other_path = tempfile.mkdtemp(prefix="pystore_delete_stores_test_")
        try:
            pystore.set_path(other_path)
            # Remove the directory to simulate a non-existent store path
            shutil.rmtree(other_path)

            # delete_stores() should raise ValueError because the path doesn't exist
            with pytest.raises(ValueError, match="does not exist"):
                pystore.delete_stores()
        finally:
            # Restore the fixture's path for subsequent tests
            pystore.set_path(temp_store_path)

    def test_path_handling(self):
        """Test various path input formats"""
        # Create secure temporary directories
        temp_dir1 = tempfile.mkdtemp(prefix="pystore_test1_")
        temp_dir2 = tempfile.mkdtemp(prefix="pystore_test2_")

        try:
            # Test with string path
            path1 = pystore.set_path(temp_dir1)
            assert isinstance(path1, Path)

            # Test with Path object
            path2 = pystore.set_path(Path(temp_dir2))
            assert isinstance(path2, Path)

            # Test with tilde expansion
            path3 = pystore.set_path("~/pystore_test3")
            assert isinstance(path3, Path)
            assert str(path3).startswith(str(Path.home()))
        finally:
            # Clean up temporary directories
            shutil.rmtree(temp_dir1, ignore_errors=True)
            shutil.rmtree(temp_dir2, ignore_errors=True)

    def test_invalid_path(self):
        """Test handling of invalid paths"""
        with pytest.raises(ValueError, match="only works with local file system"):
            pystore.set_path("s3://bucket/path")

    @pytest.mark.parametrize("store_name", INVALID_STORE_NAMES)
    def test_rejects_invalid_store_names(self, temp_store_path, store_name):
        """Store APIs reject empty, nested, and escaping names."""
        pystore.set_path(temp_store_path)

        with pytest.raises(ValueError):
            pystore.store(store_name)

        with pytest.raises(ValueError):
            pystore.delete_store(store_name)

    def test_store_item_missing_collection_does_not_create_collection(self, test_store):
        """Read access must not create a missing collection as a side effect."""
        with pytest.raises(pystore.CollectionNotFoundError):
            test_store.item("missing_collection", "missing_item")

        assert "missing_collection" not in test_store.list_collections()
