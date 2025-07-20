"""
Tests for PyStore snapshot functionality
"""

import pytest
import time
import pandas as pd
import pystore


class TestSnapshots:
    """Test snapshot operations"""
    
    def test_create_snapshot(self, test_collection, sample_data):
        """Test creating a snapshot"""
        # Write some data
        test_collection.write('item1', sample_data)
        test_collection.write('item2', sample_data * 2)
        
        # Create snapshot
        snapshot_name = 'test_snapshot'
        test_collection.create_snapshot(snapshot_name)
        
        # Verify snapshot exists
        assert snapshot_name in test_collection.list_snapshots()
    
    def test_create_snapshot_auto_name(self, test_collection, sample_data):
        """Test creating a snapshot with auto-generated name"""
        test_collection.write('item1', sample_data)
        
        # Create snapshot without name
        test_collection.create_snapshot()
        
        # Should have one snapshot with timestamp name
        snapshots = test_collection.list_snapshots()
        assert len(snapshots) == 1
    
    def test_read_from_snapshot(self, test_collection, sample_data):
        """Test reading data from a snapshot"""
        item_name = 'test_item'
        
        # Write initial data
        initial_data = sample_data[:50]
        test_collection.write(item_name, initial_data)
        
        # Create snapshot
        snapshot_name = 'snapshot1'
        test_collection.create_snapshot(snapshot_name)
        
        # Modify data after snapshot
        new_data = sample_data
        test_collection.write(item_name, new_data, overwrite=True)
        
        # Read from snapshot - should get original data
        snapshot_item = test_collection.item(item_name, snapshot=snapshot_name)
        df_snapshot = snapshot_item.to_pandas()
        
        # Read current - should get new data
        current_item = test_collection.item(item_name)
        df_current = current_item.to_pandas()
        
        # Verify
        assert len(df_snapshot) == 50
        assert len(df_current) == 100
        pd.testing.assert_frame_equal(df_snapshot, initial_data)
    
    def test_delete_snapshot(self, test_collection, sample_data):
        """Test deleting a snapshot"""
        test_collection.write('item1', sample_data)
        
        # Create and delete snapshot
        snapshot_name = 'test_snapshot'
        test_collection.create_snapshot(snapshot_name)
        assert snapshot_name in test_collection.list_snapshots()
        
        test_collection.delete_snapshot(snapshot_name)
        assert snapshot_name not in test_collection.list_snapshots()
    
    def test_delete_nonexistent_snapshot(self, test_collection):
        """Test deleting a snapshot that doesn't exist"""
        # Should not raise error (returns True)
        result = test_collection.delete_snapshot('nonexistent')
        assert result is True
    
    def test_delete_all_snapshots(self, test_collection, sample_data):
        """Test deleting all snapshots"""
        test_collection.write('item1', sample_data)
        
        # Create multiple snapshots
        for i in range(3):
            test_collection.create_snapshot(f'snapshot_{i}')
            time.sleep(0.1)  # Ensure different timestamps
        
        assert len(test_collection.list_snapshots()) == 3
        
        # Delete all snapshots
        test_collection.delete_snapshots()
        assert len(test_collection.list_snapshots()) == 0
    
    def test_snapshot_preserves_metadata(self, test_collection, sample_data):
        """Test that snapshots preserve item metadata"""
        metadata = {'source': 'test', 'version': '1.0'}
        test_collection.write('test_item', sample_data, metadata=metadata)
        
        # Create snapshot
        test_collection.create_snapshot('test_snapshot')
        
        # Read from snapshot
        snapshot_item = test_collection.item('test_item', snapshot='test_snapshot')
        assert snapshot_item.metadata['source'] == 'test'
        assert snapshot_item.metadata['version'] == '1.0'
    
    def test_snapshot_invalid_name(self, test_collection, sample_data):
        """Test creating snapshot with special characters in name"""
        test_collection.write('item1', sample_data)
        
        # Special characters should be filtered out
        test_collection.create_snapshot('test/snapshot\\with*special?chars')
        
        snapshots = test_collection.list_snapshots()
        assert len(snapshots) == 1
        # Name should have special chars removed
        snapshot_name = list(snapshots)[0]
        assert '/' not in snapshot_name
        assert '\\' not in snapshot_name
        assert '*' not in snapshot_name
    
    def test_item_not_in_snapshot(self, test_collection, sample_data):
        """Test accessing an item that doesn't exist in a snapshot"""
        # Create snapshot before adding item
        test_collection.create_snapshot('empty_snapshot')
        
        # Add item after snapshot
        test_collection.write('new_item', sample_data)
        
        # Try to access item from snapshot
        with pytest.raises(pystore.ItemNotFoundError):
            test_collection.item('new_item', snapshot='empty_snapshot')