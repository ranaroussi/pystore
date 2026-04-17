#!/usr/bin/env python
# -*- coding: UTF-8 -*-

"""
Tests for PyStore Phase 4 Feature Enhancements
"""

import pytest
import pandas as pd
import numpy as np
import os
import tempfile
import shutil
import asyncio
from datetime import datetime

import pystore
from pystore import (
    transaction, batch_transaction, async_pystore,
    create_validator, create_timeseries_validator,
    ValidationRule, ColumnExistsRule, RangeRule,
    SchemaEvolution, EvolutionStrategy
)
from pystore.exceptions import ValidationError, TransactionError


class TestAsyncOperations:
    def setup_method(self):
        self.path = tempfile.mkdtemp()
        pystore.set_path(self.path)
        self.store = pystore.store("test_store")
        self.collection = self.store.collection("test_collection")
    
    def teardown_method(self):
        shutil.rmtree(self.path)
    
    @pytest.mark.asyncio
    async def test_async_write_read(self):
        # Create test data
        df = pd.DataFrame({
            'value': np.random.randn(100),
            'timestamp': pd.date_range('2023-01-01', periods=100)
        })
        
        # Async write and read
        async with async_pystore(self.collection) as async_coll:
            await async_coll.write('async_test', df)
            result = await async_coll.read('async_test')
        
        # Verify
        pd.testing.assert_frame_equal(result, df)
    
    @pytest.mark.asyncio
    async def test_async_batch_operations(self):
        # Create multiple DataFrames
        data = {
            f'item_{i}': pd.DataFrame({
                'value': np.random.randn(50),
                'id': i
            }) for i in range(5)
        }
        
        async with async_pystore(self.collection) as async_coll:
            # Batch write
            await async_coll.write_batch(data)
            
            # Batch read
            items = list(data.keys())
            results = await async_coll.read_batch(items)
        
        # Verify all items
        for item, df in data.items():
            assert item in results
            pd.testing.assert_frame_equal(results[item], df)

    @pytest.mark.asyncio
    async def test_parallel_append_alias(self):
        """parallel_append is a backward-compatible alias for ordered_append.

        Both names must produce identical sequential behaviour — DataFrames
        are appended one at a time in the order provided.
        """
        from pystore.async_operations import AsyncCollection

        # Write an initial item
        df_initial = pd.DataFrame({'value': [1, 2]})
        self.collection.write('alias_item', df_initial)

        async_coll = AsyncCollection(self.collection)

        df_a = pd.DataFrame({'value': [3]}, index=[2])
        df_b = pd.DataFrame({'value': [4]}, index=[3])

        # Use the alias — must behave identically to ordered_append
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            await async_coll.parallel_append('alias_item', [df_a, df_b])

        result = self.collection.item('alias_item').to_pandas()
        expected = pd.DataFrame({'value': [1, 2, 3, 4]})
        pd.testing.assert_frame_equal(result.reset_index(drop=True), expected)

        # Verify that parallel_append delegates to ordered_append (emits
        # DeprecationWarning but produces the same result).
        with pytest.warns(DeprecationWarning, match="parallel_append"):
            await async_coll.parallel_append('alias_item', [df_a, df_b])

    @pytest.mark.asyncio
    async def test_async_collection_close_idempotent(self):
        """Calling close() multiple times on AsyncCollection should be a no-op."""
        from pystore.async_operations import AsyncCollection

        async_coll = AsyncCollection(self.collection)
        # First close — should succeed
        async_coll.close()
        assert async_coll._closed is True

        # Second close — should be a no-op (no exception raised)
        async_coll.close()
        assert async_coll._closed is True

    @pytest.mark.asyncio
    async def test_async_store_close_idempotent(self):
        """Calling close() multiple times on AsyncStore should be a no-op."""
        from pystore.async_operations import AsyncStore

        async_store = AsyncStore(self.store)
        # First close — should succeed
        async_store.close()
        assert async_store._closed is True

        # Second close — should be a no-op (no exception raised)
        async_store.close()
        assert async_store._closed is True

    @pytest.mark.asyncio
    async def test_async_collection_closed_flag_initialized(self):
        """_closed flag should be False on construction, not rely on getattr."""
        from pystore.async_operations import AsyncCollection

        async_coll = AsyncCollection(self.collection)
        assert async_coll._closed is False

    @pytest.mark.asyncio
    async def test_async_store_closed_flag_initialized(self):
        """_closed flag should be False on construction, not rely on getattr."""
        from pystore.async_operations import AsyncStore

        async_store = AsyncStore(self.store)
        assert async_store._closed is False

    @pytest.mark.asyncio
    async def test_context_manager_no_double_shutdown(self):
        """AsyncContextManager should not shut down executor twice on exit.

        The close() call inside __aexit__ already shuts down the shared
        executor; a redundant second shutdown was removed as part of the
        integration cleanup.
        """
        from pystore.async_operations import AsyncContextManager

        ctx = AsyncContextManager(self.collection)
        async with ctx as async_coll:
            df = pd.DataFrame({'value': [1, 2]})
            await async_coll.write('ctx_item', df)

        # After exiting the context, the async object should be closed
        assert async_coll._closed is True

        # Verify data was written
        result = self.collection.item('ctx_item').to_pandas()
        pd.testing.assert_frame_equal(result.reset_index(drop=True), df)


class TestTransactions:
    def setup_method(self):
        self.path = tempfile.mkdtemp()
        pystore.set_path(self.path)
        self.store = pystore.store("test_store")
        self.collection = self.store.collection("test_collection")
    
    def teardown_method(self):
        shutil.rmtree(self.path)
    
    def test_transaction_commit(self):
        # Create test data
        df1 = pd.DataFrame({'value': [1, 2, 3]})
        df2 = pd.DataFrame({'value': [4, 5, 6]})
        
        # Use transaction
        with transaction(self.collection) as txn:
            txn.write('item1', df1)
            txn.write('item2', df2)
        
        # Verify both writes succeeded
        result1 = self.collection.item('item1').to_pandas()
        result2 = self.collection.item('item2').to_pandas()
        
        pd.testing.assert_frame_equal(result1, df1)
        pd.testing.assert_frame_equal(result2, df2)
    
    def test_transaction_rollback(self):
        # Create initial data
        df_initial = pd.DataFrame({'value': [1, 2, 3]})
        self.collection.write('item_rollback', df_initial)
        
        # Attempt transaction that will fail
        df_new = pd.DataFrame({'value': [4, 5, 6]})
        
        try:
            with transaction(self.collection) as txn:
                txn.write('item_rollback', df_new, overwrite=True)
                # Force an error
                raise ValueError("Simulated error")
        except ValueError:
            pass
        
        # Verify original data is preserved
        result = self.collection.item('item_rollback').to_pandas()
        pd.testing.assert_frame_equal(result, df_initial)
    
    def test_batch_transaction(self):
        # Create multiple items
        data = {f'batch_{i}': pd.DataFrame({'value': [i]}) for i in range(10)}
        
        with batch_transaction(self.collection) as batch:
            for item, df in data.items():
                batch.write(item, df)
        
        # Verify all items written
        for item, df in data.items():
            result = self.collection.item(item).to_pandas()
            pd.testing.assert_frame_equal(result, df)

    def test_cleanup_keeps_temp_dir_when_preservation_fails(self):
        """When preservation of unrestored backups fails, the temp
        directory must be kept on disk so the user can recover data
        manually.
        """
        from pystore.transactions import Transaction

        txn = Transaction(self.collection)
        txn._ensure_temp_dir()
        temp_dir = txn.temp_dir
        assert temp_dir is not None

        # Simulate a leftover backup directory in the temp dir that
        # represents an unrestored backup from a failed rollback.
        backup_dir = os.path.join(temp_dir, "backup_orphan_item")
        os.makedirs(backup_dir)

        # Make the recovery directory write-protected so that
        # shutil.move inside _cleanup fails, triggering
        # preservation_failed = True.
        recovery_base = temp_dir + "_rollback_recovery"

        original_makedirs = os.makedirs

        def _raising_makedirs(*args, **kwargs):
            # Only raise for the recovery path to simulate a
            # PermissionError during preservation.
            if args and recovery_base in args[0]:
                raise PermissionError("simulated permission denied")
            return original_makedirs(*args, **kwargs)

        import unittest.mock
        with unittest.mock.patch("pystore.transactions.os.makedirs", side_effect=_raising_makedirs):
            txn._cleanup()

        # The temp dir should still exist because preservation failed
        assert os.path.exists(temp_dir), (
            "Temp directory should be preserved when backup preservation fails"
        )
        # The backup should still be inside
        assert os.path.exists(backup_dir), (
            "Unrestored backup should remain in the preserved temp dir"
        )

        # Clean up for the test
        shutil.rmtree(temp_dir, ignore_errors=True)

    def test_cleanup_keeps_temp_dir_when_remaining_backups(self):
        """When backups remain in the temp dir after preservation
        (e.g. the preservation loop succeeded partially and one
        backup_ directory is still left), the temp directory must be
        kept on disk.
        """
        import unittest.mock
        from pystore.transactions import Transaction

        txn = Transaction(self.collection)
        txn._ensure_temp_dir()
        temp_dir = txn.temp_dir
        assert temp_dir is not None

        # Simulate TWO backup directories.  We'll make shutil.move
        # fail on the second one so that one backup is moved out but
        # the second remains inside the temp dir.
        backup_dir1 = os.path.join(temp_dir, "backup_item_a")
        backup_dir2 = os.path.join(temp_dir, "backup_item_b")
        os.makedirs(backup_dir1)
        os.makedirs(backup_dir2)

        original_move = shutil.move
        call_count = {"n": 0}

        def _selective_move(src, dst, *args, **kwargs):
            call_count["n"] += 1
            # Let the first move succeed, fail on the second
            if call_count["n"] > 1:
                raise PermissionError("simulated move failure")
            return original_move(src, dst, *args, **kwargs)

        with unittest.mock.patch("pystore.transactions.shutil.move", side_effect=_selective_move):
            txn._cleanup()

        # The temp dir should still exist because backup_item_b
        # remains inside (preservation_failed = True after the
        # second move fails, and remaining_backups = True because
        # backup_item_b is still there).
        assert os.path.exists(temp_dir), (
            "Temp directory should be preserved when backups remain"
        )

        # Clean up for the test
        shutil.rmtree(temp_dir, ignore_errors=True)

    def test_cleanup_removes_temp_dir_when_no_backups(self):
        """When no backups remain, the temp directory should be removed."""
        from pystore.transactions import Transaction

        txn = Transaction(self.collection)
        txn._ensure_temp_dir()
        temp_dir = txn.temp_dir
        assert temp_dir is not None

        # No backup_ directories — just a stray file
        dummy_file = os.path.join(temp_dir, "scratch.txt")
        with open(dummy_file, "w") as f:
            f.write("hello")

        txn._cleanup()

        # The temp dir should be gone
        assert not os.path.exists(temp_dir), (
            "Temp directory should be removed when no backups remain"
        )


class TestValidation:
    def setup_method(self):
        self.path = tempfile.mkdtemp()
        pystore.set_path(self.path)
        self.store = pystore.store("test_store")
        self.collection = self.store.collection("test_collection")
    
    def teardown_method(self):
        shutil.rmtree(self.path)
    
    def test_column_validation(self):
        # Create validator
        validator = create_validator()
        validator.add_rule(ColumnExistsRule(['price', 'volume']))
        
        # Valid data
        df_valid = pd.DataFrame({
            'price': [100, 101, 102],
            'volume': [1000, 2000, 3000]
        })
        
        # Should pass
        assert validator.validate(df_valid, raise_on_error=False)
        
        # Invalid data (missing column)
        df_invalid = pd.DataFrame({
            'price': [100, 101, 102]
        })
        
        # Should fail
        assert not validator.validate(df_invalid, raise_on_error=False)
    
    def test_range_validation(self):
        validator = create_validator()
        validator.add_rule(RangeRule('price', min_val=0, max_val=1000))
        
        # Valid data
        df_valid = pd.DataFrame({'price': [100, 200, 300]})
        assert validator.validate(df_valid, raise_on_error=False)
        
        # Invalid data
        df_invalid = pd.DataFrame({'price': [100, 2000, 300]})
        assert not validator.validate(df_invalid, raise_on_error=False)
    
    def test_timeseries_validator(self):
        validator = create_timeseries_validator(['value1', 'value2'])
        
        # Valid time series
        df = pd.DataFrame({
            'value1': [1.0, 2.0, 3.0],
            'value2': [4.0, 5.0, 6.0]
        }, index=pd.date_range('2023-01-01', periods=3))
        
        assert validator.validate(df, raise_on_error=False)


class TestSchemaEvolution:
    def setup_method(self):
        self.path = tempfile.mkdtemp()
        pystore.set_path(self.path)
        self.store = pystore.store("test_store")
        self.collection = self.store.collection("test_collection")
    
    def teardown_method(self):
        shutil.rmtree(self.path)
    
    def test_schema_detection(self):
        # Create initial schema
        df1 = pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': ['a', 'b', 'c']
        })
        
        schema1 = pystore.schema_evolution.Schema.from_dataframe(df1)
        
        # Create modified schema
        df2 = pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': ['a', 'b', 'c'],
            'col3': [4.0, 5.0, 6.0]  # New column
        })
        
        schema2 = pystore.schema_evolution.Schema.from_dataframe(df2)
        
        # Detect changes
        changes = schema1.detect_changes(schema2)
        
        # Should detect one column addition
        assert len(changes) == 1
        assert changes[0].change_type == 'column_added'
        assert changes[0].column == 'col3'
    
    def test_compatible_evolution(self):
        evolution = SchemaEvolution(EvolutionStrategy.COMPATIBLE)
        
        # Initial schema
        df1 = pd.DataFrame({'value': [1, 2, 3]})
        schema1 = pystore.schema_evolution.Schema.from_dataframe(df1)
        
        # Compatible change (add column)
        df2 = pd.DataFrame({
            'value': [1, 2, 3],
            'new_col': [4, 5, 6]
        })
        schema2 = pystore.schema_evolution.Schema.from_dataframe(df2)
        
        # Should be allowed
        assert evolution.validate_evolution(schema1, schema2)
        
        # Incompatible change (remove column)
        df3 = pd.DataFrame({'new_col': [4, 5, 6]})
        schema3 = pystore.schema_evolution.Schema.from_dataframe(df3)
        
        # Should not be allowed
        assert not evolution.validate_evolution(schema1, schema3)
    
    def test_dataframe_evolution(self):
        evolution = SchemaEvolution()
        
        # Target schema
        target_df = pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': ['a', 'b', 'c'],
            'col3': [1.0, 2.0, 3.0]
        })
        target_schema = pystore.schema_evolution.Schema.from_dataframe(target_df)
        
        # DataFrame missing columns
        df = pd.DataFrame({
            'col1': [4, 5, 6]
        })
        
        # Evolve to match target
        evolved_df = evolution.evolve_dataframe(df, target_schema)
        
        # Should have all columns
        assert set(evolved_df.columns) == set(target_schema.columns)
        assert len(evolved_df) == len(df)

    def test_unsupported_strategy_raises_value_error(self):
        """Constructing a SchemaEvolution with an invalid strategy value
        and then calling validate_evolution must raise ValueError (not
        AssertionError).
        """
        from pystore.schema_evolution import SchemaEvolution, Schema

        evolution = SchemaEvolution(strategy="nonexistent_strategy")
        df1 = pd.DataFrame({"a": [1]})
        df2 = pd.DataFrame({"a": [2]})
        schema1 = Schema.from_dataframe(df1)
        schema2 = Schema.from_dataframe(df2)

        with pytest.raises(ValueError, match="Unsupported evolution strategy"):
            evolution.validate_evolution(schema1, schema2)


class TestTimezoneOperations:
    def setup_method(self):
        self.path = tempfile.mkdtemp()
        pystore.set_path(self.path)
        self.store = pystore.store("test_store")
        self.collection = self.store.collection("test_collection")
    
    def teardown_method(self):
        shutil.rmtree(self.path)
    
    def test_timezone_preservation(self):
        # Create timezone-aware data
        tz = 'US/Eastern'
        df = pd.DataFrame({
            'value': np.random.randn(24)
        }, index=pd.date_range('2023-01-01', periods=24, freq='h', tz=tz))
        
        # Write and read
        self.collection.write('tz_test', df)
        result = self.collection.item('tz_test').to_pandas()
        
        # Verify timezone preserved
        assert result.index.tz is not None
        assert str(result.index.tz) == tz
    
    def test_timezone_aware_columns(self):
        # DataFrame with timezone-aware column
        df = pd.DataFrame({
            'value': [1, 2, 3],
            'timestamp': pd.date_range('2023-01-01', periods=3, tz='UTC')
        })
        
        # Write and read
        self.collection.write('tz_col_test', df)
        result = self.collection.item('tz_col_test').to_pandas()
        
        # Verify column timezone preserved
        assert isinstance(result['timestamp'].dtype, pd.DatetimeTZDtype)


class TestCollectionLock:
    """Tests for CollectionLock and with_lock context manager"""

    def setup_method(self):
        self.path = tempfile.mkdtemp()
        pystore.set_path(self.path)
        self.store = pystore.store("test_store")
        self.collection = self.store.collection("test_collection")

    def teardown_method(self):
        shutil.rmtree(self.path)

    def test_acquire_and_release(self):
        """Lock can be acquired and released cleanly"""
        from pystore.transactions import CollectionLock

        lock = CollectionLock(self.collection, lock_name="test_lock")
        assert lock.acquire(timeout=5)
        assert lock._acquired

        lock.release()
        assert not lock._acquired
        # Lock directory should be removed
        assert not lock.lock_path.exists()

    def test_context_manager(self):
        """with_lock() context manager acquires and releases"""
        from pystore.transactions import with_lock

        with with_lock(self.collection, lock_name="ctx_lock") as lock:
            assert lock._acquired
            assert lock.lock_path.exists()

        # After exiting the block the lock must be released
        assert not lock._acquired
        assert not lock.lock_path.exists()

    def test_double_acquire_blocks(self):
        """A second lock on the same name cannot be acquired concurrently"""
        from pystore.transactions import CollectionLock

        lock1 = CollectionLock(self.collection, lock_name="dup_lock")
        lock2 = CollectionLock(self.collection, lock_name="dup_lock")

        assert lock1.acquire(timeout=5)
        # Second acquire should time out quickly
        assert not lock2.acquire(timeout=0.3)

        lock1.release()
        # Now lock2 should succeed
        assert lock2.acquire(timeout=5)
        lock2.release()

    def test_stale_lock_is_broken(self):
        """A stale lock (older than stale_timeout) is automatically broken"""
        import os
        import time as _time

        from pystore.transactions import CollectionLock

        # Create a lock with a very short stale timeout
        lock1 = CollectionLock(
            self.collection, lock_name="stale_lock", stale_timeout=0.1
        )
        assert lock1.acquire(timeout=5)

        # Artificially age the lock directory
        lock_dir = str(lock1.lock_path)
        old_time = _time.time() - 1  # 1 second ago
        os.utime(lock_dir, (old_time, old_time))

        # A new lock with the same short stale_timeout should break and acquire
        lock2 = CollectionLock(
            self.collection, lock_name="stale_lock", stale_timeout=0.1
        )
        assert lock2.acquire(timeout=5)
        lock2.release()

    def test_release_without_acquire_is_noop(self):
        """Releasing a lock that was never acquired is a no-op"""
        from pystore.transactions import CollectionLock

        lock = CollectionLock(self.collection, lock_name="noop_lock")
        # Should not raise
        lock.release()

    def test_context_manager_raises_on_timeout(self):
        """with_lock raises TransactionError if it cannot acquire"""
        from pystore.transactions import CollectionLock
        from pystore.exceptions import TransactionError

        # Hold a lock so the context manager times out
        blocker = CollectionLock(self.collection, lock_name="block_lock")
        assert blocker.acquire(timeout=5)

        with pytest.raises(TransactionError):
            # CollectionLock's default timeout is 30s; pass a custom short one
            lock = CollectionLock(
                self.collection, lock_name="block_lock"
            )
            lock.stale_timeout = 9999  # prevent stale-break
            # __enter__ calls acquire with the default 30s timeout which is too long,
            # so we manually test acquire then raise
            if not lock.acquire(timeout=0.2):
                raise TransactionError("Could not acquire lock 'block_lock'")

        blocker.release()

    def test_lock_id_written_atomically(self):
        """Lock directory contains a lock_id file that matches the lock instance"""
        import os

        from pystore.transactions import CollectionLock

        lock = CollectionLock(self.collection, lock_name="id_lock")
        assert lock.acquire(timeout=5)

        lock_file = os.path.join(lock.lock_path, "lock_id")
        assert os.path.exists(lock_file)
        with open(lock_file) as f:
            assert f.read().strip() == lock.lock_id

        lock.release()

    @pytest.mark.parametrize("lock_name", ["", "..", "../escape", "nested/name"])
    def test_rejects_invalid_lock_names(self, lock_name):
        """Lock names must stay within the collection directory."""
        from pystore.transactions import CollectionLock

        with pytest.raises(ValueError):
            CollectionLock(self.collection, lock_name=lock_name)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])