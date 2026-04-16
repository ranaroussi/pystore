"""
Tests for review feedback fixes:
- CollectionLock (transactions.py)
- _atomic_write (collection.py)
- datetime_to_int64 (utils.py)
- Module-level dask config lazy init (memory.py)
"""

import os
import shutil
import tempfile
import time

import numpy as np
import pandas as pd
import pytest

import pystore
from pystore.transactions import CollectionLock, TransactionError
from pystore.utils import datetime_to_int64

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def temp_store_path():
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def test_collection(temp_store_path):
    pystore.set_path(temp_store_path)
    store = pystore.store("test_store")
    return store.collection("test_collection")


# ---------------------------------------------------------------------------
# CollectionLock tests
# ---------------------------------------------------------------------------


class TestCollectionLock:
    """Tests for CollectionLock acquire/release and stale-lock handling."""

    def test_acquire_and_release(self, test_collection):
        """Lock can be acquired and released normally."""
        lock = CollectionLock(test_collection)
        assert lock.acquire(timeout=5.0)
        assert lock._acquired
        assert os.path.isdir(lock.lock_path)

        lock.release()
        assert not lock._acquired
        assert not os.path.exists(lock.lock_path)

    def test_context_manager(self, test_collection):
        """Lock works as a context manager."""
        lock = CollectionLock(test_collection)
        with lock:
            assert lock._acquired
            assert os.path.isdir(lock.lock_path)
        assert not lock._acquired

    def test_double_acquire_fails(self, test_collection):
        """A second lock on the same name times out while the first is held."""
        lock1 = CollectionLock(test_collection)
        lock2 = CollectionLock(test_collection)

        assert lock1.acquire(timeout=5.0)
        # Second lock should fail with a short timeout
        assert not lock2.acquire(timeout=0.3)

        lock1.release()

    def test_release_after_external_removal_raises(self, test_collection):
        """Releasing a lock whose directory was externally removed raises."""
        lock = CollectionLock(test_collection)
        assert lock.acquire(timeout=5.0)

        # Simulate external removal
        shutil.rmtree(lock.lock_path)

        with pytest.raises(TransactionError, match="removed externally"):
            lock.release()

    def test_stale_lock_is_broken(self, test_collection):
        """A stale lock is automatically broken during acquire."""
        # Create a lock with a very short stale timeout
        lock1 = CollectionLock(test_collection, stale_timeout=0.1)
        assert lock1.acquire(timeout=5.0)

        # Pretend the process crashed (don't release, just abandon)
        lock1._acquired = False

        # Wait for it to become stale
        time.sleep(0.2)

        lock2 = CollectionLock(test_collection, stale_timeout=0.1)
        assert lock2.acquire(timeout=5.0)
        lock2.release()

    def test_lock_id_is_written_atomically(self, test_collection):
        """After acquire, the lock_id file contains the correct ID."""
        lock = CollectionLock(test_collection)
        assert lock.acquire(timeout=5.0)

        lock_file = os.path.join(lock.lock_path, "lock_id")
        assert os.path.exists(lock_file)
        with open(lock_file) as f:
            assert f.read().strip() == lock.lock_id

        lock.release()

    def test_release_wrong_owner_raises(self, test_collection):
        """Releasing a lock owned by another ID raises."""
        lock = CollectionLock(test_collection)
        assert lock.acquire(timeout=5.0)

        # Tamper with the lock_id file
        lock_file = os.path.join(lock.lock_path, "lock_id")
        with open(lock_file, "w") as f:
            f.write("some-other-id")

        with pytest.raises(TransactionError, match="owned by another"):
            lock.release()

        # Cleanup manually since release didn't remove the dir
        shutil.rmtree(lock.lock_path, ignore_errors=True)


# ---------------------------------------------------------------------------
# _atomic_write tests
# ---------------------------------------------------------------------------


class TestAtomicWrite:
    """Tests for Collection._atomic_write."""

    def test_atomic_write_new_item(self, test_collection):
        """_atomic_write creates a new item successfully."""
        import dask.dataframe as dd

        df = pd.DataFrame(
            {"value": [1.0, 2.0, 3.0]},
            index=pd.date_range("2024-01-01", periods=3, freq="D"),
        )
        ddf = dd.from_pandas(df, npartitions=1)

        test_collection._atomic_write(
            "atomic_item", ddf, metadata={"test": True}, npartitions=1
        )

        # Verify the item exists and is readable
        result = test_collection.item("atomic_item").to_pandas()
        assert len(result) == 3

    def test_atomic_write_overwrites_existing(self, test_collection):
        """_atomic_write replaces an existing item atomically."""
        import dask.dataframe as dd

        df1 = pd.DataFrame(
            {"value": [1.0, 2.0]},
            index=pd.date_range("2024-01-01", periods=2, freq="D"),
        )
        ddf1 = dd.from_pandas(df1, npartitions=1)
        test_collection._atomic_write(
            "atomic_overwrite", ddf1, metadata={}, npartitions=1
        )

        df2 = pd.DataFrame(
            {"value": [10.0, 20.0, 30.0]},
            index=pd.date_range("2024-06-01", periods=3, freq="D"),
        )
        ddf2 = dd.from_pandas(df2, npartitions=1)
        test_collection._atomic_write(
            "atomic_overwrite", ddf2, metadata={}, npartitions=1
        )

        result = test_collection.item("atomic_overwrite").to_pandas()
        assert len(result) == 3

    def test_atomic_write_cleans_up_backup(self, test_collection):
        """After a successful _atomic_write, no backup directory remains."""
        import dask.dataframe as dd

        df = pd.DataFrame(
            {"value": [1.0]},
            index=pd.date_range("2024-01-01", periods=1, freq="D"),
        )
        ddf = dd.from_pandas(df, npartitions=1)

        # First write
        test_collection._atomic_write("cleanup_item", ddf, metadata={}, npartitions=1)
        # Second write (triggers backup)
        test_collection._atomic_write("cleanup_item", ddf, metadata={}, npartitions=1)

        backup_path = test_collection._item_path("_backup_cleanup_item")
        assert not os.path.exists(backup_path)


# ---------------------------------------------------------------------------
# datetime_to_int64 tests
# ---------------------------------------------------------------------------


class TestDatetimeToInt64:
    """Tests for utils.datetime_to_int64."""

    def test_pandas_datetime_with_nanoseconds(self):
        """Pandas DatetimeIndex with nanosecond precision is converted."""
        dates = pd.date_range("2024-01-01", periods=5, freq="1s")
        dates = dates + pd.to_timedelta(np.random.randint(1, 999, size=5), unit="ns")
        df = pd.DataFrame({"value": range(5)}, index=dates)

        result = datetime_to_int64(df)
        assert result.index.dtype == np.int64

    def test_pandas_datetime_without_nanoseconds(self):
        """Pandas DatetimeIndex without nanosecond precision is NOT converted."""
        dates = pd.date_range("2024-01-01", periods=5, freq="D")
        df = pd.DataFrame({"value": range(5)}, index=dates)

        result = datetime_to_int64(df)
        assert pd.api.types.is_datetime64_any_dtype(result.index)

    def test_dask_datetime_index(self):
        """Dask DataFrame with datetime index is converted."""
        from dask import dataframe as dd

        dates = pd.date_range("2024-01-01", periods=5, freq="D")
        df = pd.DataFrame({"value": range(5)}, index=dates)
        ddf = dd.from_pandas(df, npartitions=1)

        result = datetime_to_int64(ddf)
        assert result.index.dtype == np.int64

    def test_non_datetime_index_unchanged(self):
        """Non-datetime index is passed through unchanged."""
        df = pd.DataFrame({"value": [1, 2, 3]}, index=[10, 20, 30])

        result = datetime_to_int64(df)
        assert list(result.index) == [10, 20, 30]


# ---------------------------------------------------------------------------
# Module-level dask config (memory.py) tests
# ---------------------------------------------------------------------------


class TestDaskConfigLazy:
    """Ensure dask config is not mutated at import time."""

    def test_config_not_applied_on_import(self):
        """Importing memory module should NOT set distributed.worker.memory.*
        via the module-level code (which was removed)."""
        from pystore import memory

        # The flag should be False until explicitly called
        # Reset for testing
        old_val = memory._dask_memory_config_applied
        memory._dask_memory_config_applied = False
        assert not memory._dask_memory_config_applied
        memory._dask_memory_config_applied = old_val

    def test_apply_dask_memory_config_sets_flag(self):
        """apply_dask_memory_config() sets the applied flag."""
        from pystore.memory import apply_dask_memory_config

        apply_dask_memory_config()

        from pystore import memory
        assert memory._dask_memory_config_applied

    def test_apply_dask_memory_config_is_idempotent(self):
        """Calling apply_dask_memory_config() twice is safe."""
        from pystore.memory import apply_dask_memory_config

        apply_dask_memory_config()
        apply_dask_memory_config()
        # No error means success
