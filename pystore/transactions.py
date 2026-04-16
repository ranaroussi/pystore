#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# PyStore: Flat-file datastore for timeseries data
# https://github.com/ranaroussi/pystore
#
# Copyright 2018-2025 Ran Aroussi
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Transaction support for PyStore with context managers
"""

import os
import shutil
import tempfile
import threading
import uuid
from contextlib import contextmanager
from typing import Optional

import pandas as pd

from . import utils
from .exceptions import TransactionError
from .logger import get_logger

logger = get_logger(__name__)


class Transaction:
    """Manages a transaction for atomic operations on a collection"""

    def __init__(self, collection):
        self.collection = collection
        self.transaction_id = str(uuid.uuid4())
        self.operations = []
        self.temp_dir: Optional[str] = None
        self.backups = {}
        self.lock = threading.Lock()
        self._committed = False
        self._rolled_back = False

    def _ensure_temp_dir(self):
        """Create temporary directory for transaction"""
        if self.temp_dir is None:
            self.temp_dir = tempfile.mkdtemp(
                prefix=f"txn_{self.transaction_id}_",
                dir=utils.make_path(self.collection.datastore, self.collection.collection)
            )
            logger.debug(f"Created transaction directory: {self.temp_dir}")

    def write(self, item: str, data: pd.DataFrame, **kwargs):
        """Add write operation to transaction"""
        if self._committed or self._rolled_back:
            raise TransactionError("Transaction already completed")

        with self.lock:
            self._ensure_temp_dir()
            self.operations.append({
                'type': 'write',
                'item': item,
                'data': data.copy(),
                'kwargs': kwargs
            })
            logger.debug(f"Added write operation for item '{item}' to transaction")

    def append(self, item: str, data: pd.DataFrame, **kwargs):
        """Add append operation to transaction"""
        if self._committed or self._rolled_back:
            raise TransactionError("Transaction already completed")

        with self.lock:
            self._ensure_temp_dir()
            self.operations.append({
                'type': 'append',
                'item': item,
                'data': data.copy(),
                'kwargs': kwargs
            })
            logger.debug(f"Added append operation for item '{item}' to transaction")

    def delete(self, item: str):
        """Add delete operation to transaction"""
        if self._committed or self._rolled_back:
            raise TransactionError("Transaction already completed")

        with self.lock:
            self._ensure_temp_dir()
            self.operations.append({
                'type': 'delete',
                'item': item
            })
            logger.debug(f"Added delete operation for item '{item}' to transaction")

    def _backup_item(self, item: str):
        """Create backup of existing item"""
        if self.temp_dir is None:
            raise TransactionError("Transaction temp directory is not initialized")
        item_path = self.collection.get_item_path(item)
        if utils.path_exists(item_path):
            backup_path = os.path.join(self.temp_dir, f"backup_{item}")
            shutil.copytree(item_path, backup_path)
            self.backups[item] = backup_path
            logger.debug(f"Created backup of item '{item}'")

    def commit(self):
        """Commit all operations in the transaction"""
        if self._committed:
            raise TransactionError("Transaction already committed")
        if self._rolled_back:
            raise TransactionError("Transaction already rolled back")

        with self.lock:
            logger.info(f"Committing transaction {self.transaction_id} with {len(self.operations)} operations")

            try:
                # Create backups for all affected items
                affected_items = set()
                for op in self.operations:
                    item = op['item']
                    if item not in self.backups and op['type'] != 'write':
                        self._backup_item(item)
                    affected_items.add(item)

                # Execute all operations
                for i, op in enumerate(self.operations):
                    logger.debug(f"Executing operation {i+1}/{len(self.operations)}: {op['type']} on '{op['item']}'")

                    if op['type'] == 'write':
                        self.collection.write(op['item'], op['data'], **op['kwargs'])
                    elif op['type'] == 'append':
                        self.collection.append(op['item'], op['data'], **op['kwargs'])
                    elif op['type'] == 'delete':
                        self.collection.delete_item(op['item'])

                self._committed = True
                logger.info(f"Transaction {self.transaction_id} committed successfully")

            except Exception as e:
                logger.error(f"Transaction {self.transaction_id} failed: {e}")
                self._rollback_internal()
                raise TransactionError(f"Transaction failed: {str(e)}") from e
            finally:
                self._cleanup()

    def rollback(self):
        """Rollback the transaction"""
        if self._committed:
            raise TransactionError("Cannot rollback committed transaction")
        if self._rolled_back:
            raise TransactionError("Transaction already rolled back")

        with self.lock:
            logger.info(f"Rolling back transaction {self.transaction_id}")
            self._rollback_internal()
            self._rolled_back = True

    def _rollback_internal(self):
        """Internal rollback logic"""
        # Restore backups
        for item, backup_path in self.backups.items():
            item_path = self.collection._item_path(item)
            if utils.path_exists(item_path):
                shutil.rmtree(item_path)
            shutil.move(backup_path, item_path)
            logger.debug(f"Restored backup for item '{item}'")

    def _cleanup(self):
        """Clean up transaction resources"""
        if self.temp_dir and os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
            logger.debug(f"Cleaned up transaction directory: {self.temp_dir}")


class BatchTransaction:
    """Optimized transaction for batch operations"""

    def __init__(self, collection):
        self.collection = collection
        self.writes = {}
        self.appends = {}
        self.deletes = set()
        self._committed = False

    def write(self, item: str, data: pd.DataFrame, **kwargs):
        """Add write to batch"""
        if self._committed:
            raise TransactionError("Batch already committed")
        self.writes[item] = (data.copy(), kwargs)

    def append(self, item: str, data: pd.DataFrame, **kwargs):
        """Add append to batch"""
        if self._committed:
            raise TransactionError("Batch already committed")
        if item not in self.appends:
            self.appends[item] = []
        self.appends[item].append((data.copy(), kwargs))

    def delete(self, item: str):
        """Add delete to batch"""
        if self._committed:
            raise TransactionError("Batch already committed")
        self.deletes.add(item)

    def commit(self):
        """Commit all batch operations"""
        if self._committed:
            raise TransactionError("Batch already committed")

        logger.info(f"Committing batch transaction: {len(self.writes)} writes, "
                   f"{len(self.appends)} appends, {len(self.deletes)} deletes")

        # Use regular transaction for atomicity
        with transaction(self.collection) as txn:
            # Process deletes first
            for item in self.deletes:
                txn.delete(item)

            # Process writes
            for item, (data, kwargs) in self.writes.items():
                txn.write(item, data, **kwargs)

            # Process appends (combine multiple appends per item)
            for item, append_list in self.appends.items():
                # Combine all DataFrames for the item
                if len(append_list) == 1:
                    data, kwargs = append_list[0]
                    txn.append(item, data, **kwargs)
                else:
                    # Combine multiple appends
                    dfs = [data for data, _ in append_list]
                    combined = pd.concat(dfs, ignore_index=False)
                    kwargs = append_list[0][1]  # Use kwargs from first append
                    txn.append(item, combined, **kwargs)

        self._committed = True


@contextmanager
def transaction(collection):
    """Context manager for transactional operations

    Usage:
        with transaction(collection) as txn:
            txn.write('item1', df1)
            txn.append('item2', df2)
            txn.delete('item3')
        # All operations committed atomically on exit
    """
    txn = Transaction(collection)
    try:
        yield txn
        txn.commit()
    except Exception:
        if not txn._committed:
            txn.rollback()
        raise


@contextmanager
def batch_transaction(collection):
    """Context manager for batch transactional operations

    Usage:
        with batch_transaction(collection) as batch:
            batch.write('item1', df1)
            batch.write('item2', df2)
            batch.append('item3', df3)
        # All operations committed together
    """
    batch = BatchTransaction(collection)
    try:
        yield batch
        batch.commit()
    except Exception:
        logger.error("Batch transaction failed")
        raise


class CollectionLock:
    """Distributed lock for collection-level operations.

    Uses filesystem directory creation (``os.makedirs(exist_ok=False)``) as an
    atomic lock primitive.  Improvements over the naive approach:

    * **Stale lock detection** — if the lock directory exists but is older than
      ``stale_timeout`` seconds, it is assumed to be from a crashed process and
      is forcibly removed before retrying.
    * **Atomic owner identification** — the lock_id is written to a temporary
      file and renamed into the lock directory so that ownership is never in an
      unknown state.
    * **Explicit release validation** — ``release()`` raises if the lock was
      externally removed instead of silently succeeding.
    """

    def __init__(
        self,
        collection,
        lock_name: str = "collection",
        stale_timeout: float = 300.0,
    ):
        self.collection = collection
        self.lock_name = utils.validate_identifier(lock_name, "Lock")
        self.lock_path = utils.make_path(
            collection.datastore,
            collection.collection,
            f".lock_{self.lock_name}",
        )
        self.lock_id = str(uuid.uuid4())
        self.stale_timeout = stale_timeout
        self._acquired = False

    def _is_stale(self) -> bool:
        """Check whether an existing lock directory is stale."""
        import time as _time

        try:
            lock_dir = str(self.lock_path)
            mtime = os.path.getmtime(lock_dir)
            return (_time.time() - mtime) > self.stale_timeout
        except OSError:
            return False

    def _break_stale_lock(self) -> None:
        """Remove a stale lock directory."""
        try:
            shutil.rmtree(self.lock_path)
            logger.warning(
                f"Removed stale lock '{self.lock_name}' "
                f"(older than {self.stale_timeout}s)"
            )
        except OSError:
            pass  # Another process may have cleaned it up already

    def acquire(self, timeout: float = 30.0) -> bool:
        """Acquire the lock with timeout"""
        import time
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                # Try to create lock directory atomically
                os.makedirs(self.lock_path, exist_ok=False)

                # Write lock_id atomically via tmp-file + rename so the owner
                # is never in an unknown state.
                lock_file = os.path.join(self.lock_path, "lock_id")
                tmp_lock_file = lock_file + f".{os.getpid()}.tmp"
                try:
                    with open(tmp_lock_file, "w") as f:
                        f.write(self.lock_id)
                    os.replace(tmp_lock_file, lock_file)
                except Exception:
                    # If writing the lock_id fails, release the directory
                    try:
                        shutil.rmtree(self.lock_path)
                    except OSError:
                        pass
                    raise

                self._acquired = True
                logger.debug(f"Acquired lock '{self.lock_name}'")
                return True

            except FileExistsError:
                # Lock is held — check for staleness
                if self._is_stale():
                    self._break_stale_lock()
                    continue  # retry immediately
                time.sleep(0.1)

        logger.warning(f"Failed to acquire lock '{self.lock_name}' after {timeout}s")
        return False

    def release(self) -> None:
        """Release the lock.

        Raises ``TransactionError`` if the lock directory was removed externally
        (i.e., we no longer own it).
        """
        if not self._acquired:
            return

        try:
            lock_file = os.path.join(self.lock_path, "lock_id")
            if not os.path.exists(lock_file):
                raise TransactionError(
                    f"Lock '{self.lock_name}' was removed externally"
                )
            with open(lock_file) as f:
                owner = f.read().strip()
            if owner != self.lock_id:
                raise TransactionError(
                    f"Lock '{self.lock_name}' is owned by another process"
                )
            shutil.rmtree(self.lock_path)
            logger.debug(f"Released lock '{self.lock_name}'")
        except TransactionError:
            raise
        except Exception as e:
            logger.error(f"Error releasing lock: {e}")
        finally:
            self._acquired = False

    def __enter__(self):
        if not self.acquire():
            raise TransactionError(f"Could not acquire lock '{self.lock_name}'")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()


def with_lock(collection, lock_name: str = "collection"):
    """Context manager for operations with collection lock

    Usage:
        with with_lock(collection):
            # Exclusive operations on collection
            collection.write('item', df)
    """
    return CollectionLock(collection, lock_name)
