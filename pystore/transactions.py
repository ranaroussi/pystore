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
from typing import List, Dict, Any, Optional
from contextlib import contextmanager
import pandas as pd
import threading
import uuid

from . import utils
from .logger import get_logger
from .exceptions import StorageError, TransactionError

logger = get_logger(__name__)


class Transaction:
    """Manages a transaction for atomic operations on a collection"""
    
    def __init__(self, collection):
        self.collection = collection
        self.transaction_id = str(uuid.uuid4())
        self.operations = []
        self.temp_dir = None
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
        item_path = self.collection._item_path(item)
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
    """Distributed lock for collection-level operations"""
    
    def __init__(self, collection, lock_name: str = "collection"):
        self.collection = collection
        self.lock_name = lock_name
        self.lock_path = utils.make_path(
            collection.datastore,
            collection.collection,
            f".lock_{lock_name}"
        )
        self.lock_id = str(uuid.uuid4())
        self._acquired = False
    
    def acquire(self, timeout: float = 30.0) -> bool:
        """Acquire the lock with timeout"""
        import time
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                # Try to create lock file atomically
                os.makedirs(self.lock_path, exist_ok=False)
                
                # Write our lock ID
                lock_file = os.path.join(self.lock_path, "lock_id")
                with open(lock_file, 'w') as f:
                    f.write(self.lock_id)
                
                self._acquired = True
                logger.debug(f"Acquired lock '{self.lock_name}'")
                return True
                
            except FileExistsError:
                # Lock is held by someone else
                time.sleep(0.1)
        
        logger.warning(f"Failed to acquire lock '{self.lock_name}' after {timeout}s")
        return False
    
    def release(self):
        """Release the lock"""
        if self._acquired:
            try:
                # Verify we own the lock
                lock_file = os.path.join(self.lock_path, "lock_id")
                if os.path.exists(lock_file):
                    with open(lock_file, 'r') as f:
                        if f.read().strip() == self.lock_id:
                            shutil.rmtree(self.lock_path)
                            logger.debug(f"Released lock '{self.lock_name}'")
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