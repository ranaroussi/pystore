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
Async/await support for PyStore operations
"""

import asyncio
import concurrent.futures
from functools import partial
from typing import Any, Optional, Union, cast

import dask.dataframe as dd
import pandas as pd

from .logger import get_logger

logger = get_logger(__name__)


class AsyncCollection:
    """Async wrapper for PyStore Collection operations"""

    def __init__(
        self,
        collection,
        executor: Optional[concurrent.futures.Executor] = None,
    ):
        self.collection = collection
        self.executor = executor or concurrent.futures.ThreadPoolExecutor(max_workers=4)
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def _get_loop(self) -> asyncio.AbstractEventLoop:
        """Get or create event loop"""
        try:
            return asyncio.get_running_loop()
        except RuntimeError:
            if self._loop is None:
                self._loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self._loop)
            return self._loop

    async def write(
        self,
        item: str,
        data: Union[pd.DataFrame, dd.DataFrame],
        metadata: Optional[dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        """Async version of collection.write()"""
        loop = self._get_loop()
        write_func = partial(
            self.collection.write,
            item,
            data,
            metadata=metadata or {},
            **kwargs,
        )

        logger.debug(f"Starting async write for item '{item}'")
        await loop.run_in_executor(self.executor, write_func)
        logger.debug(f"Completed async write for item '{item}'")

    async def read(self, item: str, **kwargs: Any) -> pd.DataFrame:
        """Async version of reading an item"""
        loop = self._get_loop()

        def read_item() -> pd.DataFrame:
            return cast(pd.DataFrame, self.collection.item(item, **kwargs).to_pandas())

        logger.debug(f"Starting async read for item '{item}'")
        result = await loop.run_in_executor(self.executor, read_item)
        logger.debug(f"Completed async read for item '{item}'")
        return result

    async def append(self, item: str, data: pd.DataFrame, **kwargs: Any) -> None:
        """Async version of collection.append()"""
        loop = self._get_loop()
        append_func = partial(self.collection.append, item, data, **kwargs)

        logger.debug(f"Starting async append for item '{item}'")
        await loop.run_in_executor(self.executor, append_func)
        logger.debug(f"Completed async append for item '{item}'")

    async def delete(self, item: str, **kwargs: Any) -> bool:
        """Async version of collection.delete_item()"""
        loop = self._get_loop()
        delete_func = partial(self.collection.delete_item, item, **kwargs)

        logger.debug(f"Starting async delete for item '{item}'")
        result = await loop.run_in_executor(self.executor, delete_func)
        logger.debug(f"Completed async delete for item '{item}'")
        return cast(bool, result)

    async def list_items(self, **kwargs: Any) -> set[Any]:
        """Async version of collection.list_items()"""
        loop = self._get_loop()
        list_func = partial(self.collection.list_items, **kwargs)

        logger.debug("Starting async list_items")
        result = await loop.run_in_executor(self.executor, list_func)
        logger.debug("Completed async list_items")
        return cast(set[Any], result)

    async def write_batch(
        self,
        items_data: dict[str, pd.DataFrame],
        **kwargs: Any,
    ) -> None:
        """Async batch write multiple items concurrently"""
        tasks = [self.write(name, data, **kwargs) for name, data in items_data.items()]

        logger.debug(f"Starting async batch write for {len(tasks)} items")
        await asyncio.gather(*tasks)
        logger.debug(f"Completed async batch write for {len(tasks)} items")

    async def read_batch(
        self,
        items: list[str],
        **kwargs: Any,
    ) -> dict[str, Optional[pd.DataFrame]]:
        """Async batch read multiple items concurrently"""
        tasks = [self.read(item, **kwargs) for item in items]

        logger.debug(f"Starting async batch read for {len(items)} items")
        results = await asyncio.gather(*tasks, return_exceptions=True)
        logger.debug(f"Completed async batch read for {len(items)} items")

        # Return dict with results or None for failures
        return {
            item: result if not isinstance(result, BaseException) else None
            for item, result in zip(items, results)
        }

    async def parallel_append(
        self,
        item: str,
        dataframes: list[pd.DataFrame],
        **kwargs: Any,
    ) -> None:
        """Append multiple DataFrames to same item in parallel"""
        tasks = [self.append(item, df, **kwargs) for df in dataframes]

        logger.debug(f"Starting parallel append of {len(dataframes)} DataFrames to '{item}'")
        await asyncio.gather(*tasks)
        logger.debug(f"Completed parallel append to '{item}'")

    def close(self):
        """Close the executor"""
        self.executor.shutdown(wait=True)
        if self._loop is not None and not self._loop.is_running():
            self._loop.close()


class AsyncStore:
    """Async wrapper for PyStore Store operations"""

    def __init__(self, store, executor: Optional[concurrent.futures.Executor] = None):
        self.store = store
        self.executor = executor or concurrent.futures.ThreadPoolExecutor(max_workers=4)

    def collection(self, name: str) -> AsyncCollection:
        """Get async collection wrapper"""
        sync_collection = self.store.collection(name)
        return AsyncCollection(sync_collection, self.executor)

    async def list_collections(self) -> set:
        """Async list collections"""
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(self.executor, self.store.list_collections)
        return cast(set[Any], result)

    def close(self):
        """Close the executor"""
        self.executor.shutdown(wait=True)


# Convenience functions for async context managers
class AsyncContextManager:
    """Context manager for async PyStore operations"""

    def __init__(self, store_or_collection):
        self.sync_obj = store_or_collection
        self.async_obj: Optional[Any] = None
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)

    async def __aenter__(self):
        # Check class name to distinguish between Store and Collection
        if self.sync_obj.__class__.__name__ == "store":  # It's a store
            self.async_obj = AsyncStore(self.sync_obj, self.executor)
        else:  # It's a collection
            self.async_obj = AsyncCollection(self.sync_obj, self.executor)
        return self.async_obj

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.executor.shutdown(wait=True)


def async_pystore(store_or_collection: Any) -> AsyncContextManager:
    """Create async context manager for PyStore operations

    Usage:
        async with async_pystore(store) as async_store:
            async_collection = async_store.collection('mycoll')
            await async_collection.write('item', df)

    Or:
        async with async_pystore(collection) as async_collection:
            await async_collection.write('item', df)
    """
    return AsyncContextManager(store_or_collection)


# Example usage functions
async def example_concurrent_writes():
    """Example of concurrent writes"""
    import pystore

    store = pystore.store('mystore')
    collection = store.collection('mycollection')

    async with async_pystore(collection) as async_coll:
        # Create sample data
        data = {
            f'item_{i}': pd.DataFrame({
                'value': range(100),
                'timestamp': pd.date_range('2023-01-01', periods=100)
            })
            for i in range(5)
        }

        # Write all items concurrently
        await async_coll.write_batch(data)


async def example_concurrent_reads():
    """Example of concurrent reads"""
    import pystore

    store = pystore.store('mystore')
    collection = store.collection('mycollection')

    async with async_pystore(collection) as async_coll:
        # Read multiple items concurrently
        items = [f'item_{i}' for i in range(5)]
        results = await async_coll.read_batch(items)

        for item, df in results.items():
            if df is not None:
                print(f"{item}: {len(df)} rows")
