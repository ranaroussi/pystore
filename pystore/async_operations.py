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
import threading
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
        self._closed = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._loop_thread_id: Optional[int] = None  # Track which thread owns the loop

    def _get_loop(self) -> asyncio.AbstractEventLoop:
        """Get or create event loop.

        When called from within a running event loop the cached ``_loop`` is
        updated so that later calls outside any loop will reuse the same
        instance instead of silently creating a different one.

        If the cached loop is no longer running (e.g. it was stopped or
        closed), it is discarded and a fresh loop is created so that
        ``run_in_executor`` does not raise on a stopped loop.

        .. note::
            ``asyncio`` event loops are not thread-safe.  The cached loop
            must only be used from the thread that created it.  If this
            method is called from a different thread than the one that
            originally cached the loop, a fresh loop is created for the
            current thread instead of reusing the stale one.
        """
        current_thread = threading.current_thread().ident

        try:
            loop = asyncio.get_running_loop()
            self._loop = loop
            self._loop_thread_id = current_thread
            return loop
        except RuntimeError:
            # Discard cached loop if it has been stopped, closed, or
            # belongs to a different thread (asyncio loops are not
            # thread-safe).
            if self._loop is not None:
                if (
                    not self._loop.is_running()
                    or self._loop_thread_id != current_thread
                ):
                    self._loop = None
                    self._loop_thread_id = None
            if self._loop is None:
                self._loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self._loop)
                self._loop_thread_id = current_thread
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

        # Return dict with results or None for failures.
        # Re-raise BaseException subclasses that are not Exception
        # (e.g. KeyboardInterrupt, SystemExit) instead of swallowing them.
        output: dict[str, Optional[pd.DataFrame]] = {}
        for item_name, result in zip(items, results):
            if isinstance(result, BaseException) and not isinstance(result, Exception):
                raise result
            output[item_name] = result if not isinstance(result, Exception) else None
        return output

    async def ordered_append(
        self,
        item: str,
        dataframes: list[pd.DataFrame],
        **kwargs: Any,
    ) -> None:
        """Append multiple DataFrames to the same item sequentially.

        ``collection.append()`` now uses a per-item lock to prevent concurrent
        swap races, but same-item appends are still serialized here so the
        caller gets deterministic ordering across the provided DataFrames.
        """
        logger.debug(f"Starting sequential append of {len(dataframes)} DataFrames to '{item}'")
        for df in dataframes:
            await self.append(item, df, **kwargs)
        logger.debug(f"Completed sequential append to '{item}'")

    # Backward-compatible alias — the method was renamed from
    # ``parallel_append`` to ``ordered_append`` to reflect the sequential
    # behaviour introduced during the modernization effort.
    async def parallel_append(self, *args, **kwargs):
        """Deprecated: use ordered_append instead.

        The old name was misleading because the method has always been
        sequential; it was renamed to ``ordered_append`` to avoid
        confusion.
        """
        import warnings

        warnings.warn(
            "parallel_append is deprecated — use ordered_append instead. "
            "The old name was misleading because the method is sequential.",
            DeprecationWarning,
            stacklevel=2,
        )
        return await self.ordered_append(*args, **kwargs)

    def close(self):
        """Close the executor and event loop.

        Safe to call multiple times — subsequent calls are no-ops.
        """
        if self._closed:
            return
        self._closed = True
        self.executor.shutdown(wait=True)
        if self._loop is not None and not self._loop.is_running():
            self._loop.close()
            self._loop = None


class AsyncStore:
    """Async wrapper for PyStore Store operations"""

    def __init__(self, store, executor: Optional[concurrent.futures.Executor] = None):
        self.store = store
        self.executor = executor or concurrent.futures.ThreadPoolExecutor(max_workers=4)
        self._closed = False

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
        """Close the executor.

        Safe to call multiple times — subsequent calls are no-ops.
        """
        if self._closed:
            return
        self._closed = True
        self.executor.shutdown(wait=True)


# Convenience functions for async context managers
class AsyncContextManager:
    """Context manager for async PyStore operations"""

    def __init__(self, store_or_collection):
        self.sync_obj = store_or_collection
        self.async_obj: Optional[Any] = None
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)

    async def __aenter__(self):
        from .store import store as Store

        if isinstance(self.sync_obj, Store):
            self.async_obj = AsyncStore(self.sync_obj, self.executor)
        else:
            self.async_obj = AsyncCollection(self.sync_obj, self.executor)
        return self.async_obj

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Close the async wrapper — this shuts down the shared executor
        # and (for AsyncCollection) the event loop.  Do not call
        # self.executor.shutdown() again here; the executor is shared
        # and close() already shuts it down.
        if self.async_obj is not None and hasattr(self.async_obj, "close"):
            self.async_obj.close()


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
