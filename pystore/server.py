#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# PyStore: Flat-file datastore for timeseries data
# https://github.com/ranaroussi/pystore
#
# Copyright 2018-2020 Ran Aroussi
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

"""PyStore API Server with rate limiting middleware."""

import os
import json
import functools
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, HTTPException, Request, Response, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

import pystore
from pystore import store as pystore_store
from pystore.server_config import (
    CORS_ORIGINS,
    default_rate_limit_config,
    STORAGE_PATH,
    RATE_LIMIT_ENABLED
)


# Request models
class WriteItemRequest(BaseModel):
    """Request model for writing item data."""
    data: List[Dict[str, Any]]
    metadata: Optional[Dict[str, Any]] = None


class AppendItemRequest(BaseModel):
    """Request model for appending data to an item."""
    data: List[Dict[str, Any]]


class UpdateMetadataRequest(BaseModel):
    """Request model for updating item metadata."""
    metadata: Dict[str, Any]


# Parquet engine to use (pyarrow for compatibility with newer dask)
PARQUET_ENGINE = "pyarrow"

# Set storage path if configured
if STORAGE_PATH:
    pystore.set_path(STORAGE_PATH)

# Initialize rate limiter
limiter = Limiter(key_func=get_remote_address)

app = FastAPI(
    title="PyStore API",
    description="Fast data store for Pandas timeseries data",
    version="0.1.22"
)

# Add state for limiter
app.state.limiter = limiter
def is_test_environment() -> bool:
    """Return True when running under pytest."""
    return "PYTEST_CURRENT_TEST" in os.environ


def is_rate_limit_enabled() -> bool:
    """Determine if rate limiting should be enabled for this request."""
    return os.environ.get("PYSTORE_RATE_LIMIT_ENABLED", "true").lower() == "true" and not is_test_environment()


def ensure_storage_path() -> None:
    """Ensure PyStore uses the current storage path from environment."""
    storage_path = os.environ.get("PYSTORE_PATH")
    if storage_path:
        pystore.set_path(storage_path)


def rate_limit(limit: str):
    """Apply rate limiting only when enabled."""
    def decorator(func):
        limited = limiter.limit(limit)(func)

        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            if app.state.rate_limit_enabled:
                return await limited(*args, **kwargs)
            return await func(*args, **kwargs)

        return wrapper

    return decorator


app.state.rate_limit_enabled = is_rate_limit_enabled()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def configure_request_context(request: Request, call_next):
    """Ensure request-scoped config for storage path, rate limiting, and CORS headers."""
    app.state.rate_limit_enabled = is_rate_limit_enabled()
    ensure_storage_path()
    response = await call_next(request)
    if "access-control-allow-origin" not in response.headers:
        response.headers["access-control-allow-origin"] = CORS_ORIGINS[0] if CORS_ORIGINS else "*"
    return response


# Custom rate limit exceeded handler
@app.exception_handler(RateLimitExceeded)
async def rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded):
    """Handle rate limit exceeded errors."""
    response = JSONResponse(
        status_code=429,
        content={
            "error": "Rate limit exceeded",
            "message": str(exc.detail),
            "retry_after": exc.detail
        }
    )
    response.headers["Retry-After"] = str(exc.detail)
    response.headers["X-RateLimit-Limit"] = "60"
    response.headers["X-RateLimit-Remaining"] = "0"
    return response


# Helper function to get rate limit key
def get_rate_limit_key(request: Request, endpoint_type: str = "read") -> str:
    """Get rate limit key based on request."""
    return get_remote_address(request)


# Health check endpoint
@app.get("/health")
@rate_limit(default_rate_limit_config.list)
async def health_check(request: Request):
    """Health check endpoint."""
    return {"status": "healthy", "version": "0.1.22"}


# Index endpoint
@app.get("/")
@rate_limit(default_rate_limit_config.list)
async def index(request: Request):
    """API index endpoint."""
    return {
        "name": "PyStore API",
        "version": "0.1.22",
        "endpoints": {
            "health": "/health",
            "stores": "/stores",
            "collections": "/stores/{store}/collections",
            "items": "/stores/{store}/collections/{collection}/items",
            "snapshots": "/stores/{store}/collections/{collection}/snapshots"
        }
    }


# ============================================
# Store Management Endpoints
# ============================================

@app.get("/stores")
@rate_limit(default_rate_limit_config.list)
async def list_stores(request: Request):
    """List all stores."""
    stores = pystore.list_stores()
    return {"stores": stores}


@app.post("/stores/{store_name}")
@rate_limit(default_rate_limit_config.write)
async def create_store(request: Request, store_name: str):
    """Create a new store."""
    # Creating a store is done by connecting to it
    s = pystore.store(store_name, engine=PARQUET_ENGINE)
    return {"store": store_name, "created": True}


@app.delete("/stores/{store_name}")
@rate_limit(default_rate_limit_config.delete)
async def delete_store_endpoint(request: Request, store_name: str):
    """Delete a store."""
    pystore.delete_store(store_name)
    return {"store": store_name, "deleted": True}


# ============================================
# Collection Management Endpoints
# ============================================

@app.get("/stores/{store}/collections")
@rate_limit(default_rate_limit_config.list)
async def list_collections(request: Request, store: str):
    """List all collections in a store."""
    s = pystore.store(store, engine=PARQUET_ENGINE)
    collections = s.list_collections()
    return {"store": store, "collections": collections}


@app.post("/stores/{store}/collections/{collection}")
@rate_limit(default_rate_limit_config.write)
async def create_collection(request: Request, store: str, collection: str):
    """Create a new collection."""
    s = pystore.store(store, engine=PARQUET_ENGINE)
    col = s.collection(collection)
    return {"store": store, "collection": collection, "created": True}


@app.delete("/stores/{store}/collections/{collection}")
@rate_limit(default_rate_limit_config.delete)
async def delete_collection(request: Request, store: str, collection: str):
    """Delete a collection."""
    s = pystore.store(store, engine=PARQUET_ENGINE)
    s.delete_collection(collection)
    return {"store": store, "collection": collection, "deleted": True}


# ============================================
# Item Data Endpoints
# ============================================

@app.get("/stores/{store}/collections/{collection}/items")
@rate_limit(default_rate_limit_config.list)
async def list_items(request: Request, store: str, collection: str, **kwargs):
    """List all items in a collection with optional metadata filtering."""
    s = pystore.store(store, engine=PARQUET_ENGINE)
    col = s.collection(collection)
    
    # Extract query parameters for metadata filtering
    filters = {k: v for k, v in kwargs.items() if v is not None and k not in ['snapshot']}
    
    items = col.list_items(**filters) if filters else col.list_items()
    return {"store": store, "collection": collection, "items": items}


@app.get("/stores/{store}/collections/{collection}/items/{item}")
@rate_limit(default_rate_limit_config.read)
async def read_item(
    request: Request,
    store: str,
    collection: str,
    item: str,
    columns: Optional[str] = None,
    filter: Optional[str] = None,
    snapshot: Optional[str] = None
):
    """Read item data."""
    s = pystore.store(store, engine=PARQUET_ENGINE)
    col = s.collection(collection)
    
    # Get the item
    item_obj = col.item(item, snapshot=snapshot)
    
    # Convert to pandas DataFrame
    df = item_obj.to_pandas()
    
    # Apply column selection
    if columns:
        col_list = columns.split(",")
        df = df[col_list]
    
    # Apply filter expression
    if filter:
        # Simple filter implementation - could be enhanced
        df = df.query(filter)
    
    # Convert to JSON-compatible format
    data = df.to_dict(orient="records")
    
    return {
        "store": store,
        "collection": collection,
        "item": item,
        "data": data,
        "metadata": item_obj.metadata,
        "rows": len(data)
    }


@app.post("/stores/{store}/collections/{collection}/items/{item}")
@rate_limit(default_rate_limit_config.write)
async def write_item(
    request: Request,
    store: str,
    collection: str,
    item: str,
    body: WriteItemRequest = Body(...)
):
    """Write item data."""
    import pandas as pd
    
    s = pystore.store(store, engine=PARQUET_ENGINE)
    col = s.collection(collection)
    
    # Convert data to DataFrame
    df = pd.DataFrame(body.data)
    if "timestamp" in df.columns:
        # Try to convert timestamp to datetime for use as index
        # Only use as index if it's a valid datetime that can be stored as int64
        # This avoids issues with pystore's read function with string timestamps
        try:
            ts = pd.to_datetime(df["timestamp"])
            # Only use as index if all values are valid datetimes
            if ts.notna().all():
                df["timestamp"] = ts
                df = df.set_index("timestamp")
        except (ValueError, TypeError):
            # Keep timestamp as string column if conversion fails
            pass
    
    # Write to collection
    col.write(item, df, metadata=body.metadata or {})
    
    return {
        "store": store,
        "collection": collection,
        "item": item,
        "written": True,
        "rows": len(body.data)
    }


@app.post("/stores/{store}/collections/{collection}/items/{item}/append")
@rate_limit(default_rate_limit_config.write)
async def append_item(
    request: Request,
    store: str,
    collection: str,
    item: str,
    body: AppendItemRequest = Body(...)
):
    """Append data to an existing item."""
    import pandas as pd
    
    s = pystore.store(store, engine=PARQUET_ENGINE)
    col = s.collection(collection)
    
    # Convert data to DataFrame
    df = pd.DataFrame(body.data)
    if "timestamp" in df.columns:
        df = df.set_index("timestamp", drop=False)
    
    # Append to existing item
    col.append(item, df)
    
    return {
        "store": store,
        "collection": collection,
        "item": item,
        "appended": True,
        "rows": len(body.data)
    }


# ============================================
# Snapshot Endpoints
# ============================================

@app.get("/stores/{store}/collections/{collection}/snapshots")
@rate_limit(default_rate_limit_config.list)
async def list_snapshots(request: Request, store: str, collection: str):
    """List all snapshots in a collection."""
    s = pystore.store(store, engine=PARQUET_ENGINE)
    col = s.collection(collection)
    snapshots = col.list_snapshots()
    return {"store": store, "collection": collection, "snapshots": snapshots}


@app.post("/stores/{store}/collections/{collection}/snapshots/{snapshot}")
@rate_limit(default_rate_limit_config.write)
async def create_snapshot(request: Request, store: str, collection: str, snapshot: str):
    """Create a snapshot."""
    s = pystore.store(store, engine=PARQUET_ENGINE)
    col = s.collection(collection)
    col.create_snapshot(snapshot)
    return {
        "store": store,
        "collection": collection,
        "snapshot": snapshot,
        "created": True
    }


@app.delete("/stores/{store}/collections/{collection}/snapshots/{snapshot}")
@rate_limit(default_rate_limit_config.delete)
async def delete_snapshot(request: Request, store: str, collection: str, snapshot: str):
    """Delete a snapshot."""
    s = pystore.store(store, engine=PARQUET_ENGINE)
    col = s.collection(collection)
    col.delete_snapshot(snapshot)
    return {
        "store": store,
        "collection": collection,
        "snapshot": snapshot,
        "deleted": True
    }


# ============================================
# Metadata Endpoints
# ============================================

@app.get("/stores/{store}/collections/{collection}/items/{item}/metadata")
@rate_limit(default_rate_limit_config.read)
async def get_metadata(
    request: Request,
    store: str,
    collection: str,
    item: str,
    snapshot: Optional[str] = None
):
    """Get item metadata."""
    s = pystore.store(store, engine=PARQUET_ENGINE)
    col = s.collection(collection)
    
    item_obj = col.item(item, snapshot=snapshot)
    
    return {
        "store": store,
        "collection": collection,
        "item": item,
        "metadata": item_obj.metadata
    }


@app.put("/stores/{store}/collections/{collection}/items/{item}/metadata")
@rate_limit(default_rate_limit_config.write)
async def update_metadata(
    request: Request,
    store: str,
    collection: str,
    item: str,
    body: UpdateMetadataRequest = Body(...)
):
    """Update item metadata."""
    s = pystore.store(store, engine=PARQUET_ENGINE)
    col = s.collection(collection)
    
    # Get current item
    item_obj = col.item(item)
    
    # Update metadata by re-writing with merged metadata
    df = item_obj.to_pandas()
    current_metadata = item_obj.metadata.copy()
    current_metadata.update(body.metadata)
    col.write(item, df, metadata=current_metadata)
    
    return {
        "store": store,
        "collection": collection,
        "item": item,
        "metadata": current_metadata,
        "updated": True
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
