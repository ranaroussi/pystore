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

import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Union, cast

import numpy as np
import pandas as pd
from dask import dataframe as dd
from dask.distributed import Client

from . import config
from .exceptions import StorageError
from .logger import get_logger

logger = get_logger(__name__)


def read_csv(
    urlpath: Union[str, Path, list[str]], *args: Any, **kwargs: Any
) -> dd.DataFrame:
    def rename_dask_index(df: dd.DataFrame, name: str) -> dd.DataFrame:
        df.index.name = name
        return df

    index_col = index_name = None

    if "index" in kwargs:
        del kwargs["index"]
    if "index_col" in kwargs:
        index_col = kwargs["index_col"]
        if isinstance(index_col, list):
            index_col = index_col[0]
        del kwargs["index_col"]
    if "index_name" in kwargs:
        index_name = kwargs["index_name"]
        del kwargs["index_name"]

    df = cast(dd.DataFrame, dd.read_csv(urlpath, *args, **kwargs))

    if index_col is not None:
        df = df.set_index(index_col)

    if index_name is not None:
        df = df.map_partitions(rename_dask_index, index_name)

    return df


def datetime_to_int64(df: Union[pd.DataFrame, dd.DataFrame]) -> Union[pd.DataFrame, dd.DataFrame]:
    """Convert datetime index to epoch int (nanoseconds since epoch).

    This allows for cross language/platform portability.  The conversion
    is unconditional for DatetimeIndex — callers opt in by setting
    ``epochdate=True`` or by having a datetime-typed index.
    """
    if isinstance(df.index, pd.DatetimeIndex):
        # Pandas DataFrame with DatetimeIndex — always convert to int64.
        df.index = df.index.astype(np.int64)
    elif isinstance(df.index, dd.Index):
        # Dask DataFrame — convert when the underlying dtype is datetime.
        if pd.api.types.is_datetime64_any_dtype(df.index.dtype):
            df.index = df.index.astype(np.int64)

    return df


def subdirs(d: Union[str, Path]) -> list[str]:
    """use this to construct paths for future storage support"""
    return [
        o.parts[-1]
        for o in Path(d).iterdir()
        if o.is_dir() and o.parts[-1] != "_snapshots"
    ]


def path_exists(path: Union[str, Path]) -> bool:
    """use this to construct paths for future storage support"""
    return Path(path).exists()


def read_metadata(path: Union[str, Path]) -> dict[str, Any]:
    """use this to construct paths for future storage support"""
    dest = make_path(path, "pystore_metadata.json")
    if path_exists(dest):
        with dest.open() as f:
            return cast(dict[str, Any], json.load(f))
    else:
        return {}


def write_metadata(
    path: Union[str, Path], metadata: Optional[dict[str, Any]] = None
) -> None:
    """use this to construct paths for future storage support"""
    if metadata is None:
        metadata = {}
    now = datetime.now(timezone.utc)  # Use UTC for consistency
    metadata["_updated"] = now.strftime(
        "%Y-%m-%d %H:%M:%S.%f"
    )  # Correctly formats minutes using %M
    meta_file = make_path(path, "pystore_metadata.json")
    # Ensure parent directory exists
    meta_file.parent.mkdir(parents=True, exist_ok=True)
    with meta_file.open("w") as f:
        json.dump(metadata, f, ensure_ascii=False)


def validate_identifier(name: Union[str, os.PathLike], kind: str = "Identifier") -> str:
    """Validate a user-facing name as a single safe path component."""
    if name is None:
        raise ValueError(f"{kind} name must not be empty")

    value = os.fspath(name) if isinstance(name, os.PathLike) else str(name)
    if not value.strip():
        raise ValueError(f"{kind} name must not be empty")
    if "\x00" in value:
        raise ValueError(f"{kind} name contains null byte, which is not permitted")
    if value in {".", ".."}:
        raise ValueError(f"{kind} name '{value}' is invalid")
    if "/" in value or "\\" in value:
        raise ValueError(f"{kind} name '{value}' must be a single path component")
    if Path(value).is_absolute():
        raise ValueError(f"{kind} name '{value}' must be relative")

    return value


def sanitize_snapshot_name(snapshot: Union[str, os.PathLike]) -> str:
    """Sanitize a snapshot name and ensure it remains a valid component."""
    snapshot_str = (
        os.fspath(snapshot) if isinstance(snapshot, os.PathLike) else str(snapshot)
    )
    snapshot_name = "".join(
        char for char in snapshot_str if char.isalnum() or char in [".", "_"]
    )
    return validate_identifier(snapshot_name, "Snapshot")


def make_path(*args: Union[str, Path]) -> Path:
    """use this to construct paths for future storage support"""
    if not args:
        return Path()

    path = Path(args[0])
    for component in args[1:]:
        component_path = Path(component)
        if component_path.is_absolute():
            raise ValueError("Path components must be relative")
        path = path / component_path

    return path


def get_path(*args: str) -> Path:
    """use this to construct paths for future storage support"""
    components = [validate_identifier(arg, "Path component") for arg in args]
    return make_path(config.DEFAULT_PATH, *components)


def set_path(path: Optional[Union[str, Path]] = None) -> Path:
    """Set the base path for PyStore data

    Parameters
    ----------
    path : str or Path, optional
        Base path for data storage. Defaults to ~/pystore
    """
    if path is None:
        path = Path.home() / "pystore"
    else:
        path_str = str(path)
        if "://" in path_str and "file://" not in path_str:
            raise ValueError("PyStore currently only works with local file system")

        # Handle both string and Path objects
        path = Path(path).expanduser()
        if not path.is_absolute():
            path = path.absolute()

    # Create directory if it doesn't exist
    try:
        path.mkdir(parents=True, exist_ok=True)
    except PermissionError as err:
        raise PermissionError(f"Cannot create directory at {path}") from err

    # Store as string for compatibility
    config.DEFAULT_PATH = str(path)
    return path


def list_stores() -> list[str]:
    if not path_exists(get_path()):
        os.makedirs(get_path())
    return subdirs(get_path())


def delete_store(store: Union[str, os.PathLike]) -> bool:
    store_name = validate_identifier(store, "Store")
    store_path = get_path(store_name)
    if not path_exists(store_path):
        raise ValueError(f"Store '{store_name}' does not exist")
    try:
        shutil.rmtree(store_path)
        return True
    except Exception as e:
        raise StorageError(f"Failed to delete store '{store_name}': {str(e)}") from e


def delete_stores() -> bool:
    store_path = get_path()
    if not path_exists(store_path):
        raise ValueError(f"Store path '{store_path}' does not exist")
    shutil.rmtree(store_path)
    return True


def set_client(scheduler: Optional[Any] = None) -> Optional[Client]:
    if scheduler != config._SCHEDULER and config._CLIENT is not None:
        try:
            config._CLIENT.shutdown()
        except Exception as e:
            # Distinguish between a genuinely failed shutdown and an
            # already-closed client.  Either way, clear the reference to
            # avoid holding a stale client object.
            if "already closed" in str(e).lower() or "shutdown" in str(e).lower():
                logger.debug(f"Dask client was already shut down: {e}")
            else:
                logger.warning(f"Failed to shut down existing Dask client: {e}")
        config._CLIENT = None

    config._SCHEDULER = scheduler
    if scheduler is not None:
        config._CLIENT = Client(scheduler)

    return config._CLIENT


def get_client() -> Optional[Client]:
    return config._CLIENT


def set_partition_size(size: Optional[Union[int, float]] = None) -> Union[int, float]:
    if size is None:
        size = cast(Union[int, float], config.DEFAULT_PARTITION_SIZE * 1)
    config.PARTITION_SIZE = size
    return cast(Union[int, float], config.PARTITION_SIZE)


def get_partition_size() -> Union[int, float]:
    return cast(Union[int, float], config.PARTITION_SIZE)
