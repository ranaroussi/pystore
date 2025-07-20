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

# PyArrow is now the only supported engine

from .store import store
from .utils import (
    read_csv,
    set_path,
    get_path,
    set_client,
    get_client,
    set_partition_size,
    get_partition_size,
    list_stores,
    delete_store,
    delete_stores,
)
from .exceptions import (
    PyStoreError,
    DataIntegrityError,
    ItemNotFoundError,
    ItemExistsError,
    CollectionNotFoundError,
    CollectionExistsError,
    SnapshotNotFoundError,
    StorageError,
    SchemaError,
    ConfigurationError,
    ValidationError,
    TransactionError,
)

# Import new modules for Phase 4 features
from .async_operations import async_pystore, AsyncCollection, AsyncStore
from .transactions import transaction, batch_transaction, with_lock
from .validation import (
    create_validator,
    with_validation,
    create_timeseries_validator,
    create_financial_validator,
    ValidationRule,
    DataValidator,
    ColumnExistsRule,
    RangeRule,
    NoNullRule,
)
from .schema_evolution import SchemaEvolution, EvolutionStrategy

__version__ = "1.0.0"
__author__ = "Ran Aroussi"

__all__ = [
    "store",
    "read_csv",
    "get_path",
    "set_path",
    "set_client",
    "get_client",
    "set_partition_size",
    "get_partition_size",
    "list_stores",
    "delete_store",
    "delete_stores",
    # Exceptions
    "PyStoreError",
    "DataIntegrityError",
    "ItemNotFoundError",
    "ItemExistsError",
    "CollectionNotFoundError",
    "CollectionExistsError",
    "SnapshotNotFoundError",
    "StorageError",
    "SchemaError",
    "ConfigurationError",
    "ValidationError",
    "TransactionError",
    # Phase 4 Features
    "async_pystore",
    "AsyncCollection",
    "AsyncStore",
    "transaction",
    "batch_transaction",
    "with_lock",
    "create_validator",
    "with_validation",
    "create_timeseries_validator",
    "create_financial_validator",
    "ValidationRule",
    "DataValidator",
    "ColumnExistsRule",
    "RangeRule",
    "NoNullRule",
    "SchemaEvolution",
    "EvolutionStrategy",
]
