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
Custom exceptions for PyStore
"""


class PyStoreError(Exception):
    """Base exception for all PyStore errors"""
    pass


class DataIntegrityError(PyStoreError):
    """Raised when data integrity issues are detected"""
    pass


class ItemNotFoundError(PyStoreError):
    """Raised when an item is not found"""
    pass


class ItemExistsError(PyStoreError):
    """Raised when trying to create an item that already exists"""
    pass


class CollectionNotFoundError(PyStoreError):
    """Raised when a collection is not found"""
    pass


class CollectionExistsError(PyStoreError):
    """Raised when trying to create a collection that already exists"""
    pass


class SnapshotNotFoundError(PyStoreError):
    """Raised when a snapshot is not found"""
    pass


class StorageError(PyStoreError):
    """Raised when storage operations fail"""
    pass


class SchemaError(PyStoreError):
    """Raised when schema incompatibilities are found"""
    pass


class ConfigurationError(PyStoreError):
    """Raised when configuration is invalid"""
    pass


class ValidationError(PyStoreError):
    """Raised when data validation fails"""
    pass