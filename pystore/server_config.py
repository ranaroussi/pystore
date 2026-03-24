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

"""Server configuration module for PyStore API server."""

import os
from typing import Optional

# Server settings
HOST = os.environ.get("PYSTORE_HOST", "0.0.0.0")
PORT = int(os.environ.get("PYSTORE_PORT", "8000"))
RELOAD = os.environ.get("PYSTORE_RELOAD", "false").lower() == "true"

# CORS settings
CORS_ORIGINS = os.environ.get("PYSTORE_CORS_ORIGINS", "*").split(",")

# Storage path - uses PYSTORE_PATH env var or defaults to ~/pystore
STORAGE_PATH = os.environ.get("PYSTORE_PATH", None)

# Rate limiting settings
RATE_LIMIT_ENABLED = os.environ.get("PYSTORE_RATE_LIMIT_ENABLED", "true").lower() == "true"
RATE_LIMIT_READ = os.environ.get("PYSTORE_RATE_LIMIT_READ", "100/minute")
RATE_LIMIT_WRITE = os.environ.get("PYSTORE_RATE_LIMIT_WRITE", "20/minute")
RATE_LIMIT_DELETE = os.environ.get("PYSTORE_RATE_LIMIT_DELETE", "10/minute")
RATE_LIMIT_LIST = os.environ.get("PYSTORE_RATE_LIMIT_LIST", "60/minute")


class RateLimitConfig:
    """Rate limit configuration for different endpoint types."""
    
    def __init__(
        self,
        read: str = RATE_LIMIT_READ,
        write: str = RATE_LIMIT_WRITE,
        delete: str = RATE_LIMIT_DELETE,
        list_items: str = RATE_LIMIT_LIST
    ):
        self.read = read
        self.write = write
        self.delete = delete
        self.list = list_items


# Default rate limit config instance
default_rate_limit_config = RateLimitConfig()
