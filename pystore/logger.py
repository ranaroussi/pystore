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
Logging configuration for PyStore
"""

import logging
import os
from typing import Optional


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance for the given name"""
    _ensure_logging_configured()
    return logging.getLogger(f"pystore.{name}")


def setup_logging(level: Optional[str] = None) -> None:
    """
    Setup logging for PyStore

    Parameters
    ----------
    level : str, optional
        Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        If not provided, uses PYSTORE_LOG_LEVEL environment variable
        or defaults to WARNING
    """
    if level is None:
        level = os.environ.get('PYSTORE_LOG_LEVEL', 'WARNING')

    # Configure root logger for pystore
    logger = logging.getLogger('pystore')
    logger.setLevel(getattr(logging, level.upper()))

    # Remove existing handlers to avoid duplicates
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # Create console handler with formatting
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Prevent propagation to root logger
    logger.propagate = False


# Defer full handler setup until first use so that importing pystore
# does not mutate the global logging configuration.  Individual
# ``get_logger()`` calls will lazily configure the pystore logger when
# a message is actually emitted.
_lazy_configured = False


def _ensure_logging_configured() -> None:
    """Lazily configure the pystore logger on first actual use."""
    global _lazy_configured
    if _lazy_configured:
        return
    _lazy_configured = True
    setup_logging()
