"""
PyStore test configuration and fixtures
"""

import pytest
import tempfile
import shutil
from pathlib import Path
import pandas as pd
import numpy as np
import pystore


@pytest.fixture
def temp_store_path():
    """Create a temporary directory for test store"""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    # Cleanup
    shutil.rmtree(temp_dir)


@pytest.fixture
def test_store(temp_store_path):
    """Create a test store"""
    pystore.set_path(temp_store_path)
    store = pystore.store('test_store')
    yield store
    # No cleanup needed as temp_store_path fixture handles it


@pytest.fixture
def test_collection(test_store):
    """Create a test collection"""
    collection = test_store.collection('test_collection')
    yield collection


@pytest.fixture
def sample_data():
    """Create sample pandas DataFrame with datetime index"""
    dates = pd.date_range('2024-01-01', periods=100, freq='D')
    data = pd.DataFrame({
        'value1': np.random.randn(100),
        'value2': np.random.randn(100) * 100,
        'category': np.random.choice(['A', 'B', 'C'], 100)
    }, index=dates)
    return data


@pytest.fixture
def sample_data_with_duplicates():
    """Create sample data with duplicate indices"""
    dates = pd.date_range('2024-01-01', periods=50, freq='D')
    # Repeat some dates
    dates = dates.append(dates[40:])
    data = pd.DataFrame({
        'value1': np.random.randn(60),
        'value2': np.random.randn(60) * 100,
        'category': np.random.choice(['A', 'B', 'C'], 60)
    }, index=dates)
    return data


@pytest.fixture
def sample_data_nanosecond():
    """Create sample data with nanosecond precision timestamps"""
    dates = pd.date_range('2024-01-01', periods=10, freq='1s')
    # Add nanosecond precision
    dates = dates + pd.to_timedelta(np.random.randint(0, 1000, size=10), unit='ns')
    data = pd.DataFrame({
        'value': np.random.randn(10)
    }, index=dates)
    return data