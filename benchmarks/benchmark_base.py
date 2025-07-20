"""
Base classes and utilities for PyStore benchmarks
"""

import time
import psutil
import pandas as pd
import numpy as np
from contextlib import contextmanager
from pathlib import Path
import tempfile
import shutil
import pystore


class BenchmarkResult:
    """Store benchmark results"""
    def __init__(self, name, duration, memory_used, rows_processed=None):
        self.name = name
        self.duration = duration
        self.memory_used = memory_used
        self.rows_processed = rows_processed
        self.throughput = rows_processed / duration if rows_processed else None
    
    def __str__(self):
        result = f"{self.name}:\n"
        result += f"  Duration: {self.duration:.3f}s\n"
        result += f"  Memory: {self.memory_used / 1024 / 1024:.1f} MB\n"
        if self.throughput:
            result += f"  Throughput: {self.throughput:,.0f} rows/sec\n"
        return result


@contextmanager
def measure_performance(name, rows_processed=None):
    """Context manager to measure performance"""
    # Get initial memory
    process = psutil.Process()
    initial_memory = process.memory_info().rss
    
    # Start timer
    start_time = time.time()
    
    yield
    
    # Calculate metrics
    duration = time.time() - start_time
    final_memory = process.memory_info().rss
    memory_used = final_memory - initial_memory
    
    result = BenchmarkResult(name, duration, memory_used, rows_processed)
    print(result)
    return result


class BenchmarkBase:
    """Base class for benchmarks"""
    
    def __init__(self):
        self.temp_dir = None
        self.store = None
        self.collection = None
    
    def setup(self):
        """Setup test environment"""
        self.temp_dir = Path(tempfile.mkdtemp())
        pystore.set_path(self.temp_dir)
        self.store = pystore.store('benchmark_store')
        self.collection = self.store.collection('benchmark_collection')
    
    def teardown(self):
        """Cleanup test environment"""
        if self.temp_dir and self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
    
    def generate_data(self, rows, columns=5, freq='1min'):
        """Generate test data"""
        dates = pd.date_range('2020-01-01', periods=rows, freq=freq)
        data = {}
        
        for i in range(columns):
            if i == 0:
                # First column: integers
                data[f'col_{i}'] = np.random.randint(0, 100, size=rows)
            elif i == 1:
                # Second column: floats
                data[f'col_{i}'] = np.random.randn(rows) * 100
            elif i == 2:
                # Third column: categories
                data[f'col_{i}'] = np.random.choice(['A', 'B', 'C', 'D'], size=rows)
            else:
                # Rest: random floats
                data[f'col_{i}'] = np.random.randn(rows)
        
        return pd.DataFrame(data, index=dates)
    
    def run_benchmark(self):
        """Override this method in subclasses"""
        raise NotImplementedError