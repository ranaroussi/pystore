"""
Benchmarks for write operations
"""

from benchmark_base import BenchmarkBase, measure_performance
import pandas as pd
import numpy as np


class WriteBenchmark(BenchmarkBase):
    """Benchmark write operations"""
    
    def run_benchmark(self):
        """Run write benchmarks"""
        print("=" * 60)
        print("WRITE OPERATION BENCHMARKS")
        print("=" * 60)
        
        # Test different data sizes
        sizes = [1_000, 10_000, 100_000, 1_000_000]
        
        for size in sizes:
            print(f"\nTesting with {size:,} rows:")
            
            # Generate data
            data = self.generate_data(size)
            
            # Benchmark: Simple write
            with measure_performance(f"Write {size:,} rows", rows_processed=size):
                self.collection.write(f'item_{size}', data, overwrite=True)
            
            # Benchmark: Write with metadata
            metadata = {'source': 'benchmark', 'rows': size, 'columns': len(data.columns)}
            with measure_performance(f"Write {size:,} rows with metadata", rows_processed=size):
                self.collection.write(f'item_meta_{size}', data, metadata=metadata, overwrite=True)
            
            # Benchmark: Write with different partition sizes
            if size >= 100_000:
                for npartitions in [1, 10, 50]:
                    with measure_performance(f"Write {size:,} rows with {npartitions} partitions", rows_processed=size):
                        self.collection.write(f'item_part_{size}_{npartitions}', data, 
                                            npartitions=npartitions, overwrite=True)
        
        # Test writing with different data types
        print("\n" + "-" * 40)
        print("Testing different data types:")
        
        # All numeric
        numeric_data = pd.DataFrame({
            f'col_{i}': np.random.randn(10_000) 
            for i in range(20)
        })
        with measure_performance("Write numeric data (10k rows, 20 cols)", rows_processed=10_000):
            self.collection.write('numeric_data', numeric_data, overwrite=True)
        
        # Mixed types
        mixed_data = pd.DataFrame({
            'int_col': np.random.randint(0, 100, 10_000),
            'float_col': np.random.randn(10_000),
            'str_col': np.random.choice(['A', 'B', 'C'], 10_000),
            'bool_col': np.random.choice([True, False], 10_000),
            'datetime_col': pd.date_range('2020-01-01', periods=10_000, freq='1H')
        })
        with measure_performance("Write mixed type data (10k rows)", rows_processed=10_000):
            self.collection.write('mixed_data', mixed_data, overwrite=True)


if __name__ == "__main__":
    benchmark = WriteBenchmark()
    benchmark.setup()
    try:
        benchmark.run_benchmark()
    finally:
        benchmark.teardown()