"""
Benchmarks for read operations
"""

from benchmark_base import BenchmarkBase, measure_performance
import pandas as pd
import numpy as np


class ReadBenchmark(BenchmarkBase):
    """Benchmark read operations"""
    
    def setup_data(self):
        """Setup test data for read benchmarks"""
        # Create datasets of various sizes
        sizes = [10_000, 100_000, 1_000_000]
        
        for size in sizes:
            data = self.generate_data(size, columns=10)
            self.collection.write(f'read_test_{size}', data, 
                                metadata={'rows': size, 'columns': 10})
        
        # Create partitioned dataset
        large_data = self.generate_data(1_000_000, columns=20)
        self.collection.write('partitioned_data', large_data, npartitions=50)
        
        # Create dataset with many columns
        wide_data = self.generate_data(100_000, columns=50)
        self.collection.write('wide_data', wide_data)
    
    def run_benchmark(self):
        """Run read benchmarks"""
        print("=" * 60)
        print("READ OPERATION BENCHMARKS")
        print("=" * 60)
        
        # Setup test data
        print("Setting up test data...")
        self.setup_data()
        
        # Test basic reads
        print("\nBasic Read Operations:")
        sizes = [10_000, 100_000, 1_000_000]
        
        for size in sizes:
            # Full read
            with measure_performance(f"Read {size:,} rows (to_pandas)", rows_processed=size):
                item = self.collection.item(f'read_test_{size}')
                df = item.to_pandas()
            
            # Just loading metadata
            with measure_performance(f"Load item metadata for {size:,} rows"):
                item = self.collection.item(f'read_test_{size}')
                _ = item.metadata
        
        # Test column selection
        print("\n" + "-" * 40)
        print("Column Selection Performance:")
        
        # Read all columns
        with measure_performance("Read 1M rows - all 20 columns", rows_processed=1_000_000):
            item = self.collection.item('partitioned_data')
            df = item.to_pandas()
        
        # Read subset of columns
        columns_to_read = ['col_0', 'col_1', 'col_2']
        with measure_performance(f"Read 1M rows - only {len(columns_to_read)} columns", 
                               rows_processed=1_000_000):
            item = self.collection.item('partitioned_data', columns=columns_to_read)
            df = item.to_pandas()
        
        # Read single column
        with measure_performance("Read 1M rows - single column", rows_processed=1_000_000):
            item = self.collection.item('partitioned_data', columns=['col_0'])
            df = item.to_pandas()
        
        # Test filter performance
        print("\n" + "-" * 40)
        print("Filter Performance:")
        
        # No filter
        with measure_performance("Read 100k rows - no filter", rows_processed=100_000):
            item = self.collection.item('read_test_100000')
            df = item.to_pandas()
        
        # With filter (assuming col_2 has categories A, B, C, D)
        with measure_performance("Read 100k rows - with filter (category == 'A')"):
            item = self.collection.item('read_test_100000', 
                                      filters=[('col_2', '==', 'A')])
            df = item.to_pandas()
            print(f"  Rows after filter: {len(df):,}")
        
        # Multiple filters
        with measure_performance("Read 100k rows - multiple filters"):
            item = self.collection.item('read_test_100000', 
                                      filters=[('col_2', '==', 'A'), 
                                             ('col_0', '>', 50)])
            df = item.to_pandas()
            print(f"  Rows after filters: {len(df):,}")
        
        # Test head/tail operations
        print("\n" + "-" * 40)
        print("Head/Tail Operations:")
        
        item = self.collection.item('read_test_1000000')
        
        with measure_performance("Head operation (first 5 rows)"):
            head_df = item.head()
        
        with measure_performance("Tail operation (last 5 rows)"):
            tail_df = item.tail()
        
        # Test metadata operations
        print("\n" + "-" * 40)
        print("Metadata Operations:")
        
        # List items with different sizes
        for num_items in [10, 100, 1000]:
            # Create items
            for i in range(num_items):
                small_data = pd.DataFrame({'value': [1, 2, 3]})
                self.collection.write(f'meta_test_{i}', small_data, 
                                    metadata={'index': i, 'type': 'test'})
            
            with measure_performance(f"List {num_items} items"):
                items = self.collection.list_items()
            
            with measure_performance(f"List {num_items} items with filter"):
                items = self.collection.list_items(type='test')


if __name__ == "__main__":
    benchmark = ReadBenchmark()
    benchmark.setup()
    try:
        benchmark.run_benchmark()
    finally:
        benchmark.teardown()