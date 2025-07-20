"""
Benchmarks for append operations
"""

from benchmark_base import BenchmarkBase, measure_performance
import pandas as pd
import numpy as np


class AppendBenchmark(BenchmarkBase):
    """Benchmark append operations"""
    
    def run_benchmark(self):
        """Run append benchmarks"""
        print("=" * 60)
        print("APPEND OPERATION BENCHMARKS")
        print("=" * 60)
        
        # Test incremental appends
        print("\nIncremental Append Test:")
        
        # Initial data
        initial_size = 100_000
        initial_data = self.generate_data(initial_size)
        self.collection.write('append_test', initial_data)
        
        # Append different chunk sizes
        chunk_sizes = [100, 1_000, 10_000, 50_000]
        
        for chunk_size in chunk_sizes:
            append_data = self.generate_data(chunk_size)
            
            # Ensure non-overlapping index
            last_date = initial_data.index[-1]
            new_dates = pd.date_range(start=last_date + pd.Timedelta(minutes=1), 
                                     periods=chunk_size, freq='1min')
            append_data.index = new_dates
            
            with measure_performance(f"Append {chunk_size:,} rows", rows_processed=chunk_size):
                self.collection.append('append_test', append_data)
        
        # Test append with duplicates
        print("\n" + "-" * 40)
        print("Append with Duplicate Handling:")
        
        # Create base data
        base_data = self.generate_data(50_000)
        self.collection.write('dup_test', base_data)
        
        # Create overlapping data (50% overlap)
        overlap_start = base_data.index[25_000]
        overlap_dates = pd.date_range(start=overlap_start, periods=50_000, freq='1min')
        overlap_data = self.generate_data(50_000)
        overlap_data.index = overlap_dates
        
        # Test different duplicate strategies
        strategies = ['keep_last', 'keep_first', 'keep_all']
        
        for strategy in strategies:
            self.collection.write(f'dup_test_{strategy}', base_data, overwrite=True)
            
            with measure_performance(f"Append 50k rows with {strategy} (50% overlap)", 
                                   rows_processed=50_000):
                self.collection.append(f'dup_test_{strategy}', overlap_data, 
                                     duplicate_handling=strategy)
        
        # Test multiple small appends vs one large append
        print("\n" + "-" * 40)
        print("Multiple Small vs Single Large Append:")
        
        # Setup
        base_data = self.generate_data(10_000)
        total_append_rows = 100_000
        
        # Test 1: Many small appends
        self.collection.write('many_small', base_data, overwrite=True)
        small_chunk_size = 1_000
        num_chunks = total_append_rows // small_chunk_size
        
        print(f"\nAppending {num_chunks} chunks of {small_chunk_size:,} rows each:")
        total_time = 0
        
        for i in range(num_chunks):
            chunk_data = self.generate_data(small_chunk_size)
            # Ensure non-overlapping
            last_date = pd.Timestamp('2020-01-01') + pd.Timedelta(minutes=(10_000 + i * small_chunk_size))
            chunk_dates = pd.date_range(start=last_date, periods=small_chunk_size, freq='1min')
            chunk_data.index = chunk_dates
            
            with measure_performance(f"Small append {i+1}/{num_chunks}", rows_processed=small_chunk_size) as perf:
                self.collection.append('many_small', chunk_data)
                total_time += perf.duration
        
        print(f"Total time for {num_chunks} small appends: {total_time:.3f}s")
        
        # Test 2: One large append
        self.collection.write('one_large', base_data, overwrite=True)
        large_data = self.generate_data(total_append_rows)
        last_date = base_data.index[-1]
        large_dates = pd.date_range(start=last_date + pd.Timedelta(minutes=1), 
                                   periods=total_append_rows, freq='1min')
        large_data.index = large_dates
        
        with measure_performance(f"Single append of {total_append_rows:,} rows", 
                               rows_processed=total_append_rows):
            self.collection.append('one_large', large_data)


if __name__ == "__main__":
    benchmark = AppendBenchmark()
    benchmark.setup()
    try:
        benchmark.run_benchmark()
    finally:
        benchmark.teardown()