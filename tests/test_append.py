"""
Tests for PyStore append operations - Critical functionality
"""

import concurrent.futures
import threading

import numpy as np
import pandas as pd
import pytest

import pystore


class TestAppend:
    """Test append operations with focus on data integrity"""

    def test_append_basic(self, test_collection, sample_data):
        """Test basic append operation"""
        item_name = 'test_item'

        # Write initial data (first 50 rows)
        initial_data = sample_data.iloc[:50]
        test_collection.write(item_name, initial_data)

        # Append new data (last 50 rows)
        new_data = sample_data.iloc[50:]
        test_collection.append(item_name, new_data)

        # Read and verify
        item = test_collection.item(item_name)
        df_read = item.to_pandas()

        # Should have all 100 rows
        assert len(df_read) == 100
        pd.testing.assert_frame_equal(df_read, sample_data)

    def test_append_nonexistent_item(self, test_collection, sample_data):
        """Test appending to an item that doesn't exist"""
        with pytest.raises(pystore.ItemNotFoundError):
            test_collection.append('nonexistent', sample_data)

    def test_append_empty_dataframe(self, test_collection, sample_data):
        """Test appending an empty DataFrame"""
        item_name = 'test_item'
        test_collection.write(item_name, sample_data)

        # Append empty DataFrame
        empty_df = pd.DataFrame(columns=sample_data.columns)
        test_collection.append(item_name, empty_df)

        # Data should remain unchanged
        item = test_collection.item(item_name)
        df_read = item.to_pandas()
        pd.testing.assert_frame_equal(df_read, sample_data)

    def test_append_duplicate_handling_keep_last(self, test_collection):
        """Test append with duplicate indices - keep_last strategy"""
        dates = pd.date_range('2024-01-01', periods=10, freq='D')

        # Initial data
        initial_data = pd.DataFrame({
            'value': range(10)
        }, index=dates)
        test_collection.write('test_item', initial_data)

        # Overlapping data with different values
        overlap_dates = pd.date_range('2024-01-06', periods=10, freq='D')
        new_data = pd.DataFrame({
            'value': range(100, 110)
        }, index=overlap_dates)

        # Append with keep_last (default)
        test_collection.append('test_item', new_data, duplicate_handling='keep_last')

        # Read and verify
        item = test_collection.item('test_item')
        df_read = item.to_pandas()

        # Should have 15 unique dates (10 original + 10 new - 5 overlapping)
        assert len(df_read) == 15
        # Overlapping dates should have new values
        assert df_read.loc['2024-01-06']['value'] == 100
        assert df_read.loc['2024-01-10']['value'] == 104

    def test_append_duplicate_handling_keep_first(self, test_collection):
        """Test append with duplicate indices - keep_first strategy"""
        dates = pd.date_range('2024-01-01', periods=10, freq='D')

        # Initial data
        initial_data = pd.DataFrame({
            'value': range(10)
        }, index=dates)
        test_collection.write('test_item', initial_data)

        # Overlapping data
        overlap_dates = pd.date_range('2024-01-06', periods=10, freq='D')
        new_data = pd.DataFrame({
            'value': range(100, 110)
        }, index=overlap_dates)

        # Append with keep_first
        test_collection.append('test_item', new_data, duplicate_handling='keep_first')

        # Read and verify
        item = test_collection.item('test_item')
        df_read = item.to_pandas()

        # Should have 15 unique dates
        assert len(df_read) == 15
        # Overlapping dates should have original values
        assert df_read.loc['2024-01-06']['value'] == 5
        assert df_read.loc['2024-01-10']['value'] == 9

    def test_append_duplicate_handling_error(self, test_collection):
        """Test append with duplicate indices - error strategy"""
        dates = pd.date_range('2024-01-01', periods=10, freq='D')

        # Initial data
        initial_data = pd.DataFrame({
            'value': range(10)
        }, index=dates)
        test_collection.write('test_item', initial_data)

        # Overlapping data
        overlap_data = pd.DataFrame({
            'value': [999]
        }, index=[dates[5]])

        # Should raise error on duplicates
        with pytest.raises(pystore.DataIntegrityError, match="duplicate indices"):
            test_collection.append('test_item', overlap_data, duplicate_handling='error')

    def test_append_schema_validation(self, test_collection):
        """Test append with schema validation"""
        # Initial data with specific columns
        initial_data = pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': ['a', 'b', 'c']
        })
        test_collection.write('test_item', initial_data)

        # Try to append with different columns
        bad_data = pd.DataFrame({
            'col1': [4, 5],
            'col3': ['d', 'e']  # col3 instead of col2
        })

        with pytest.raises(pystore.ValidationError, match="Schema mismatch"):
            test_collection.append('test_item', bad_data, validate_schema=True)

    def test_append_no_schema_validation(self, test_collection):
        """Test append without schema validation"""
        # Initial data
        initial_data = pd.DataFrame({
            'col1': [1, 2, 3]
        })
        test_collection.write('test_item', initial_data)

        # Append with different schema (should work with validate_schema=False)
        new_data = pd.DataFrame({
            'col1': [4, 5],
            'col2': ['a', 'b']
        })

        # This should work but may cause issues later
        test_collection.append('test_item', new_data, validate_schema=False)

    def test_append_preserves_metadata(self, test_collection, sample_data):
        """Test that append preserves item metadata"""
        item_name = 'test_item'
        metadata = {'source': 'test', 'version': '1.0'}

        # Write with metadata
        test_collection.write(item_name, sample_data[:50], metadata=metadata)

        # Append more data
        test_collection.append(item_name, sample_data[50:])

        # Verify metadata is preserved
        item = test_collection.item(item_name)
        assert item.metadata['source'] == 'test'
        assert item.metadata['version'] == '1.0'

    def test_append_maintains_sort_order(self, test_collection):
        """Test that append maintains sorted index"""
        # Create data with non-sequential dates
        dates1 = pd.date_range('2024-01-01', periods=5, freq='D')
        dates2 = pd.date_range('2024-01-20', periods=5, freq='D')
        dates3 = pd.date_range('2024-01-10', periods=5, freq='D')

        data1 = pd.DataFrame({'value': range(5)}, index=dates1)
        data2 = pd.DataFrame({'value': range(5, 10)}, index=dates2)
        data3 = pd.DataFrame({'value': range(10, 15)}, index=dates3)

        # Write and append in non-chronological order
        test_collection.write('test_item', data1)
        test_collection.append('test_item', data2)
        test_collection.append('test_item', data3)

        # Read and verify sort order
        item = test_collection.item('test_item')
        df_read = item.to_pandas()

        # Index should be sorted
        assert df_read.index.is_monotonic_increasing
        assert len(df_read) == 15

    def test_append_nanosecond_precision(self, test_collection, sample_data_nanosecond):
        """Test appending data with nanosecond precision"""
        # Write initial data
        test_collection.write('test_item', sample_data_nanosecond[:5])

        # Append more nanosecond data
        test_collection.append('test_item', sample_data_nanosecond[5:])

        # Verify
        item = test_collection.item('test_item')
        df_read = item.to_pandas()
        assert len(df_read) == len(sample_data_nanosecond)

    def test_append_threaded_mode_preserves_all_rows(self, test_collection, sample_data):
        """Regression: threaded append should preserve the complete dataset."""
        test_collection.write('test_item', sample_data.iloc[:50])

        with pytest.warns(DeprecationWarning, match="write_threaded is deprecated"):
            test_collection.append(
                'test_item',
                sample_data.iloc[50:],
                threaded=True,
            )

        df_read = test_collection.item('test_item').to_pandas()
        pd.testing.assert_frame_equal(df_read, sample_data)

    def test_concurrent_appends_to_same_item_are_serialized(self, test_collection):
        """Concurrent appends to one item should not lose data or collide."""
        initial_data = pd.DataFrame(
            {'value': [1, 2]},
            index=pd.date_range('2024-01-01', periods=2, freq='D'),
        )
        append_a = pd.DataFrame(
            {'value': [3, 4]},
            index=pd.date_range('2024-01-03', periods=2, freq='D'),
        )
        append_b = pd.DataFrame(
            {'value': [5, 6]},
            index=pd.date_range('2024-01-05', periods=2, freq='D'),
        )
        test_collection.write('test_item', initial_data)

        start_together = threading.Barrier(2)

        def append_frame(frame):
            start_together.wait(timeout=5)
            test_collection.append('test_item', frame)

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(append_frame, append_a),
                executor.submit(append_frame, append_b),
            ]
            for future in futures:
                future.result(timeout=30)

        expected = pd.concat([initial_data, append_a, append_b]).sort_index()
        df_read = test_collection.item('test_item').to_pandas()
        pd.testing.assert_frame_equal(df_read, expected, check_freq=False)

    def test_append_identical_data(self, test_collection):
        """Test appending identical data (regression test for issue #69)"""
        # Create data where only index differs but values are same
        dates1 = pd.date_range('2024-01-01', periods=3, freq='D')
        dates2 = pd.date_range('2024-01-04', periods=3, freq='D')

        # Same values, different indices
        data1 = pd.DataFrame({'value': [np.nan, np.nan, np.nan]}, index=dates1)
        data2 = pd.DataFrame({'value': [np.nan, np.nan, np.nan]}, index=dates2)

        test_collection.write('test_item', data1)
        test_collection.append('test_item', data2)

        # Should have all 6 rows, not just 3
        item = test_collection.item('test_item')
        df_read = item.to_pandas()
        assert len(df_read) == 6


class TestAppendStreamComplexTypes:
    """Regression tests for append_stream with MultiIndex and timezone-aware data.

    These exercise the _validate_schema_compatibility path inside
    append_stream, which uses ``current_item.to_pandas()`` to compare
    schemas.  MultiIndex and timezone-aware items have different
    on-disk representations vs. their pandas-visible schemas.
    """

    def test_append_stream_multiindex(self, test_collection):
        """append_stream must validate and append MultiIndex DataFrames correctly."""
        index1 = pd.MultiIndex.from_tuples(
            [("A", "2024-01-01"), ("A", "2024-01-02"), ("B", "2024-01-01")],
            names=["category", "date"],
        )
        df_initial = pd.DataFrame({"value": [1.0, 2.0, 3.0]}, index=index1)
        test_collection.write("stream_mi", df_initial)

        index2 = pd.MultiIndex.from_tuples(
            [("A", "2024-01-03"), ("B", "2024-01-02")],
            names=["category", "date"],
        )
        chunk1 = pd.DataFrame({"value": [4.0, 5.0]}, index=index2)
        chunk2 = pd.DataFrame(
            {"value": [6.0, 7.0]},
            index=pd.MultiIndex.from_tuples(
                [("C", "2024-01-01"), ("C", "2024-01-02")],
                names=["category", "date"],
            ),
        )

        test_collection.append_stream("stream_mi", iter([chunk1, chunk2]))

        result = test_collection.item("stream_mi").to_pandas()
        assert isinstance(result.index, pd.MultiIndex)
        assert len(result) == 7

    def test_append_stream_timezone_aware(self, test_collection):
        """append_stream must validate and append timezone-aware DataFrames correctly."""
        tz = "US/Eastern"
        df_initial = pd.DataFrame(
            {"value": [1.0, 2.0, 3.0]},
            index=pd.date_range("2024-01-01", periods=3, freq="h", tz=tz),
        )
        test_collection.write("stream_tz", df_initial)

        chunk = pd.DataFrame(
            {"value": [4.0, 5.0]},
            index=pd.date_range("2024-01-01 05:00", periods=2, freq="h", tz=tz),
        )

        test_collection.append_stream("stream_tz", iter([chunk]))

        result = test_collection.item("stream_tz").to_pandas()
        assert result.index.tz is not None
        assert len(result) == 5
