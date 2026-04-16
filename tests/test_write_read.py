"""
Tests for PyStore write and read operations
"""

import pytest
import pandas as pd
import numpy as np
import dask.dataframe as dd
import pystore


class TestWriteRead:
    """Test data write and read operations"""
    
    def test_write_read_basic(self, test_collection, sample_data):
        """Test basic write and read functionality"""
        item_name = 'test_item'
        
        # Write data
        test_collection.write(item_name, sample_data, metadata={'source': 'test'})
        
        # Read data back
        item = test_collection.item(item_name)
        df_read = item.to_pandas()
        
        # Verify data integrity
        pd.testing.assert_frame_equal(df_read, sample_data)
        assert item.metadata['source'] == 'test'
    
    def test_write_overwrite_error(self, test_collection, sample_data):
        """Test error when writing to existing item without overwrite"""
        item_name = 'test_item'
        test_collection.write(item_name, sample_data)
        
        with pytest.raises(pystore.ItemExistsError):
            test_collection.write(item_name, sample_data, overwrite=False)
    
    def test_write_overwrite(self, test_collection, sample_data):
        """Test overwriting an existing item"""
        item_name = 'test_item'
        
        # Write initial data
        test_collection.write(item_name, sample_data)
        
        # Overwrite with new data
        new_data = sample_data * 2
        test_collection.write(item_name, new_data, overwrite=True)
        
        # Verify new data
        item = test_collection.item(item_name)
        df_read = item.to_pandas()
        pd.testing.assert_frame_equal(df_read, new_data)
    
    def test_write_dask_dataframe(self, test_collection, sample_data):
        """Test writing a Dask DataFrame"""
        item_name = 'test_item'
        
        # Convert to Dask DataFrame
        dask_df = dd.from_pandas(sample_data, npartitions=2)
        
        # Write Dask DataFrame
        test_collection.write(item_name, dask_df)
        
        # Read and verify.
        # Note: Dask internally converts object-dtype string columns to
        # string[pyarrow].  Values must round-trip correctly; dtype differences
        # for string-typed columns are expected and accepted here.
        item = test_collection.item(item_name)
        df_read = item.to_pandas()
        pd.testing.assert_frame_equal(df_read, sample_data, check_dtype=False)
    
    def test_write_with_epochdate(self, test_collection):
        """Test writing with epochdate conversion"""
        dates = pd.date_range('2024-01-01', periods=10, freq='D')
        data = pd.DataFrame({'value': range(10)}, index=dates)
        
        test_collection.write('test_item', data, epochdate=True)
        
        # Read back
        item = test_collection.item('test_item')
        df_read = item.to_pandas()
        
        # Index should be datetime after reading
        assert isinstance(df_read.index, pd.DatetimeIndex)
    
    def test_write_nanosecond_precision(self, test_collection, sample_data_nanosecond):
        """Test writing data with nanosecond precision timestamps"""
        # This previously caused TypeError with 'times' parameter
        test_collection.write('test_item', sample_data_nanosecond)
        
        # Read back and verify
        item = test_collection.item('test_item')
        df_read = item.to_pandas()
        
        # Verify data integrity
        assert len(df_read) == len(sample_data_nanosecond)
        assert isinstance(df_read.index, pd.DatetimeIndex)
    
    def test_read_nonexistent_item(self, test_collection):
        """Test reading an item that doesn't exist"""
        with pytest.raises(pystore.ItemNotFoundError):
            test_collection.item('nonexistent')
    
    def test_read_with_filters(self, test_collection):
        """Test reading with filters"""
        # Create data with multiple values
        dates = pd.date_range('2024-01-01', periods=100, freq='D')
        data = pd.DataFrame({
            'value': range(100),
            'category': ['A'] * 50 + ['B'] * 50
        }, index=dates)
        
        test_collection.write('test_item', data)
        
        # Read with filter
        item = test_collection.item('test_item', filters=[('category', '==', 'A')])
        df_filtered = item.to_pandas()
        
        assert len(df_filtered) == 50
        assert all(df_filtered['category'] == 'A')
    
    def test_read_with_columns(self, test_collection, sample_data):
        """Test reading specific columns"""
        test_collection.write('test_item', sample_data)
        
        # Read only specific columns
        item = test_collection.item('test_item', columns=['value1'])
        df_read = item.to_pandas()
        
        assert list(df_read.columns) == ['value1']
        assert len(df_read) == len(sample_data)


class TestDatetimeIndexFreqPreservation:
    """Regression tests for item.py _restore_datetime_frequency.

    Parquet round-trips silently drop DatetimeIndex.freq.  The new
    ``index_freq`` metadata field must restore the *exact* frequency that was
    present at write time — including the case where freq was originally None
    (no inferred value should be injected on read-back).
    """

    def test_datetime_index_with_freq_preserved(self, test_collection):
        """A DatetimeIndex with an explicit freq must round-trip with that freq."""
        index = pd.date_range("2024-01-01", periods=10, freq="D")
        assert index.freq is not None, "pre-condition: index has freq"

        df = pd.DataFrame({"value": range(10)}, index=index)
        test_collection.write("freq_item", df)

        result = test_collection.item("freq_item").to_pandas()

        assert isinstance(result.index, pd.DatetimeIndex)
        assert result.index.freq is not None, "freq was dropped on read-back"
        assert result.index.freqstr == "D", (
            f"Expected freq='D', got {result.index.freqstr!r}"
        )

    def test_datetime_index_with_freq_none_preserved(self, test_collection):
        """A DatetimeIndex with freq=None must not have a freq injected on read-back.

        Before the fix, _restore_datetime_frequency would fall through to
        pd.infer_freq and silently attach a frequency to an index that the
        caller explicitly created without one.
        """
        # Build an index that has no frequency
        index = pd.DatetimeIndex(
            ["2024-01-01", "2024-01-03", "2024-01-07"]  # irregular spacing
        )
        assert index.freq is None, "pre-condition: irregular index has no freq"

        df = pd.DataFrame({"value": [1.0, 2.0, 3.0]}, index=index)
        test_collection.write("no_freq_item", df)

        result = test_collection.item("no_freq_item").to_pandas()

        assert isinstance(result.index, pd.DatetimeIndex)
        assert result.index.freq is None, (
            f"freq was incorrectly injected as {result.index.freq!r} "
            "for an index that had no frequency at write time"
        )

    def test_datetime_index_regular_spacing_explicit_freq_none(self, test_collection):
        """An index that *looks* regular but was created without a freq attribute
        (freq=None) must not have a freq inferred and injected on read-back.
        """
        # Create a regularly-spaced index without assigning a freq
        raw_timestamps = pd.date_range("2024-06-01", periods=5, freq="h")
        index = pd.DatetimeIndex(raw_timestamps.values)  # drops .freq
        assert index.freq is None, "pre-condition: freq stripped via .values"

        df = pd.DataFrame({"value": range(5)}, index=index)
        test_collection.write("stripped_freq_item", df)

        result = test_collection.item("stripped_freq_item").to_pandas()

        assert isinstance(result.index, pd.DatetimeIndex)
        assert result.index.freq is None, (
            f"freq={result.index.freq!r} was injected for an index "
            "whose original freq was None"
        )

    def test_datetime_index_hourly_freq_preserved(self, test_collection):
        """Hourly frequency must survive a write/read round-trip."""
        index = pd.date_range("2024-01-01", periods=24, freq="h")
        df = pd.DataFrame({"value": range(24)}, index=index)
        test_collection.write("hourly_item", df)

        result = test_collection.item("hourly_item").to_pandas()

        assert result.index.freq is not None
        assert result.index.freqstr == "h"