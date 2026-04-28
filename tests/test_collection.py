"""
Tests for PyStore collection functionality
"""

import pandas as pd
import pytest

import pystore

INVALID_NAMES = [
    "",
    "   ",
    ".",
    "..",
    "../escape",
    "nested/name",
    r"nested\name",
    "/tmp/abs",
]


class TestCollection:
    """Test collection operations"""

    def test_collection_creation(self, test_store):
        """Test creating a new collection"""
        test_store.collection("test_collection")
        assert "test_collection" in test_store.list_collections()

    def test_collection_exists_error(self, test_store):
        """Test reusing an existing collection without overwrite"""
        collection1 = test_store.collection("test_collection")
        collection2 = test_store.collection("test_collection", overwrite=False)

        assert collection2.collection == collection1.collection
        assert collection2.datastore == collection1.datastore

    def test_collection_overwrite(self, test_store):
        """Test overwriting an existing collection"""
        # Create collection with data
        collection1 = test_store.collection("test_collection")
        data = pd.DataFrame({"value": [1, 2, 3]})
        collection1.write("item1", data)

        # Overwrite collection
        collection2 = test_store.collection("test_collection", overwrite=True)
        assert "item1" not in collection2.list_items()

    def test_delete_collection(self, test_store):
        """Test deleting a collection"""
        test_store.collection("test_collection")
        assert "test_collection" in test_store.list_collections()

        test_store.delete_collection("test_collection")
        assert "test_collection" not in test_store.list_collections()

    def test_delete_nonexistent_collection(self, test_store):
        """Test deleting a collection that doesn't exist"""
        with pytest.raises(pystore.CollectionNotFoundError):
            test_store.delete_collection("nonexistent")

    @pytest.mark.parametrize("collection_name", INVALID_NAMES)
    def test_rejects_invalid_collection_names(self, test_store, collection_name):
        """Collection APIs reject empty, nested, and escaping names."""
        with pytest.raises(ValueError):
            test_store.collection(collection_name)

        with pytest.raises(ValueError):
            test_store.delete_collection(collection_name)

    def test_list_items(self, test_collection, sample_data):
        """Test listing items in a collection"""
        # Write multiple items
        for i in range(3):
            test_collection.write(f"item_{i}", sample_data)

        items = test_collection.list_items()
        assert len(items) == 3
        for i in range(3):
            assert f"item_{i}" in items

    def test_list_items_with_metadata_filter(self, test_collection, sample_data):
        """Test listing items with metadata filter"""
        # Write items with different metadata
        test_collection.write("item1", sample_data, metadata={"type": "A"})
        test_collection.write("item2", sample_data, metadata={"type": "B"})
        test_collection.write("item3", sample_data, metadata={"type": "A"})

        # Filter by metadata
        items_a = test_collection.list_items(type="A")
        assert len(items_a) == 2
        assert "item1" in items_a
        assert "item3" in items_a
        assert "item2" not in items_a

    @pytest.mark.parametrize("item_name", INVALID_NAMES)
    def test_rejects_invalid_item_names(self, test_collection, sample_data, item_name):
        """Item APIs reject empty, nested, and escaping names."""
        with pytest.raises(ValueError):
            test_collection.write(item_name, sample_data)

        with pytest.raises(ValueError):
            test_collection.item(item_name)

        with pytest.raises(ValueError):
            test_collection.delete_item(item_name)


class TestAppendValidateSchemaTransformed:
    """Regression tests for collection.py _validate_schema_compatibility.

    The validate_schema path must compare against the *restored* pandas
    schema (current_df), not the raw on-disk Dask schema (current.data).
    When an item was written with a MultiIndex or with interval/category
    columns those two representations differ; using the raw Dask columns
    would raise a spurious ValidationError.
    """

    def test_validate_schema_multiindex_append(self, test_collection):
        """validate_schema=True must not raise when appending to a MultiIndex item.

        After storage, current.data has the MultiIndex levels as plain columns.
        The validate_schema path must compare against current_df (restored
        MultiIndex), otherwise the column sets differ and a false mismatch is
        reported.
        """
        index1 = pd.MultiIndex.from_tuples(
            [("A", "2024-01-01"), ("A", "2024-01-02"), ("B", "2024-01-01")],
            names=["category", "date"],
        )
        df1 = pd.DataFrame({"value": [1.0, 2.0, 3.0]}, index=index1)

        test_collection.write("mi_item", df1)

        index2 = pd.MultiIndex.from_tuples(
            [("A", "2024-01-03"), ("B", "2024-01-02")],
            names=["category", "date"],
        )
        df2 = pd.DataFrame({"value": [4.0, 5.0]}, index=index2)

        # Must not raise ValidationError even though current.data columns
        # include the flattened index levels ("category", "date", "value").
        test_collection.append("mi_item", df2, validate_schema=True)

        result = test_collection.item("mi_item").to_pandas()
        assert isinstance(result.index, pd.MultiIndex)
        assert len(result) == 5
        assert list(result.columns) == ["value"]

    def test_validate_schema_category_column_append(self, test_collection):
        """validate_schema=True must not raise when appending to an item with
        a category-dtype column.

        DataTypeHandler serialises category columns for storage; the
        raw on-disk schema diverges from the pandas-visible schema.
        validate_schema must compare the pandas-level schemas, not the
        storage-level ones.
        """
        df1 = pd.DataFrame(
            {
                "cat_col": pd.Categorical(
                    ["a", "b", "c"], categories=["a", "b", "c"], ordered=False
                ),
                "value": [1.0, 2.0, 3.0],
            },
            index=pd.date_range("2024-01-01", periods=3, freq="D"),
        )
        test_collection.write("cat_item", df1)

        df2 = pd.DataFrame(
            {
                "cat_col": pd.Categorical(
                    ["a", "c"], categories=["a", "b", "c"], ordered=False
                ),
                "value": [4.0, 5.0],
            },
            index=pd.date_range("2024-01-04", periods=2, freq="D"),
        )

        # Must not raise ValidationError — both DataFrames have identical
        # column names at the pandas level.
        test_collection.append("cat_item", df2, validate_schema=True)

        result = test_collection.item("cat_item").to_pandas()
        assert len(result) == 5
        assert set(result.columns) == {"cat_col", "value"}

    def test_validate_schema_interval_column_append(self, test_collection):
        """validate_schema=True must not raise when appending to an item that
        has an interval-dtype column.
        """
        df1 = pd.DataFrame(
            {
                "interval_col": pd.interval_range(start=0, end=3),
                "value": [10.0, 20.0, 30.0],
            },
            index=pd.date_range("2024-01-01", periods=3, freq="D"),
        )
        test_collection.write("interval_item", df1)

        df2 = pd.DataFrame(
            {
                "interval_col": pd.interval_range(start=3, end=5),
                "value": [40.0, 50.0],
            },
            index=pd.date_range("2024-01-04", periods=2, freq="D"),
        )

        # Must not raise ValidationError.
        test_collection.append("interval_item", df2, validate_schema=True)

        result = test_collection.item("interval_item").to_pandas()
        assert len(result) == 5
        assert set(result.columns) == {"interval_col", "value"}

    def test_validate_schema_rejects_mismatched_columns(self, test_collection):
        """validate_schema=True must still raise when columns genuinely differ."""
        df1 = pd.DataFrame(
            {"col_a": [1.0, 2.0], "col_b": [3.0, 4.0]},
            index=pd.date_range("2024-01-01", periods=2, freq="D"),
        )
        test_collection.write("mismatch_item", df1)

        df2 = pd.DataFrame(
            {"col_a": [5.0], "col_c": [6.0]},  # col_b missing, col_c extra
            index=pd.date_range("2024-01-03", periods=1, freq="D"),
        )

        with pytest.raises(pystore.ValidationError):
            test_collection.append("mismatch_item", df2, validate_schema=True)


class TestMetadataCacheEviction:
    """Test that the metadata cache evicts entries beyond _METADATA_CACHE_MAX."""

    def test_cache_eviction_occurs_beyond_max(self, test_collection):
        """Inserting > _METADATA_CACHE_MAX items should evict the least-recently-accessed."""
        from pystore.collection import Collection

        max_cache = Collection._METADATA_CACHE_MAX

        # Write more items than the cache limit
        for i in range(max_cache + 10):
            data = pd.DataFrame({"value": [i]}, index=pd.date_range("2024-01-01", periods=1, freq="D"))
            test_collection.write(f"evict_item_{i}", data)

        # Access metadata for all items — this will fill the cache
        for i in range(max_cache + 10):
            test_collection.get_item_metadata(f"evict_item_{i}", use_cache=True)

        # The cache should not exceed _METADATA_CACHE_MAX
        assert len(test_collection._metadata_cache) <= max_cache
        assert len(test_collection._cache_timestamp) <= max_cache

        # The earliest-accessed items should have been evicted
        # Items 0–9 should be evicted (first accessed, then pushed out by later items)
        for i in range(10):
            assert f"evict_item_{i}" not in test_collection._metadata_cache

        # The most recently accessed items should still be in cache
        for i in range(max_cache - 5, max_cache + 10):
            assert f"evict_item_{i}" in test_collection._metadata_cache

    def test_clear_metadata_cache_specific_item(self, test_collection):
        """clear_metadata_cache(item) should remove only that item's cache entry."""
        data = pd.DataFrame({"value": [1]}, index=pd.date_range("2024-01-01", periods=1, freq="D"))
        test_collection.write("cache_item_a", data)
        test_collection.write("cache_item_b", data)

        # Populate cache
        test_collection.get_item_metadata("cache_item_a", use_cache=True)
        test_collection.get_item_metadata("cache_item_b", use_cache=True)

        assert "cache_item_a" in test_collection._metadata_cache
        assert "cache_item_b" in test_collection._metadata_cache

        # Clear only one
        test_collection.clear_metadata_cache("cache_item_a")

        assert "cache_item_a" not in test_collection._metadata_cache
        assert "cache_item_b" in test_collection._metadata_cache

    def test_clear_metadata_cache_all(self, test_collection):
        """clear_metadata_cache() with no args should clear the entire cache."""
        data = pd.DataFrame({"value": [1]}, index=pd.date_range("2024-01-01", periods=1, freq="D"))
        test_collection.write("cache_item_c", data)
        test_collection.write("cache_item_d", data)

        test_collection.get_item_metadata("cache_item_c", use_cache=True)
        test_collection.get_item_metadata("cache_item_d", use_cache=True)

        test_collection.clear_metadata_cache()

        assert len(test_collection._metadata_cache) == 0
        assert len(test_collection._cache_timestamp) == 0


class TestOverwriteRecovery:
    """Test that append recovers when a previous attempt left a __tmp item on disk."""

    def test_append_recovers_from_stale_tmp_item(self, test_collection):
        """A failed append that leaves a __tmp directory should not prevent retry."""
        import shutil

        from pystore import utils

        # Write initial item
        initial = pd.DataFrame(
            {"value": [1, 2, 3]},
            index=pd.date_range("2024-01-01", periods=3, freq="D"),
        )
        test_collection.write("recover_item", initial)

        # Simulate a leftover __tmp directory from a prior failed append
        tmp_path = test_collection.get_item_path("__recover_item")
        tmp_path.mkdir(parents=True, exist_ok=True)
        # Write a stale parquet file into it so it's non-empty
        stale_df = pd.DataFrame({"value": [0]}, index=pd.date_range("2020-01-01", periods=1, freq="D"))
        stale_df.to_parquet(str(tmp_path / "part.0.parquet"))

        # Now append should succeed — overwrite=True on the temp item will
        # replace the stale directory.
        new_data = pd.DataFrame(
            {"value": [4, 5]},
            index=pd.date_range("2024-01-04", periods=2, freq="D"),
        )
        test_collection.append("recover_item", new_data)

        # Verify the data is correct (3 original + 2 appended)
        result = test_collection.item("recover_item").to_pandas()
        assert len(result) == 5
        assert list(result["value"]) == [1, 2, 3, 4, 5]


class TestItemPathDeprecation:
    """Test that _item_path emits a DeprecationWarning."""

    def test_item_path_emits_deprecation_warning(self, test_collection, sample_data):
        """Calling _item_path must emit a DeprecationWarning."""
        test_collection.write("depr_item", sample_data)

        with pytest.warns(DeprecationWarning, match="_item_path is deprecated"):
            path = test_collection._item_path("depr_item")

        # Result should match get_item_path
        expected = test_collection.get_item_path("depr_item")
        assert path == expected

    def test_item_path_as_string_emits_deprecation_warning(self, test_collection):
        """Calling _item_path(as_string=True) must also emit a DeprecationWarning."""
        with pytest.warns(DeprecationWarning, match="_item_path is deprecated"):
            path = test_collection._item_path("some_item", as_string=True)

        expected = test_collection.get_item_path("some_item", as_string=True)
        assert path == expected


class TestWriteThreadedDeprecation:
    """Test that write_threaded emits a DeprecationWarning."""

    def test_write_threaded_emits_deprecation_warning(self, test_collection, sample_data):
        """Calling write_threaded must emit a DeprecationWarning."""
        with pytest.warns(DeprecationWarning, match="write_threaded is deprecated"):
            test_collection.write_threaded("threaded_item", sample_data)

        # Data should still be written correctly (delegates to write)
        result = test_collection.item("threaded_item").to_pandas()
        pd.testing.assert_frame_equal(result, sample_data)

    def test_write_threaded_overwrite_emits_deprecation_warning(
        self, test_collection, sample_data
    ):
        """Calling write_threaded with overwrite must emit a DeprecationWarning."""
        test_collection.write("threaded_overwrite_item", sample_data)

        with pytest.warns(DeprecationWarning, match="write_threaded is deprecated"):
            test_collection.write_threaded(
                "threaded_overwrite_item", sample_data, overwrite=True
            )


class TestIndexEmptyGuard:
    """Test that Collection.index(last=True) handles empty index gracefully."""

    def test_index_last_empty_index_returns_none(self, test_collection):
        """When the computed index is empty, index(item, last=True) returns None."""
        from unittest.mock import patch

        # Write a real item so the path exists
        data = pd.DataFrame(
            {"value": [1, 2, 3]},
            index=pd.date_range("2024-01-01", periods=3, freq="D"),
        )
        test_collection.write("guard_item", data)

        # Mock dd.read_parquet to return a Dask DataFrame with an empty index
        empty_df = pd.DataFrame({"value": []}, index=pd.DatetimeIndex([], name="index"))
        import dask.dataframe as dd

        mock_dask_df = dd.from_pandas(empty_df, npartitions=1)

        with patch("pystore.collection.dd.read_parquet", return_value=mock_dask_df):
            result = test_collection.index("guard_item", last=True)

        assert result is None
