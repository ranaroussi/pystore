"""
Tests for PyStore collection functionality
"""

import numpy as np
import pandas as pd
import pytest

import pystore


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
                "interval_col": pd.array(
                    pd.interval_range(start=0, end=3), dtype="interval[int64, right]"
                ),
                "value": [10.0, 20.0, 30.0],
            },
            index=pd.date_range("2024-01-01", periods=3, freq="D"),
        )
        test_collection.write("interval_item", df1)

        df2 = pd.DataFrame(
            {
                "interval_col": pd.array(
                    pd.interval_range(start=3, end=5), dtype="interval[int64, right]"
                ),
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
