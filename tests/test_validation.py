"""
Tests for dtype compatibility validation.
"""

import pandas as pd
import pytest

import pystore
from pystore.dataframe import are_dtypes_compatible


class TestDataValidation:
    def test_append_validates_dtype_compatibility(self, test_collection):
        """validate_schema=True should reject incompatible column dtypes."""
        initial_data = pd.DataFrame({"value": pd.Series([1, 2, 3], dtype="int64")})
        test_collection.write("dtype_item", initial_data)

        incompatible_data = pd.DataFrame(
            {"value": pd.Series(["4", "5"], dtype="object")}
        )

        with pytest.raises(pystore.ValidationError, match="Dtype mismatch"):
            test_collection.append(
                "dtype_item", incompatible_data, validate_schema=True
            )


class TestDtypeCompatibility:
    def test_are_dtypes_compatible_exact_match(self):
        int64_dtype = pd.Series([1, 2, 3], dtype="int64").dtype
        bool_dtype = pd.Series([True, False], dtype="bool").dtype

        assert are_dtypes_compatible(int64_dtype, int64_dtype)
        assert not are_dtypes_compatible(int64_dtype, bool_dtype)

    def test_are_dtypes_compatible_numeric(self):
        int32_dtype = pd.Series([1, 2, 3], dtype="int32").dtype
        int64_dtype = pd.Series([1, 2, 3], dtype="int64").dtype
        float64_dtype = pd.Series([1.0, 2.0, 3.0], dtype="float64").dtype

        assert are_dtypes_compatible(int32_dtype, int64_dtype)
        assert are_dtypes_compatible(int64_dtype, float64_dtype)

    def test_are_dtypes_compatible_string(self):
        object_dtype = pd.Series(["a", "b"], dtype="object").dtype
        string_dtype = pd.Series(["a", "b"], dtype="string").dtype
        pyarrow_string_dtype = pd.Series(
            ["a", "b"], dtype=pd.StringDtype(storage="pyarrow")
        ).dtype

        assert are_dtypes_compatible(object_dtype, string_dtype)
        assert are_dtypes_compatible(string_dtype, pyarrow_string_dtype)
        assert are_dtypes_compatible(object_dtype, pyarrow_string_dtype)
