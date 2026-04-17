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

    def test_are_dtypes_compatible_datetime(self):
        dt_ns = pd.Series(pd.date_range("2024-01-01", periods=2, freq="D")).dtype
        dt_us = pd.Series(
            pd.to_datetime(["2024-01-01", "2024-01-02"]).as_unit("us")
        ).dtype

        assert are_dtypes_compatible(dt_ns, dt_us)

    def test_are_dtypes_compatible_datetime_tz(self):
        tz_utc = pd.Series(
            pd.date_range("2024-01-01", periods=2, freq="D", tz="UTC")
        ).dtype
        tz_utc2 = pd.Series(
            pd.date_range("2024-01-01", periods=2, freq="D", tz="UTC")
        ).dtype
        tz_us = pd.Series(
            pd.date_range("2024-01-01", periods=2, freq="D", tz="US/Eastern")
        ).dtype
        naive = pd.Series(pd.date_range("2024-01-01", periods=2, freq="D")).dtype

        # Same TZ is compatible
        assert are_dtypes_compatible(tz_utc, tz_utc2)
        # Different TZ is not compatible
        assert not are_dtypes_compatible(tz_utc, tz_us)
        # TZ-aware vs naive is not compatible
        assert not are_dtypes_compatible(tz_utc, naive)
        assert not are_dtypes_compatible(naive, tz_utc)

    def test_are_dtypes_compatible_timedelta(self):
        td_ns = pd.Series(pd.to_timedelta(["1 day", "2 days"])).dtype
        td_us = pd.Series(
            pd.to_timedelta(["1 day", "2 days"]).as_unit("us")
        ).dtype

        assert are_dtypes_compatible(td_ns, td_us)

    def test_are_dtypes_compatible_cross_type_rejection(self):
        int_dtype = pd.Series([1, 2], dtype="int64").dtype
        str_dtype = pd.Series(["a", "b"], dtype="object").dtype
        bool_dtype = pd.Series([True, False], dtype="bool").dtype
        dt_dtype = pd.Series(pd.date_range("2024-01-01", periods=2)).dtype

        assert not are_dtypes_compatible(int_dtype, str_dtype)
        assert not are_dtypes_compatible(str_dtype, dt_dtype)
        assert not are_dtypes_compatible(bool_dtype, int_dtype)
        assert not are_dtypes_compatible(dt_dtype, int_dtype)
