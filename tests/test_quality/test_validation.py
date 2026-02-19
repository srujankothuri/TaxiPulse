"""
TaxiPulse — Tests for Data Quality Validation Engine
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from quality.validate_data import (
    check_not_null,
    check_between,
    check_in_set,
    check_year_between,
    check_column_comparison,
    validate_bronze_data,
)
from quality.expectations.taxi_expectations import ALL_EXPECTATIONS


class TestNullCheck:
    """Test not-null validation."""

    def test_all_valid(self):
        df = pd.DataFrame({"col": [1, 2, 3]})
        mask = check_not_null(df, "col")
        assert mask.all()

    def test_with_nulls(self):
        df = pd.DataFrame({"col": [1, None, 3]})
        mask = check_not_null(df, "col")
        assert mask.sum() == 2
        assert not mask.iloc[1]

    def test_all_null(self):
        df = pd.DataFrame({"col": [None, None, None]})
        mask = check_not_null(df, "col")
        assert mask.sum() == 0


class TestRangeCheck:
    """Test between/range validation."""

    def test_all_in_range(self):
        df = pd.DataFrame({"fare": [10.0, 20.0, 50.0]})
        mask = check_between(df, "fare", 0, 500)
        assert mask.all()

    def test_below_range(self):
        df = pd.DataFrame({"fare": [-5.0, 10.0, 20.0]})
        mask = check_between(df, "fare", 0, 500)
        assert mask.sum() == 2
        assert not mask.iloc[0]

    def test_above_range(self):
        df = pd.DataFrame({"fare": [10.0, 20.0, 999.0]})
        mask = check_between(df, "fare", 0, 500)
        assert mask.sum() == 2
        assert not mask.iloc[2]

    def test_null_passes(self):
        """Null values should pass range check (null checks are separate)."""
        df = pd.DataFrame({"fare": [10.0, None, 20.0]})
        mask = check_between(df, "fare", 0, 500)
        assert mask.all()


class TestCategoricalCheck:
    """Test in-set validation."""

    def test_all_valid(self):
        df = pd.DataFrame({"payment": [1, 2, 3]})
        mask = check_in_set(df, "payment", [1, 2, 3, 4, 5])
        assert mask.all()

    def test_invalid_value(self):
        df = pd.DataFrame({"payment": [1, 99, 3]})
        mask = check_in_set(df, "payment", [1, 2, 3, 4, 5])
        assert mask.sum() == 2
        assert not mask.iloc[1]

    def test_null_passes(self):
        df = pd.DataFrame({"payment": [1, None, 3]})
        mask = check_in_set(df, "payment", [1, 2, 3])
        assert mask.all()


class TestTemporalCheck:
    """Test datetime validations."""

    def test_valid_year(self):
        df = pd.DataFrame({
            "dt": pd.to_datetime(["2024-01-01", "2024-06-15"])
        })
        mask = check_year_between(df, "dt", 2020, 2026)
        assert mask.all()

    def test_invalid_year(self):
        df = pd.DataFrame({
            "dt": pd.to_datetime(["2019-01-01", "2024-06-15"])
        })
        mask = check_year_between(df, "dt", 2020, 2026)
        assert mask.sum() == 1
        assert not mask.iloc[0]

    def test_dropoff_after_pickup(self):
        df = pd.DataFrame({
            "pickup": pd.to_datetime(["2024-01-01 08:00"]),
            "dropoff": pd.to_datetime(["2024-01-01 08:30"]),
        })
        mask = check_column_comparison(df, "dropoff", "pickup", ">=")
        assert mask.all()

    def test_dropoff_before_pickup(self):
        df = pd.DataFrame({
            "pickup": pd.to_datetime(["2024-01-01 08:30"]),
            "dropoff": pd.to_datetime(["2024-01-01 08:00"]),
        })
        mask = check_column_comparison(df, "dropoff", "pickup", ">=")
        assert not mask.iloc[0]


class TestFullValidation:
    """Test the full validation pipeline."""

    def _make_valid_row(self):
        """Create a single valid trip record."""
        return {
            "tpep_pickup_datetime": pd.Timestamp("2024-01-15 08:00"),
            "tpep_dropoff_datetime": pd.Timestamp("2024-01-15 08:30"),
            "fare_amount": 15.0,
            "total_amount": 20.0,
            "trip_distance": 3.5,
            "pu_location_id": 132,
            "do_location_id": 79,
            "passenger_count": 2,
            "tip_amount": 3.0,
            "tolls_amount": 0.0,
            "payment_type": 1,
            "vendor_id": 2,
            "ratecode_id": 1,
        }

    def test_valid_data_passes(self):
        """All-valid data should have 100% pass rate."""
        row = self._make_valid_row()
        df = pd.DataFrame([row] * 10)

        result = validate_bronze_data(df)
        assert result["summary"]["overall_pass_rate"] == 100.0
        assert len(result["clean_df"]) == 10
        assert len(result["quarantine_df"]) == 0

    def test_negative_fare_quarantined(self):
        """Negative fare should be quarantined."""
        row = self._make_valid_row()
        bad_row = {**row, "fare_amount": -50.0, "total_amount": -30.0}
        df = pd.DataFrame([row, bad_row])

        result = validate_bronze_data(df)
        assert len(result["clean_df"]) == 1
        assert len(result["quarantine_df"]) == 1

    def test_expectations_count(self):
        """Should have expected number of expectations."""
        assert len(ALL_EXPECTATIONS) >= 15

    def test_empty_dataframe(self):
        """Empty DataFrame should return empty results."""
        df = pd.DataFrame(columns=["tpep_pickup_datetime", "fare_amount"])
        result = validate_bronze_data(df)
        assert result["summary"]["total_rows"] == 0