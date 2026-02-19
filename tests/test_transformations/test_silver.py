"""
TaxiPulse — Tests for Silver Layer Transformations
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from transformations.silver.clean_and_validate import (
    standardize_columns,
    enforce_types,
    add_derived_columns,
    remove_duplicates,
)


class TestStandardizeColumns:
    """Test column name standardization."""

    def test_renames_columns(self):
        df = pd.DataFrame({
            "tpep_pickup_datetime": ["2024-01-01"],
            "tpep_dropoff_datetime": ["2024-01-01"],
            "pu_location_id": [132],
            "do_location_id": [79],
            "payment_type": [1],
            "ratecode_id": [1],
        })
        result = standardize_columns(df)
        assert "pickup_datetime" in result.columns
        assert "dropoff_datetime" in result.columns
        assert "pickup_location_id" in result.columns
        assert "dropoff_location_id" in result.columns
        assert "payment_type_id" in result.columns

    def test_keeps_unmapped_columns(self):
        df = pd.DataFrame({
            "fare_amount": [10.0],
            "tip_amount": [2.0],
        })
        result = standardize_columns(df)
        assert "fare_amount" in result.columns
        assert "tip_amount" in result.columns


class TestEnforceTypes:
    """Test data type enforcement."""

    def test_int_columns(self):
        df = pd.DataFrame({
            "vendor_id": [1.0, 2.0],
            "passenger_count": [1.0, None],
            "rate_code_id": [1.0, 2.0],
            "pickup_location_id": [132, 79],
            "dropoff_location_id": [79, 132],
            "payment_type_id": [1, 2],
            "pickup_datetime": ["2024-01-01", "2024-01-02"],
            "dropoff_datetime": ["2024-01-01", "2024-01-02"],
        })
        result = enforce_types(df)
        assert result["vendor_id"].dtype in [np.int64, np.int32, int]
        assert result["passenger_count"].iloc[1] == 0  # null filled with 0

    def test_datetime_columns(self):
        df = pd.DataFrame({
            "pickup_datetime": ["2024-01-01 08:00:00"],
            "dropoff_datetime": ["2024-01-01 08:30:00"],
        })
        result = enforce_types(df)
        assert pd.api.types.is_datetime64_any_dtype(result["pickup_datetime"])


class TestDerivedColumns:
    """Test derived column computation."""

    def test_trip_duration(self):
        df = pd.DataFrame({
            "pickup_datetime": pd.to_datetime(["2024-01-01 08:00:00"]),
            "dropoff_datetime": pd.to_datetime(["2024-01-01 08:30:00"]),
        })
        result = add_derived_columns(df)
        assert "trip_duration_minutes" in result.columns
        assert result["trip_duration_minutes"].iloc[0] == 30.0

    def test_duration_capped(self):
        """Duration should be capped at 1440 minutes (24 hours)."""
        df = pd.DataFrame({
            "pickup_datetime": pd.to_datetime(["2024-01-01 08:00:00"]),
            "dropoff_datetime": pd.to_datetime(["2024-01-03 08:00:00"]),
        })
        result = add_derived_columns(df)
        assert result["trip_duration_minutes"].iloc[0] == 1440

    def test_negative_duration_capped(self):
        """Negative duration should be capped at 0."""
        df = pd.DataFrame({
            "pickup_datetime": pd.to_datetime(["2024-01-01 09:00:00"]),
            "dropoff_datetime": pd.to_datetime(["2024-01-01 08:00:00"]),
        })
        result = add_derived_columns(df)
        assert result["trip_duration_minutes"].iloc[0] == 0


class TestRemoveDuplicates:
    """Test duplicate removal."""

    def test_removes_exact_duplicates(self):
        df = pd.DataFrame({
            "vendor_id": [1, 1, 2],
            "pickup_datetime": pd.to_datetime(["2024-01-01"] * 3),
            "dropoff_datetime": pd.to_datetime(["2024-01-01"] * 3),
            "pickup_location_id": [132, 132, 79],
            "dropoff_location_id": [79, 79, 132],
            "fare_amount": [10.0, 10.0, 20.0],
            "total_amount": [15.0, 15.0, 25.0],
        })
        result = remove_duplicates(df)
        assert len(result) == 2

    def test_keeps_different_rows(self):
        df = pd.DataFrame({
            "vendor_id": [1, 2],
            "pickup_datetime": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            "dropoff_datetime": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            "pickup_location_id": [132, 79],
            "dropoff_location_id": [79, 132],
            "fare_amount": [10.0, 20.0],
            "total_amount": [15.0, 25.0],
        })
        result = remove_duplicates(df)
        assert len(result) == 2