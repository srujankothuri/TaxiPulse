"""
TaxiPulse — Data Quality Expectations for NYC Taxi Data
Defines validation rules that every trip record must pass
before being promoted from Bronze to Silver layer.

Each expectation is a dict with:
  - name: Human-readable name
  - column: Column to validate
  - type: Type of check (not_null, between, in_set, custom)
  - params: Parameters for the check
  - severity: "critical" (blocks record) or "warning" (logs but allows)
"""

# ============================================================
# Null Checks — Critical fields must not be null
# ============================================================

NULL_CHECKS = [
    {
        "name": "pickup_datetime_not_null",
        "column": "tpep_pickup_datetime",
        "type": "not_null",
        "severity": "critical",
        "description": "Pickup datetime must not be null",
    },
    {
        "name": "dropoff_datetime_not_null",
        "column": "tpep_dropoff_datetime",
        "type": "not_null",
        "severity": "critical",
        "description": "Dropoff datetime must not be null",
    },
    {
        "name": "fare_amount_not_null",
        "column": "fare_amount",
        "type": "not_null",
        "severity": "critical",
        "description": "Fare amount must not be null",
    },
    {
        "name": "total_amount_not_null",
        "column": "total_amount",
        "type": "not_null",
        "severity": "critical",
        "description": "Total amount must not be null",
    },
    {
        "name": "trip_distance_not_null",
        "column": "trip_distance",
        "type": "not_null",
        "severity": "critical",
        "description": "Trip distance must not be null",
    },
    {
        "name": "pu_location_not_null",
        "column": "pu_location_id",
        "type": "not_null",
        "severity": "critical",
        "description": "Pickup location must not be null",
    },
    {
        "name": "do_location_not_null",
        "column": "do_location_id",
        "type": "not_null",
        "severity": "critical",
        "description": "Dropoff location must not be null",
    },
]

# ============================================================
# Range Checks — Numeric fields must be within valid ranges
# ============================================================

RANGE_CHECKS = [
    {
        "name": "fare_amount_range",
        "column": "fare_amount",
        "type": "between",
        "params": {"min": 0.0, "max": 500.0},
        "severity": "critical",
        "description": "Fare must be between $0 and $500",
    },
    {
        "name": "total_amount_range",
        "column": "total_amount",
        "type": "between",
        "params": {"min": 0.0, "max": 1000.0},
        "severity": "critical",
        "description": "Total amount must be between $0 and $1000",
    },
    {
        "name": "trip_distance_range",
        "column": "trip_distance",
        "type": "between",
        "params": {"min": 0.0, "max": 200.0},
        "severity": "critical",
        "description": "Trip distance must be between 0 and 200 miles",
    },
    {
        "name": "passenger_count_range",
        "column": "passenger_count",
        "type": "between",
        "params": {"min": 0, "max": 9},
        "severity": "warning",
        "description": "Passenger count should be between 0 and 9",
    },
    {
        "name": "tip_amount_range",
        "column": "tip_amount",
        "type": "between",
        "params": {"min": 0.0, "max": 200.0},
        "severity": "critical",
        "description": "Tip must be between $0 and $200",
    },
    {
        "name": "tolls_amount_range",
        "column": "tolls_amount",
        "type": "between",
        "params": {"min": 0.0, "max": 200.0},
        "severity": "critical",
        "description": "Tolls must be between $0 and $200",
    },
]

# ============================================================
# Categorical Checks — Values must be in expected sets
# ============================================================

CATEGORICAL_CHECKS = [
    {
        "name": "payment_type_valid",
        "column": "payment_type",
        "type": "in_set",
        "params": {"values": [1, 2, 3, 4, 5, 6]},
        "severity": "warning",
        "description": "Payment type must be 1-6",
    },
    {
        "name": "vendor_id_valid",
        "column": "vendor_id",
        "type": "in_set",
        "params": {"values": [1, 2]},
        "severity": "warning",
        "description": "Vendor ID must be 1 or 2",
    },
    {
        "name": "ratecode_valid",
        "column": "ratecode_id",
        "type": "in_set",
        "params": {"values": [1, 2, 3, 4, 5, 6, 99]},
        "severity": "warning",
        "description": "Rate code must be 1-6 or 99",
    },
]

# ============================================================
# Temporal Checks — Date/time fields must be reasonable
# ============================================================

TEMPORAL_CHECKS = [
    {
        "name": "pickup_year_valid",
        "column": "tpep_pickup_datetime",
        "type": "year_between",
        "params": {"min_year": 2020, "max_year": 2026},
        "severity": "critical",
        "description": "Pickup year must be between 2020 and 2026",
    },
    {
        "name": "dropoff_after_pickup",
        "type": "column_comparison",
        "params": {
            "column_a": "tpep_dropoff_datetime",
            "column_b": "tpep_pickup_datetime",
            "operator": ">=",
        },
        "severity": "critical",
        "description": "Dropoff time must be >= pickup time",
    },
]

# ============================================================
# Combine All Expectations
# ============================================================

ALL_EXPECTATIONS = (
    NULL_CHECKS + RANGE_CHECKS + CATEGORICAL_CHECKS + TEMPORAL_CHECKS
)

# Quick counts for logging
TOTAL_EXPECTATIONS = len(ALL_EXPECTATIONS)
CRITICAL_COUNT = sum(1 for e in ALL_EXPECTATIONS if e["severity"] == "critical")
WARNING_COUNT = sum(1 for e in ALL_EXPECTATIONS if e["severity"] == "warning")