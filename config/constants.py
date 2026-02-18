"""
TaxiPulse — Constants
Static values used across the project.
"""

# ============================================================
# NYC TLC Yellow Taxi Schema
# These are the columns in the raw Parquet files
# ============================================================

RAW_COLUMNS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "Airport_fee",
]

# ============================================================
# Standardized column names (used in Silver layer)
# ============================================================

SILVER_COLUMNS = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "RatecodeID": "rate_code_id",
    "store_and_fwd_flag": "store_and_fwd_flag",
    "PULocationID": "pickup_location_id",
    "DOLocationID": "dropoff_location_id",
    "payment_type": "payment_type_id",
    "fare_amount": "fare_amount",
    "extra": "extra",
    "mta_tax": "mta_tax",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "total_amount": "total_amount",
    "congestion_surcharge": "congestion_surcharge",
    "Airport_fee": "airport_fee",
}

# ============================================================
# Payment Type Mapping
# ============================================================

PAYMENT_TYPES = {
    1: "Credit Card",
    2: "Cash",
    3: "No Charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided Trip",
}

# ============================================================
# Rate Code Mapping
# ============================================================

RATE_CODES = {
    1: "Standard Rate",
    2: "JFK",
    3: "Newark",
    4: "Nassau or Westchester",
    5: "Negotiated Fare",
    6: "Group Ride",
    99: "Unknown",
}

# ============================================================
# Vendor Mapping
# ============================================================

VENDORS = {
    1: "Creative Mobile Technologies",
    2: "VeriFone Inc.",
}

# ============================================================
# Data Quality Thresholds
# ============================================================

QUALITY_THRESHOLDS = {
    "fare_amount_min": 0.0,
    "fare_amount_max": 500.0,
    "trip_distance_min": 0.0,
    "trip_distance_max": 200.0,
    "passenger_count_min": 0,
    "passenger_count_max": 9,
    "total_amount_min": 0.0,
    "total_amount_max": 1000.0,
    "tip_amount_min": 0.0,
    "tip_amount_max": 200.0,
    "null_threshold_pct": 5.0,  # Max 5% nulls allowed
}

# ============================================================
# BigQuery Table Names
# ============================================================

BQ_TABLES = {
    # Bronze
    "raw_trips": "raw_yellow_trips",
    # Silver
    "clean_trips": "clean_yellow_trips",
    "quarantined_trips": "quarantined_yellow_trips",
    # Gold - Fact
    "fact_trips": "fact_trips",
    # Gold - Dimensions
    "dim_datetime": "dim_datetime",
    "dim_pickup_location": "dim_pickup_location",
    "dim_dropoff_location": "dim_dropoff_location",
    "dim_payment_type": "dim_payment_type",
    "dim_rate_code": "dim_rate_code",
    # Gold - Aggregations
    "agg_hourly_zone_revenue": "agg_hourly_zone_revenue",
    "agg_daily_summary": "agg_daily_summary",
    # Anomaly
    "anomaly_log": "anomaly_log",
    # Data Quality
    "quality_log": "quality_log",
}