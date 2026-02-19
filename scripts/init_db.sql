-- ============================================================
-- TaxiPulse — PostgreSQL Database Initialization
-- ============================================================
-- This script runs automatically when the PostgreSQL container
-- starts for the first time. It creates the medallion layer
-- schemas (bronze, silver, gold) for the data warehouse.
-- ============================================================

-- Create Medallion Architecture Schemas
CREATE SCHEMA IF NOT EXISTS bronze;
COMMENT ON SCHEMA bronze IS 'Raw, unprocessed NYC taxi trip data';

CREATE SCHEMA IF NOT EXISTS silver;
COMMENT ON SCHEMA silver IS 'Cleaned and validated taxi trip data';

CREATE SCHEMA IF NOT EXISTS gold;
COMMENT ON SCHEMA gold IS 'Business-ready star schema and aggregations';

-- ============================================================
-- Bronze Layer — Raw trips table
-- ============================================================
-- Stores raw data exactly as received from NYC TLC files.
-- No transformations, no cleaning — source of truth backup.

CREATE TABLE IF NOT EXISTS bronze.raw_yellow_trips (
    vendor_id                INTEGER,
    tpep_pickup_datetime     TIMESTAMP,
    tpep_dropoff_datetime    TIMESTAMP,
    passenger_count          DOUBLE PRECISION,
    trip_distance            DOUBLE PRECISION,
    ratecode_id              DOUBLE PRECISION,
    store_and_fwd_flag       VARCHAR(1),
    pu_location_id           INTEGER,
    do_location_id           INTEGER,
    payment_type             INTEGER,
    fare_amount              DOUBLE PRECISION,
    extra                    DOUBLE PRECISION,
    mta_tax                  DOUBLE PRECISION,
    tip_amount               DOUBLE PRECISION,
    tolls_amount             DOUBLE PRECISION,
    improvement_surcharge    DOUBLE PRECISION,
    total_amount             DOUBLE PRECISION,
    congestion_surcharge     DOUBLE PRECISION,
    airport_fee              DOUBLE PRECISION,
    -- Metadata columns
    load_timestamp           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file              VARCHAR(255)
);

COMMENT ON TABLE bronze.raw_yellow_trips IS 'Raw NYC TLC Yellow Taxi trip records';

-- ============================================================
-- Silver Layer — Cleaned trips table
-- ============================================================
-- Cleaned, validated, standardized data.
-- Nulls handled, duplicates removed, types enforced.

CREATE TABLE IF NOT EXISTS silver.clean_yellow_trips (
    trip_id                  SERIAL PRIMARY KEY,
    vendor_id                INTEGER NOT NULL,
    pickup_datetime          TIMESTAMP NOT NULL,
    dropoff_datetime         TIMESTAMP NOT NULL,
    passenger_count          INTEGER,
    trip_distance            DOUBLE PRECISION NOT NULL,
    rate_code_id             INTEGER,
    store_and_fwd_flag       VARCHAR(1),
    pickup_location_id       INTEGER NOT NULL,
    dropoff_location_id      INTEGER NOT NULL,
    payment_type_id          INTEGER NOT NULL,
    fare_amount              DOUBLE PRECISION NOT NULL,
    extra                    DOUBLE PRECISION DEFAULT 0,
    mta_tax                  DOUBLE PRECISION DEFAULT 0,
    tip_amount               DOUBLE PRECISION DEFAULT 0,
    tolls_amount             DOUBLE PRECISION DEFAULT 0,
    improvement_surcharge    DOUBLE PRECISION DEFAULT 0,
    total_amount             DOUBLE PRECISION NOT NULL,
    congestion_surcharge     DOUBLE PRECISION DEFAULT 0,
    airport_fee              DOUBLE PRECISION DEFAULT 0,
    -- Derived columns
    trip_duration_minutes    DOUBLE PRECISION,
    -- Metadata
    load_timestamp           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file              VARCHAR(255)
);

COMMENT ON TABLE silver.clean_yellow_trips IS 'Cleaned and validated taxi trip records';

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_silver_pickup_datetime
    ON silver.clean_yellow_trips (pickup_datetime);
CREATE INDEX IF NOT EXISTS idx_silver_pickup_location
    ON silver.clean_yellow_trips (pickup_location_id);
CREATE INDEX IF NOT EXISTS idx_silver_dropoff_location
    ON silver.clean_yellow_trips (dropoff_location_id);

-- ============================================================
-- Silver Layer — Quarantined (failed quality checks)
-- ============================================================

CREATE TABLE IF NOT EXISTS silver.quarantined_yellow_trips (
    vendor_id                INTEGER,
    pickup_datetime          TIMESTAMP,
    dropoff_datetime         TIMESTAMP,
    passenger_count          DOUBLE PRECISION,
    trip_distance            DOUBLE PRECISION,
    rate_code_id             DOUBLE PRECISION,
    pickup_location_id       INTEGER,
    dropoff_location_id      INTEGER,
    payment_type_id          INTEGER,
    fare_amount              DOUBLE PRECISION,
    total_amount             DOUBLE PRECISION,
    -- Why it was quarantined
    quarantine_reason        TEXT,
    quarantine_timestamp     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file              VARCHAR(255)
);

COMMENT ON TABLE silver.quarantined_yellow_trips IS 'Records that failed data quality validation';

-- ============================================================
-- Gold Layer — Dimension Tables (Star Schema)
-- ============================================================

-- Dimension: Date/Time
CREATE TABLE IF NOT EXISTS gold.dim_datetime (
    datetime_id              SERIAL PRIMARY KEY,
    full_datetime            TIMESTAMP UNIQUE NOT NULL,
    date                     DATE NOT NULL,
    year                     INTEGER NOT NULL,
    month                    INTEGER NOT NULL,
    day                      INTEGER NOT NULL,
    hour                     INTEGER NOT NULL,
    day_of_week              INTEGER NOT NULL,    -- 0=Monday, 6=Sunday
    day_name                 VARCHAR(10) NOT NULL, -- Monday, Tuesday, etc.
    month_name               VARCHAR(10) NOT NULL, -- January, February, etc.
    is_weekend               BOOLEAN NOT NULL,
    quarter                  INTEGER NOT NULL
);

COMMENT ON TABLE gold.dim_datetime IS 'Date and time dimension for trip analysis';

-- Dimension: Payment Type
CREATE TABLE IF NOT EXISTS gold.dim_payment_type (
    payment_type_id          INTEGER PRIMARY KEY,
    payment_description      VARCHAR(50) NOT NULL
);

COMMENT ON TABLE gold.dim_payment_type IS 'Payment method dimension';

INSERT INTO gold.dim_payment_type (payment_type_id, payment_description) VALUES
    (1, 'Credit Card'),
    (2, 'Cash'),
    (3, 'No Charge'),
    (4, 'Dispute'),
    (5, 'Unknown'),
    (6, 'Voided Trip')
ON CONFLICT (payment_type_id) DO NOTHING;

-- Dimension: Rate Code
CREATE TABLE IF NOT EXISTS gold.dim_rate_code (
    rate_code_id             INTEGER PRIMARY KEY,
    rate_description         VARCHAR(50) NOT NULL
);

COMMENT ON TABLE gold.dim_rate_code IS 'Rate code dimension (fare calculation method)';

INSERT INTO gold.dim_rate_code (rate_code_id, rate_description) VALUES
    (1, 'Standard Rate'),
    (2, 'JFK'),
    (3, 'Newark'),
    (4, 'Nassau or Westchester'),
    (5, 'Negotiated Fare'),
    (6, 'Group Ride'),
    (99, 'Unknown')
ON CONFLICT (rate_code_id) DO NOTHING;

-- Dimension: Pickup Location
CREATE TABLE IF NOT EXISTS gold.dim_pickup_location (
    pickup_location_id       INTEGER PRIMARY KEY,
    borough                  VARCHAR(50),
    zone_name                VARCHAR(100),
    service_zone             VARCHAR(50)
);

COMMENT ON TABLE gold.dim_pickup_location IS 'Pickup location zone dimension';

-- Dimension: Dropoff Location
CREATE TABLE IF NOT EXISTS gold.dim_dropoff_location (
    dropoff_location_id      INTEGER PRIMARY KEY,
    borough                  VARCHAR(50),
    zone_name                VARCHAR(100),
    service_zone             VARCHAR(50)
);

COMMENT ON TABLE gold.dim_dropoff_location IS 'Dropoff location zone dimension';

-- ============================================================
-- Gold Layer — Fact Table
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.fact_trips (
    trip_id                  SERIAL PRIMARY KEY,
    datetime_id              INTEGER REFERENCES gold.dim_datetime(datetime_id),
    pickup_location_id       INTEGER REFERENCES gold.dim_pickup_location(pickup_location_id),
    dropoff_location_id      INTEGER REFERENCES gold.dim_dropoff_location(dropoff_location_id),
    payment_type_id          INTEGER REFERENCES gold.dim_payment_type(payment_type_id),
    rate_code_id             INTEGER REFERENCES gold.dim_rate_code(rate_code_id),
    -- Measures
    passenger_count          INTEGER,
    trip_distance            DOUBLE PRECISION,
    trip_duration_minutes    DOUBLE PRECISION,
    fare_amount              DOUBLE PRECISION,
    tip_amount               DOUBLE PRECISION,
    tolls_amount             DOUBLE PRECISION,
    total_amount             DOUBLE PRECISION,
    congestion_surcharge     DOUBLE PRECISION,
    airport_fee              DOUBLE PRECISION
);

COMMENT ON TABLE gold.fact_trips IS 'Fact table — one row per taxi trip with measures and dimension keys';

CREATE INDEX IF NOT EXISTS idx_fact_datetime
    ON gold.fact_trips (datetime_id);
CREATE INDEX IF NOT EXISTS idx_fact_pickup
    ON gold.fact_trips (pickup_location_id);
CREATE INDEX IF NOT EXISTS idx_fact_dropoff
    ON gold.fact_trips (dropoff_location_id);

-- ============================================================
-- Gold Layer — Aggregation Tables
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.agg_hourly_zone_revenue (
    date                     DATE NOT NULL,
    hour                     INTEGER NOT NULL,
    pickup_location_id       INTEGER NOT NULL,
    total_trips              INTEGER,
    total_revenue            DOUBLE PRECISION,
    avg_fare                 DOUBLE PRECISION,
    avg_trip_distance        DOUBLE PRECISION,
    avg_trip_duration        DOUBLE PRECISION,
    PRIMARY KEY (date, hour, pickup_location_id)
);

COMMENT ON TABLE gold.agg_hourly_zone_revenue IS 'Hourly revenue aggregation by pickup zone';

CREATE TABLE IF NOT EXISTS gold.agg_daily_summary (
    date                     DATE PRIMARY KEY,
    total_trips              INTEGER,
    total_revenue            DOUBLE PRECISION,
    avg_fare                 DOUBLE PRECISION,
    avg_distance             DOUBLE PRECISION,
    avg_duration             DOUBLE PRECISION,
    total_passengers         INTEGER,
    credit_card_pct          DOUBLE PRECISION,
    cash_pct                 DOUBLE PRECISION
);

COMMENT ON TABLE gold.agg_daily_summary IS 'Daily summary statistics';

-- ============================================================
-- Anomaly & Quality Logging Tables
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.anomaly_log (
    anomaly_id               SERIAL PRIMARY KEY,
    detected_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    anomaly_type             VARCHAR(50) NOT NULL,
    severity                 VARCHAR(20) NOT NULL,
    zone_id                  INTEGER,
    metric_name              VARCHAR(50),
    expected_value           DOUBLE PRECISION,
    actual_value             DOUBLE PRECISION,
    z_score                  DOUBLE PRECISION,
    description              TEXT,
    alert_sent               BOOLEAN DEFAULT FALSE
);

COMMENT ON TABLE gold.anomaly_log IS 'Log of detected pricing and volume anomalies';

CREATE TABLE IF NOT EXISTS gold.quality_log (
    log_id                   SERIAL PRIMARY KEY,
    check_timestamp          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file              VARCHAR(255),
    total_records            INTEGER,
    passed_records           INTEGER,
    failed_records           INTEGER,
    pass_rate                DOUBLE PRECISION,
    check_details            JSONB
);

COMMENT ON TABLE gold.quality_log IS 'Data quality validation results log';

-- ============================================================
-- Done!
-- ============================================================

DO $$
BEGIN
    RAISE NOTICE '✅ TaxiPulse database initialized successfully!';
    RAISE NOTICE '   Schemas: bronze, silver, gold';
    RAISE NOTICE '   All tables created.';
END $$;