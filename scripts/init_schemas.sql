-- ============================================================================
-- Retail Sales Lakehouse Migration - Schema Initialization Script
-- ============================================================================
-- This script creates all schemas and tables for the medallion architecture:
-- - audit: metadata tracking (file ingestion log, watermarks)
-- - bronze: raw immutable data landing zone
-- - silver: cleaned, typed, validated data
-- - gold: star schema (fact and dimension tables)
-- - observability: pipeline monitoring and metrics
--
-- Requirements: 8.8, 1.7, 2.1, 3.8, 4.1, 4.4–4.9, 4.12, 4.13, 11.1–11.4
-- ============================================================================

-- ============================================================================
-- SCHEMA CREATION
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS audit;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS observability;

-- ============================================================================
-- AUDIT SCHEMA TABLES
-- ============================================================================

-- File ingestion audit log
-- Tracks all files ingested from source to bronze layer with checksums
CREATE TABLE IF NOT EXISTS audit.file_ingestion_log (
    id              SERIAL PRIMARY KEY,
    source_filename TEXT NOT NULL,
    bronze_path     TEXT NOT NULL,
    file_size_bytes BIGINT,
    checksum_sha256 TEXT,
    ingested_at     TIMESTAMPTZ DEFAULT now(),
    status          TEXT NOT NULL CHECK (status IN ('success', 'failed'))
);

-- Watermark tracking for incremental loading
-- Maintains high-water-mark timestamps for each data source
CREATE TABLE IF NOT EXISTS audit.watermarks (
    source_name     TEXT PRIMARY KEY,
    watermark_ts    TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01T00:00:00Z',
    updated_at      TIMESTAMPTZ DEFAULT now()
);

-- ============================================================================
-- BRONZE SCHEMA TABLES
-- ============================================================================

-- Raw sales records staging table
-- Stores unparsed CSV rows for audit and reprocessing capability
CREATE TABLE IF NOT EXISTS bronze.raw_sales_records (
    _source_file    TEXT,
    _ingested_at    TIMESTAMPTZ DEFAULT now(),
    _row_number     BIGINT,
    raw_line        TEXT
);

-- ============================================================================
-- SILVER SCHEMA TABLES
-- ============================================================================

-- Cleaned and validated sales records
-- Type-cast, deduplicated, whitespace-trimmed data ready for analytics
CREATE TABLE IF NOT EXISTS silver.sales_records (
    order_id        TEXT PRIMARY KEY,
    order_date      DATE NOT NULL,
    ship_date       DATE,
    country_raw     TEXT,
    region_raw      TEXT,
    branch_raw      TEXT,
    item_type_raw   TEXT,
    sales_channel   TEXT,
    units_sold      INTEGER NOT NULL CHECK (units_sold > 0),
    unit_price      NUMERIC(10,2) NOT NULL CHECK (unit_price >= 0),
    unit_cost       NUMERIC(10,2) NOT NULL CHECK (unit_cost >= 0),
    _source_file    TEXT,
    _loaded_at      TIMESTAMPTZ DEFAULT now()
);

-- Silver layer rejection log
-- Records all rows that failed validation with detailed rejection reasons
CREATE TABLE IF NOT EXISTS silver.rejection_log (
    id              SERIAL PRIMARY KEY,
    source_file     TEXT,
    row_number      BIGINT,
    raw_data        TEXT,
    rejection_reason TEXT,
    rejected_at     TIMESTAMPTZ DEFAULT now()
);

-- ============================================================================
-- GOLD SCHEMA DIMENSION TABLES
-- ============================================================================

-- Country dimension
-- Contains country master data with surrogate keys
CREATE TABLE IF NOT EXISTS gold.dim_country (
    country_key     INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    c_id            TEXT UNIQUE NOT NULL,
    country_name    TEXT NOT NULL
);

-- Region dimension
-- Contains region master data with surrogate keys
CREATE TABLE IF NOT EXISTS gold.dim_region (
    region_key      INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    region_id       TEXT UNIQUE NOT NULL,
    region_name     TEXT
);

-- Branch dimension
-- Contains branch master data with surrogate keys
CREATE TABLE IF NOT EXISTS gold.dim_branch (
    branch_key      INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    b_id            TEXT UNIQUE NOT NULL,
    b_name          TEXT
);

-- Product dimension
-- Contains product/item type master data with surrogate keys
CREATE TABLE IF NOT EXISTS gold.dim_product (
    product_key     INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    item_type       TEXT UNIQUE NOT NULL
);

-- Category dimension
-- Contains category master data with surrogate keys
CREATE TABLE IF NOT EXISTS gold.dim_category (
    category_key    INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    c_id            TEXT UNIQUE NOT NULL,
    c_name          TEXT NOT NULL
);

-- Channel dimension
-- Contains sales channel master data with surrogate keys
CREATE TABLE IF NOT EXISTS gold.dim_channel (
    channel_key     INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sales_channel   TEXT UNIQUE NOT NULL
);

-- ============================================================================
-- GOLD SCHEMA FACT TABLE
-- ============================================================================

-- Sales fact table
-- Star schema fact table with foreign keys to all dimensions
-- Revenue and cost metrics are computed and stored via generated columns
CREATE TABLE IF NOT EXISTS gold.fact_sales (
    fact_key        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id        TEXT NOT NULL,
    order_date      DATE NOT NULL,
    ship_date       DATE,
    country_key     INTEGER NOT NULL REFERENCES gold.dim_country(country_key),
    region_key      INTEGER NOT NULL REFERENCES gold.dim_region(region_key),
    branch_key      INTEGER NOT NULL REFERENCES gold.dim_branch(branch_key),
    product_key     INTEGER NOT NULL REFERENCES gold.dim_product(product_key),
    category_key    INTEGER NOT NULL REFERENCES gold.dim_category(category_key),
    channel_key     INTEGER NOT NULL REFERENCES gold.dim_channel(channel_key),
    units_sold      INTEGER NOT NULL,
    unit_price      NUMERIC(10,2) NOT NULL,
    unit_cost       NUMERIC(10,2) NOT NULL,
    total_revenue   NUMERIC(14,2) GENERATED ALWAYS AS (units_sold * unit_price) STORED,
    total_cost      NUMERIC(14,2) GENERATED ALWAYS AS (units_sold * unit_cost) STORED,
    _loaded_at      TIMESTAMPTZ DEFAULT now()
);

-- ============================================================================
-- GOLD SCHEMA INDEXES
-- ============================================================================

-- Fact table indexes on foreign keys for efficient joins
CREATE INDEX IF NOT EXISTS idx_fact_sales_order_date   ON gold.fact_sales(order_date);
CREATE INDEX IF NOT EXISTS idx_fact_sales_country_key  ON gold.fact_sales(country_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_region_key   ON gold.fact_sales(region_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_branch_key   ON gold.fact_sales(branch_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product_key  ON gold.fact_sales(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_category_key ON gold.fact_sales(category_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_channel_key  ON gold.fact_sales(channel_key);

-- ============================================================================
-- OBSERVABILITY SCHEMA TABLES
-- ============================================================================

-- Pipeline execution tracking
-- Logs all pipeline task runs with execution metadata and status
CREATE TABLE IF NOT EXISTS observability.pipeline_runs (
    run_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dag_run_id      TEXT,
    task_id         TEXT,
    status          TEXT,
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    duration_secs   NUMERIC,
    error_message   TEXT,
    records_processed BIGINT
);

-- Layer row count snapshots
-- Tracks row counts per layer/table for monitoring data volume trends
CREATE TABLE IF NOT EXISTS observability.layer_row_counts (
    snapshot_at     TIMESTAMPTZ DEFAULT now(),
    layer           TEXT,
    table_name      TEXT,
    row_count       BIGINT
);

-- Data quality test results
-- Stores historical dbt test results for quality trend analysis
CREATE TABLE IF NOT EXISTS observability.dq_results (
    snapshot_at     TIMESTAMPTZ DEFAULT now(),
    test_name       TEXT,
    status          TEXT,
    failure_count   BIGINT,
    details         JSONB
);

-- Data freshness monitoring
-- Tracks lag between source data arrival and gold layer availability
CREATE TABLE IF NOT EXISTS observability.freshness_metrics (
    measured_at             TIMESTAMPTZ DEFAULT now(),
    source_name             TEXT,
    last_source_file_ts     TIMESTAMPTZ,
    gold_layer_updated_at   TIMESTAMPTZ,
    freshness_lag_hours     NUMERIC
);

-- ============================================================================
-- INITIALIZATION COMPLETE
-- ============================================================================
