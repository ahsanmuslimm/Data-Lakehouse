{{
    config(
        materialized='table',
        schema='silver'
    )
}}

-- ============================================================================
-- Silver Layer Model: silver_rejection_log
-- ============================================================================
-- Purpose:
--   Capture and log all rejected records that failed validation in the Silver layer.
--   Provides audit trail and diagnostic information for data quality monitoring.
--
-- Rejection Reasons:
--   - NULL or empty order_id
--   - Invalid order_date format (not parseable to DATE)
--   - NULL or invalid units_sold (not parseable to INTEGER)
--   - units_sold <= 0 (must be positive)
--
-- Requirements: 3.4, 3.7
-- ============================================================================

WITH source_data AS (
  SELECT
    _source_file,
    _ingested_at,
    _row_number,
    raw_line,
    TRIM(order_id_raw) AS order_id_trimmed,
    TRIM(order_date_raw) AS order_date_trimmed,
    TRIM(units_sold_raw) AS units_sold_trimmed
  FROM {{ ref('stg_bronze_sales') }}
),

-- Identify rows with validation failures
invalid_rows AS (
  SELECT
    _source_file,
    _row_number,
    raw_line,
    CASE
      WHEN COALESCE(NULLIF(order_id_trimmed, ''), NULL) IS NULL
        THEN 'NULL or empty order_id'
      WHEN TRY_CAST(order_date_trimmed AS DATE) IS NULL
        THEN 'Invalid order_date format (expected DATE)'
      WHEN TRY_CAST(units_sold_trimmed AS INTEGER) IS NULL
        THEN 'Invalid units_sold format (not parseable to INTEGER)'
      WHEN TRY_CAST(units_sold_trimmed AS INTEGER) IS NOT NULL 
        AND TRY_CAST(units_sold_trimmed AS INTEGER) <= 0
        THEN 'units_sold must be greater than 0'
    END AS rejection_reason
  FROM source_data
  WHERE
    -- At least one validation failure must be true
    COALESCE(NULLIF(order_id_trimmed, ''), NULL) IS NULL
    OR TRY_CAST(order_date_trimmed AS DATE) IS NULL
    OR TRY_CAST(units_sold_trimmed AS INTEGER) IS NULL
    OR (TRY_CAST(units_sold_trimmed AS INTEGER) IS NOT NULL 
      AND TRY_CAST(units_sold_trimmed AS INTEGER) <= 0)
)

SELECT
  _source_file AS source_file,
  _row_number AS row_number,
  raw_line AS raw_data,
  rejection_reason,
  CURRENT_TIMESTAMP AS rejected_at
FROM invalid_rows
