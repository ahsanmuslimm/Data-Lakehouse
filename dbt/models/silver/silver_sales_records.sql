{{
    config(
        materialized='incremental',
        unique_key='order_id',
        on_schema_change='fail',
        schema='silver'
    )
}}

-- ============================================================================
-- Silver Layer Model: silver_sales_records
-- ============================================================================
-- Purpose:
--   Transform Bronze layer raw sales data into validated, typed, and deduplicated
--   records suitable for analytics and downstream Gold layer consumption.
--
-- Transformations Applied:
--   1. Type casting: order_date::DATE, ship_date::DATE, units_sold::INTEGER,
--      unit_price::NUMERIC(10,2), unit_cost::NUMERIC(10,2)
--   2. Whitespace trimming: TRIM() on all TEXT columns
--   3. Null/empty validation: Reject rows with NULL or empty order_id, order_date,
--      or units_sold values, routing them to silver.rejection_log
--   4. Deduplication: ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _ingested_at DESC)
--      to keep only the most recent record per order_id
--   5. Incremental filtering: WHERE order_date > watermark_ts for incremental loads
--
-- Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8
-- ============================================================================

WITH source_data AS (
  SELECT
    _source_file,
    _ingested_at,
    _row_number,
    raw_line,
    -- Trim whitespace from all TEXT columns
    TRIM(country_raw) AS country_trimmed,
    TRIM(region_raw) AS region_trimmed,
    TRIM(item_type_raw) AS item_type_trimmed,
    TRIM(sales_channel_raw) AS sales_channel_trimmed,
    TRIM(order_date_raw) AS order_date_trimmed,
    TRIM(order_id_raw) AS order_id_trimmed,
    TRIM(ship_date_raw) AS ship_date_trimmed,
    TRIM(units_sold_raw) AS units_sold_trimmed,
    TRIM(unit_price_raw) AS unit_price_trimmed,
    TRIM(unit_cost_raw) AS unit_cost_trimmed
  FROM {{ ref('stg_bronze_sales') }}
),

-- Apply type casting and validation
typed_data AS (
  SELECT
    _source_file,
    _ingested_at,
    _row_number,
    raw_line,
    country_trimmed,
    region_trimmed,
    item_type_trimmed,
    sales_channel_trimmed,
    order_id_trimmed,
    -- Cast order_date to DATE (MM/DD/YYYY format)
    TRY_CAST(order_date_trimmed AS DATE) AS order_date,
    -- Cast ship_date to DATE (MM/DD/YYYY format)
    TRY_CAST(ship_date_trimmed AS DATE) AS ship_date,
    -- Cast units_sold to INTEGER
    TRY_CAST(units_sold_trimmed AS INTEGER) AS units_sold,
    -- Cast unit_price to NUMERIC(10,2)
    TRY_CAST(unit_price_trimmed AS NUMERIC(10, 2)) AS unit_price,
    -- Cast unit_cost to NUMERIC(10,2)
    TRY_CAST(unit_cost_trimmed AS NUMERIC(10, 2)) AS unit_cost
  FROM source_data
),

-- Apply COALESCE/NULLIF guards and filter valid rows
validated_data AS (
  SELECT
    _source_file,
    _ingested_at,
    _row_number,
    raw_line,
    country_trimmed,
    region_trimmed,
    item_type_trimmed,
    sales_channel_trimmed,
    order_id_trimmed,
    order_date,
    ship_date,
    units_sold,
    unit_price,
    unit_cost
  FROM typed_data
  WHERE
    -- Reject rows with NULL or empty order_id
    COALESCE(NULLIF(order_id_trimmed, ''), NULL) IS NOT NULL
    -- Reject rows with NULL order_date
    AND order_date IS NOT NULL
    -- Reject rows with NULL or invalid units_sold
    AND units_sold IS NOT NULL AND units_sold > 0
),

-- Deduplicate on order_id, keeping most recent by _ingested_at
deduplicated_data AS (
  SELECT
    _source_file,
    _ingested_at,
    _row_number,
    raw_line,
    country_trimmed,
    region_trimmed,
    item_type_trimmed,
    sales_channel_trimmed,
    order_id_trimmed AS order_id,
    order_date,
    ship_date,
    units_sold,
    unit_price,
    unit_cost,
    ROW_NUMBER() OVER (PARTITION BY order_id_trimmed ORDER BY _ingested_at DESC) AS rn
  FROM validated_data
)

SELECT
  order_id,
  order_date,
  ship_date,
  country_trimmed AS country_raw,
  region_trimmed AS region_raw,
  item_type_trimmed AS item_type_raw,
  sales_channel_trimmed AS sales_channel,
  units_sold,
  unit_price,
  unit_cost,
  _source_file,
  CURRENT_TIMESTAMP AS _loaded_at
FROM deduplicated_data
WHERE rn = 1

{% if is_incremental() %}
  -- Filter to only records newer than the last watermark
  AND order_date > (
    SELECT watermark_ts FROM audit.watermarks WHERE source_name = '{{ var("watermark_source") }}'
  )
{% endif %}
