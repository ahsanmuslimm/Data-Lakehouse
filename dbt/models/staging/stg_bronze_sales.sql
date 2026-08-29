{{
  config(
    materialized='view',
    schema='staging'
  )
}}

-- ============================================================================
-- Staging Model: stg_bronze_sales
-- ============================================================================
-- Purpose:
--   Parse raw CSV lines from bronze.raw_sales_records into individual columns
--   No filtering or transformation at this stage - expose raw values as-is
--   Serves as the intermediate layer between raw Bronze data and Silver transformations
--
-- Requirements: 3.1
-- ============================================================================

WITH source_data AS (
  SELECT
    _source_file,
    _ingested_at,
    _row_number,
    raw_line
  FROM {{ source('bronze', 'raw_sales_records') }}
)

SELECT
  _source_file,
  _ingested_at,
  _row_number,
  raw_line,
  -- Parse CSV columns using split_part
  -- Column order: Region,Country,Item Type,Sales Channel,Order Date,Order ID,Ship Date,Units Sold,Unit Price,Unit Cost
  split_part(raw_line, ',', 1)::TEXT AS region_raw,
  split_part(raw_line, ',', 2)::TEXT AS country_raw,
  split_part(raw_line, ',', 3)::TEXT AS item_type_raw,
  split_part(raw_line, ',', 4)::TEXT AS sales_channel_raw,
  split_part(raw_line, ',', 5)::TEXT AS order_date_raw,
  split_part(raw_line, ',', 6)::TEXT AS order_id_raw,
  split_part(raw_line, ',', 7)::TEXT AS ship_date_raw,
  split_part(raw_line, ',', 8)::TEXT AS units_sold_raw,
  split_part(raw_line, ',', 9)::TEXT AS unit_price_raw,
  split_part(raw_line, ',', 10)::TEXT AS unit_cost_raw,
  split_part(raw_line, ',', 11)::TEXT AS branch_raw,
  split_part(raw_line, ',', 12)::TEXT AS category_raw
FROM source_data
