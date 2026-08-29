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

WITH source_data AS (
  SELECT
    _source_file,
    _ingested_at,
    _row_number,
    raw_line,
    TRIM(country_raw) AS country_trimmed,
    TRIM(region_raw) AS region_trimmed,
    TRIM(item_type_raw) AS item_type_trimmed,
    TRIM(sales_channel_raw) AS sales_channel_trimmed,
    TRIM(order_date_raw) AS order_date_trimmed,
    TRIM(order_id_raw) AS order_id_trimmed,
    TRIM(ship_date_raw) AS ship_date_trimmed,
    TRIM(units_sold_raw) AS units_sold_trimmed,
    TRIM(unit_price_raw) AS unit_price_trimmed,
    TRIM(unit_cost_raw) AS unit_cost_trimmed,
    TRIM(branch_raw) AS branch_trimmed,
    TRIM(category_raw) AS category_trimmed
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
    TRY_CAST(order_date_trimmed AS DATE) AS order_date,
    TRY_CAST(ship_date_trimmed AS DATE) AS ship_date,
    TRY_CAST(units_sold_trimmed AS INTEGER) AS units_sold,
    TRY_CAST(unit_price_trimmed AS NUMERIC(10, 2)) AS unit_price,
    TRY_CAST(unit_cost_trimmed AS NUMERIC(10, 2)) AS unit_cost,
    branch_trimmed,
    category_trimmed
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
    unit_cost,
    branch_trimmed,
    category_trimmed
  FROM typed_data
  WHERE
    COALESCE(NULLIF(order_id_trimmed, ''), NULL) IS NOT NULL
    AND order_date IS NOT NULL
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
    branch_trimmed,
    category_trimmed,
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
  branch_trimmed AS branch_raw,
  category_trimmed AS category_raw,
  units_sold,
  unit_price,
  unit_cost,
  _source_file,
  CURRENT_TIMESTAMP AS _loaded_at
FROM deduplicated_data
WHERE rn = 1

{% if is_incremental() %}
  AND order_date > (
    SELECT watermark_ts FROM audit.watermarks WHERE source_name = '{{ var("watermark_source") }}'
  )
{% endif %}
