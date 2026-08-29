{{
    config(
        materialized='table',
        schema='gold'
    )
}}

-- ============================================================================
-- Gold Layer Model: fact_sales
-- ============================================================================
-- Joins Silver sales records to all dimensions.
-- Resolves surrogate keys by looking up natural keys in dimension tables.
-- Falls back to the Unknown sentinel (-1) if no match is found.

WITH silver_sales AS (
    SELECT * FROM {{ ref('silver_sales_records') }}
),

dim_country AS (
    SELECT * FROM {{ ref('dim_country') }}
),

dim_region AS (
    SELECT * FROM {{ ref('dim_region') }}
),

dim_branch AS (
    SELECT * FROM {{ ref('dim_branch') }}
),

dim_product AS (
    SELECT * FROM {{ ref('dim_product') }}
),

dim_category AS (
    SELECT * FROM {{ ref('dim_category') }}
),

dim_channel AS (
    SELECT * FROM {{ ref('dim_channel') }}
)

SELECT
    s.order_id,
    s.order_date,
    s.ship_date,
    
    COALESCE(c.country_key, '-1') AS country_key,
    COALESCE(r.region_key, '-1') AS region_key,
    COALESCE(b.branch_key, '-1') AS branch_key,
    COALESCE(p.product_key, '-1') AS product_key,
    COALESCE(cat.category_key, '-1') AS category_key,
    COALESCE(ch.channel_key, '-1') AS channel_key,
    
    s.units_sold,
    s.unit_price,
    s.unit_cost,
    
    s._loaded_at
FROM silver_sales s
LEFT JOIN dim_country c ON s.country_raw = c.country_name
LEFT JOIN dim_region r ON s.region_raw = r.region_name
LEFT JOIN dim_branch b ON s.branch_raw = b.b_name
LEFT JOIN dim_product p ON s.item_type_raw = p.item_type
LEFT JOIN dim_category cat ON s.category_raw = cat.c_name
LEFT JOIN dim_channel ch ON s.sales_channel = ch.sales_channel
