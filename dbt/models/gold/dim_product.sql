{{ config(materialized='table') }}

SELECT
    {{ generate_surrogate_key(['i_id']) }} AS product_key,
    i_type AS item_type
FROM {{ ref('product') }}
