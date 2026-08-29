{{ config(materialized='table') }}

SELECT
    {{ generate_surrogate_key(['c_id']) }} AS category_key,
    c_id,
    c_name
FROM {{ ref('category') }}
