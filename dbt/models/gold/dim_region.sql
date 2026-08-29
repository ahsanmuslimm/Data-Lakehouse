{{ config(materialized='table') }}

SELECT
    {{ generate_surrogate_key(['r_id']) }} AS region_key,
    r_id AS region_id,
    "Region" AS region_name
FROM {{ ref('region') }}
