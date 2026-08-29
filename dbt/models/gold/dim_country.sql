{{ config(materialized='table') }}

SELECT
    {{ generate_surrogate_key(['c_id']) }} AS country_key,
    c_id,
    "Country" AS country_name
FROM {{ ref('country') }}
