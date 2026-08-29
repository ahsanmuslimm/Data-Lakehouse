{{ config(materialized='table') }}

SELECT
    {{ generate_surrogate_key(['b_id']) }} AS branch_key,
    b_id,
    b_name
FROM {{ ref('branch') }}
