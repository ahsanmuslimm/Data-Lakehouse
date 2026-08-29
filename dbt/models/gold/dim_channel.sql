{{ config(materialized='table') }}

SELECT
    {{ generate_surrogate_key(['s_id']) }} AS channel_key,
    s_channel AS sales_channel
FROM {{ ref('channel') }}
