-- ============================================================================
-- Macro: reject_to_error_table
-- ============================================================================
-- Purpose:
--   Helper macro to route rejected rows to silver.rejection_log table.
--   Handles rows with NULL or invalid required fields (order_id, order_date, units_sold).
--
-- Parameters:
--   - rejected_rows: CTE containing raw rejected row data
--   - rejection_reason: String reason for rejection
--
-- Usage:
--   {% set rejected = get_rejected_rows() %}
--   {{ reject_to_error_table(rejected, 'NULL order_id') }}
--
-- ============================================================================

{% macro reject_to_error_table(source_file, row_number, raw_data, rejection_reason) %}
  INSERT INTO {{ source('silver', 'rejection_log') }}
    (source_file, row_number, raw_data, rejection_reason)
  SELECT
    '{{ source_file }}'::TEXT as source_file,
    {{ row_number }}::BIGINT as row_number,
    '{{ raw_data }}'::TEXT as raw_data,
    '{{ rejection_reason }}'::TEXT as rejection_reason
  {% if not execute %}
    WHERE FALSE  -- during parsing, don't execute
  {% endif %}
{% endmacro %}
