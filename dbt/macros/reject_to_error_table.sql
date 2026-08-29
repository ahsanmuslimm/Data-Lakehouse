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
    {% set insert_sql %}
        INSERT INTO silver.rejection_log (
            source_file,
            row_number,
            raw_data,
            rejection_reason,
            rejected_at
        ) VALUES (
            '{{ source_file }}',
            {{ row_number }},
            '{{ raw_data | replace("'", "''") }}',
            '{{ rejection_reason }}',
            now()
        );
    {% endset %}
    
    {% if execute %}
        {% do run_query(insert_sql) %}
    {% endif %}
{% endmacro %}
