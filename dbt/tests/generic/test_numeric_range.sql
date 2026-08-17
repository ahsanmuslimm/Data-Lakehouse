-- ============================================================================
-- Generic Test: numeric_range
-- ============================================================================
-- Validates that a numeric column falls within a specified range.
--
-- Parameters:
--   - column_name: the column to validate
--   - min_value: minimum allowed value (inclusive)
--   - max_value: maximum allowed value (inclusive)
--
-- Example:
--   - name: units_sold
--     data_tests:
--       - numeric_range:
--           min_value: 1
--           max_value: 10000
-- ============================================================================

{% test numeric_range(model, column_name, min_value, max_value) %}
  SELECT *
  FROM {{ model }}
  WHERE {{ column_name }} < {{ min_value }} OR {{ column_name }} > {{ max_value }}
{% endtest %}
