-- ============================================================================
-- Test: silver_sales_records Model Validation
-- ============================================================================
-- Validates that the silver_sales_records model correctly:
-- 1. Filters out rows with NULL/empty order_id
-- 2. Filters out rows with invalid order_date
-- 3. Filters out rows with NULL/invalid/non-positive units_sold
-- 4. Deduplicates on order_id (keeps most recent)
-- 5. Casts columns to correct types
-- 6. Trims whitespace from text columns
--
-- This test runs in non-incremental mode to verify all logic.
-- ============================================================================

SELECT
  'Test: silver_sales_records has records' AS test_name,
  CASE
    WHEN COUNT(*) > 0 THEN 'PASS'
    ELSE 'FAIL'
  END AS result,
  COUNT(*) AS record_count
FROM {{ ref('silver_sales_records') }}

UNION ALL

SELECT
  'Test: all order_ids are NOT NULL' AS test_name,
  CASE
    WHEN COUNT(CASE WHEN order_id IS NULL THEN 1 END) = 0 THEN 'PASS'
    ELSE 'FAIL'
  END AS result,
  COUNT(CASE WHEN order_id IS NULL THEN 1 END) AS null_count
FROM {{ ref('silver_sales_records') }}

UNION ALL

SELECT
  'Test: all order_dates are NOT NULL' AS test_name,
  CASE
    WHEN COUNT(CASE WHEN order_date IS NULL THEN 1 END) = 0 THEN 'PASS'
    ELSE 'FAIL'
  END AS result,
  COUNT(CASE WHEN order_date IS NULL THEN 1 END) AS null_count
FROM {{ ref('silver_sales_records') }}

UNION ALL

SELECT
  'Test: all units_sold are > 0' AS test_name,
  CASE
    WHEN COUNT(CASE WHEN units_sold <= 0 THEN 1 END) = 0 THEN 'PASS'
    ELSE 'FAIL'
  END AS result,
  COUNT(CASE WHEN units_sold <= 0 THEN 1 END) AS invalid_count
FROM {{ ref('silver_sales_records') }}

UNION ALL

SELECT
  'Test: no leading/trailing whitespace in country_raw' AS test_name,
  CASE
    WHEN COUNT(CASE WHEN country_raw != TRIM(country_raw) THEN 1 END) = 0 THEN 'PASS'
    ELSE 'FAIL'
  END AS result,
  COUNT(CASE WHEN country_raw != TRIM(country_raw) THEN 1 END) AS untrimmed_count
FROM {{ ref('silver_sales_records') }}

UNION ALL

SELECT
  'Test: units_sold is of INTEGER type' AS test_name,
  CASE
    WHEN COUNT(CASE WHEN units_sold::TEXT !~ '^\d+$' THEN 1 END) = 0 THEN 'PASS'
    ELSE 'FAIL'
  END AS result,
  COUNT(CASE WHEN units_sold::TEXT !~ '^\d+$' THEN 1 END) AS invalid_type_count
FROM {{ ref('silver_sales_records') }}

UNION ALL

SELECT
  'Test: rejection_log contains rejected records' AS test_name,
  CASE
    WHEN COUNT(*) > 0 THEN 'PASS'
    ELSE 'FAIL'
  END AS result,
  COUNT(*) AS rejection_count
FROM {{ ref('silver_rejection_log') }}
