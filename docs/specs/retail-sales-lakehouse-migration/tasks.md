# Implementation Plan: Retail Sales Lakehouse Migration

## Overview

Implement a medallion-architecture data lakehouse (Bronze → Silver → Gold) on top of a fully
containerized local stack (PostgreSQL, MinIO, Airflow, Metabase) with dbt transformations,
Hypothesis property-based tests, GitHub Actions CI/CD, and Terraform IaC.  Tasks are ordered
so every step produces executable, integrated code — nothing is left orphaned.

---

## Tasks

- [x] 1. Repository scaffolding and local dev environment
  - [x] 1.1 Create top-level directory structure
    - Create folders: `airflow/dags/`, `airflow/plugins/operators/`, `airflow/plugins/hooks/`, `dbt/`, `scripts/`, `tests/properties/`, `tests/unit/`, `tests/integration/`, `terraform/modules/`, `.github/workflows/`
    - Add root `.env.example` with all required env-var keys (DB creds, MinIO creds, `ALERT_WEBHOOK_URL`, Airflow Fernet key)
    - Add `requirements.txt` / `pyproject.toml` pinning: `apache-airflow==2.9.*`, `dbt-core`, `dbt-postgres`, `hypothesis`, `pytest`, `sqlfluff`, `boto3`, `minio`
    - _Requirements: 8.1_

  - [x] 1.2 Write `docker-compose.yml`
    - Define services: `postgres:15`, `minio/minio:latest` (ports 9000/9001), `airflow-webserver` (port 8080), `airflow-scheduler`, `airflow-worker`, `airflow-init` (one-shot DB init), `schema-init` (one-shot DDL + seed), `metabase/metabase:latest` (port 3000)
    - Mount `./dbt:/opt/airflow/dbt` and `./airflow/dags:/opt/airflow/dags`
    - Define named volumes `postgres_data` and `minio_data`
    - Add `healthcheck` entries so dependent services wait for Postgres and MinIO to be ready
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7, 8.10_

- [x] 2. Database schema initialization
  - [x] 2.1 Write `scripts/init_schemas.sql`
    - `CREATE SCHEMA IF NOT EXISTS` for: `audit`, `bronze`, `silver`, `gold`, `observability`
    - Create `audit.file_ingestion_log` with columns: `id SERIAL PK`, `source_filename TEXT`, `bronze_path TEXT`, `file_size_bytes BIGINT`, `checksum_sha256 TEXT`, `ingested_at TIMESTAMPTZ`, `status TEXT CHECK (status IN ('success','failed'))`
    - Create `audit.watermarks` with columns: `source_name TEXT PK`, `watermark_ts TIMESTAMPTZ DEFAULT '1970-01-01T00:00:00Z'`, `updated_at TIMESTAMPTZ`
    - Create `bronze.raw_sales_records` with columns: `_source_file TEXT`, `_ingested_at TIMESTAMPTZ`, `_row_number BIGINT`, `raw_line TEXT`
    - Create `silver.sales_records` with columns per data model (order_id PK, dates, raw dimension columns, numeric columns with CHECK constraints, audit columns)
    - Create `silver.rejection_log` with columns: `id SERIAL PK`, `source_file TEXT`, `row_number BIGINT`, `raw_data TEXT`, `rejection_reason TEXT`, `rejected_at TIMESTAMPTZ`
    - Create all six `gold.dim_*` tables and `gold.fact_sales` with FK references and all indexes listed in the data model section
    - Create all four `observability.*` tables: `pipeline_runs`, `layer_row_counts`, `dq_results`, `freshness_metrics`
    - _Requirements: 8.8, 1.7, 2.1, 3.8, 4.1, 4.4–4.9, 4.12, 4.13, 11.1–11.4_

  - [x] 2.2 Wire `schema-init` Docker service to run `init_schemas.sql` on startup
    - Write `scripts/init_container.sh` that waits for Postgres, runs `psql < init_schemas.sql`, then exits 0
    - Reference it as the `command` for the `schema-init` service in `docker-compose.yml`
    - _Requirements: 8.8_

- [x] 3. dbt project setup
  - [x] 3.1 Initialise dbt project and configure profiles
    - Run `dbt init retail_sales` inside `dbt/`, configure `dbt_project.yml` (project name, model paths, seed paths, test paths, macro paths)
    - Write `dbt/profiles.yml` using env-var references for host, port, dbname, user, password; define `dev`, `ci`, and `prod` targets
    - _Requirements: 3.8, 4.1_

  - [x] 3.2 Add dimension seed CSV files and `Unknown` sentinel rows
    - Copy `B1/*.csv` dimension files into `dbt/seeds/` (branch, category, channel, country, product, region)
    - Add one row per seed file with the natural-key value `'Unknown'` and surrogate key hint `-1` so the Unknown sentinel is seeded alongside real data
    - Verify seeds load via `dbt seed --target dev`
    - _Requirements: 4.11, 8.9_

  - [x] 3.3 Write staging model `dbt/models/staging/stg_bronze_sales.sql`
    - Select all columns from `bronze.raw_sales_records`, parse `raw_line` CSV columns into named fields using `split_part` or a dbt macro
    - No filtering at this stage — expose raw column values with `_source_file` and `_ingested_at`
    - _Requirements: 3.1_

- [ ] 4. Source Simulator script
  - [-] 4.1 Implement `scripts/simulate_feed.py`
    - Parse CLI args `--date YYYY-MM-DD` (default: today) and `--records N` (default: 150)
    - Read each dimension seed CSV to build valid FK value lists for Country, Region, Branch, Item_Type, Category, Sales_Channel
    - Maintain a `counter.json` file to persist the last used Order_ID; assign sequential IDs across runs
    - Generate N records with: `Order_Date = --date`, random FK selections from dimension lists, `Units_Sold` in [100, 10000], `Unit_Price` in [5.00, 1000.00]
    - Write output to an in-memory CSV buffer and upload to MinIO `source/sales_{YYYY-MM-DD}.csv` via `boto3`
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7, 7.8, 7.9, 7.10_

  - [ ] 4.2 Write property test — Property 1: Source Simulator Sequential Order ID Uniqueness
    - **Property 1: Source Simulator Sequential Order ID Uniqueness**
    - **Validates: Requirements 7.3**
    - Use `@given(st.integers(min_value=1, max_value=5), st.integers(min_value=50, max_value=200))` to generate run counts and record counts
    - Assert all Order_IDs across all runs are unique and form a contiguous ascending sequence
    - Located in `tests/properties/test_simulator_properties.py`

  - [ ] 4.3 Write property test — Property 2: Source Simulator Valid Dimension References
    - **Property 2: Source Simulator Valid Dimension References**
    - **Validates: Requirements 7.5**
    - Load dimension seed CSVs and verify every generated record's FK values are members of the corresponding seed set
    - Located in `tests/properties/test_simulator_properties.py`

  - [ ] 4.4 Write property test — Property 3: Source Simulator Field Range Invariants
    - **Property 3: Source Simulator Field Range Invariants**
    - **Validates: Requirements 7.6, 7.7**
    - Use `@given(st.dates(), st.integers(min_value=50, max_value=200))` to vary date and record count
    - Assert `100 <= units_sold <= 10000` and `5.00 <= unit_price <= 1000.00` for every generated row
    - Located in `tests/properties/test_simulator_properties.py`

- [ ] 5. Bronze Ingest Operator
  - [x] 5.1 Implement `airflow/plugins/operators/bronze_ingest_operator.py`
    - Extend `BaseOperator`; define `execute(context)` method
    - List files in MinIO `source/` bucket uploaded since last DAG logical date
    - For each file: copy to `bronze/sales/{YYYY}/{MM}/{DD}/{filename}_{timestamp}.csv`, compute SHA-256 checksum, write a row to `audit.file_ingestion_log` with `status='success'`
    - Implement exponential backoff retry (30 s, 60 s, 120 s) using `tenacity` or Airflow's built-in retry; on final failure write `status='failed'` and raise
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7_

  - [ ] 5.2 Write unit tests for Bronze Ingest Operator
    - Test Bronze path construction for various input filenames and dates
    - Test SHA-256 checksum computation against a known fixture file
    - Test retry logic: mock MinIO to fail twice then succeed; assert audit log entry is `success`
    - Test final-failure path: mock MinIO to fail all 3 attempts; assert audit log entry is `failed`
    - Located in `tests/unit/test_bronze_operator.py`
    - _Requirements: 1.5, 1.6, 1.7_

- [ ] 6. Watermark Manager hook
  - [-] 6.1 Implement `airflow/plugins/hooks/watermark_hook.py`
    - Extend `BaseHook`; wrap a PostgreSQL connection (use Airflow's `PostgresHook` internally)
    - Implement `get_watermark(source_name) -> datetime`: SELECT watermark_ts, return epoch if row missing
    - Implement `set_watermark(source_name, new_watermark)`: UPSERT row in `audit.watermarks`, set `updated_at = now()`
    - Implement `initialize_if_missing(source_name)`: INSERT with epoch timestamp only if row absent (idempotent)
    - _Requirements: 2.1, 2.4, 2.5_

  - [ ] 6.2 Write unit tests for Watermark Hook
    - Test epoch initialization when `audit.watermarks` row does not exist
    - Test `get_watermark` returns the stored timestamp on subsequent calls
    - Test `set_watermark` advances the timestamp and `updated_at` is updated
    - Test idempotency: calling `initialize_if_missing` twice does not reset a previously advanced watermark
    - Located in `tests/unit/test_watermark_hook.py`
    - _Requirements: 2.1, 2.5_

  - [ ] 6.3 Write property test — Property 4: Watermark Monotonicity
    - **Property 4: Watermark Monotonicity**
    - **Validates: Requirements 2.4**
    - Use a Hypothesis strategy generating ordered sequences of datetime values representing successive max Order_Date values
    - Call `set_watermark` for each value in order; assert stored watermark never decreases between calls
    - Located in `tests/properties/test_watermark_properties.py`

  - [ ] 6.4 Write property test — Property 5: Incremental Filter Correctness
    - **Property 5: Incremental Filter Correctness**
    - **Validates: Requirements 2.3**
    - Generate an arbitrary watermark timestamp W and a list of records with random Order_Date values spanning before and after W
    - Apply the incremental filter predicate (`order_date > W`) and assert output contains exactly the records where `order_date > W`
    - Located in `tests/properties/test_watermark_properties.py`

- [ ] 7. Silver dbt models
  - [x] 7.1 Write `dbt/models/silver/silver_sales_records.sql`
    - Configure as `materialized='incremental'`, `unique_key='order_id'`, `on_schema_change='fail'`
    - In the incremental block, filter `WHERE order_date > (SELECT watermark_ts FROM audit.watermarks WHERE source_name = 'sales')`
    - Cast columns: `order_date::DATE`, `ship_date::DATE`, `units_sold::INTEGER`, `unit_price::NUMERIC(10,2)`, `unit_cost::NUMERIC(10,2)`
    - Trim whitespace on all TEXT columns with `TRIM()`
    - Apply `COALESCE` / `NULLIF` guards; rows with NULL/empty `order_id`, `order_date`, or `units_sold` are directed to `silver.rejection_log` via the `reject_to_error_table` macro and excluded from the main model
    - Deduplicate on `order_id` using `ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _ingested_at DESC) = 1`
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8_

  - [-] 7.2 Write `dbt/macros/reject_to_error_table.sql`
    - Accept arguments: `source_file`, `row_number`, `raw_data`, `rejection_reason`
    - Execute an INSERT into `silver.rejection_log` using dbt `run_query`
    - _Requirements: 3.4, 3.7_

  - [ ] 7.3 Write property test — Property 6: Silver Layer Type Validity
    - **Property 6: Silver Layer Type Validity**
    - **Validates: Requirements 3.2, 3.3**
    - Generate valid Bronze rows using Hypothesis; run them through the casting logic (extracted as a pure Python function or against a test PG schema); assert all output column types match the Silver schema
    - Located in `tests/properties/test_silver_properties.py`

  - [ ] 7.4 Write property test — Property 7: Silver Deduplication
    - **Property 7: Silver Deduplication**
    - **Validates: Requirements 3.5**
    - Generate Bronze datasets containing repeated `order_id` values with varying `_ingested_at` timestamps; apply dedup logic; assert exactly one row per distinct `order_id` survives
    - Located in `tests/properties/test_silver_properties.py`

  - [ ] 7.5 Write property test — Property 8: Silver Whitespace Trimming
    - **Property 8: Silver Whitespace Trimming**
    - **Validates: Requirements 3.6**
    - Use `st.text()` with leading/trailing whitespace strategy for text columns; apply trim function; assert no leading or trailing whitespace in output
    - Located in `tests/properties/test_silver_properties.py`

  - [ ] 7.6 Write property test — Property 9: Silver Rejection on Invalid Required Fields
    - **Property 9: Silver Rejection on Invalid Required Fields**
    - **Validates: Requirements 3.7**
    - Generate rows where `order_id`, `order_date`, or `units_sold` is NULL or empty string; run through Silver transform; assert no such row in `silver.sales_records` and a corresponding `rejection_log` entry exists
    - Located in `tests/properties/test_silver_properties.py`

  - [ ] 7.7 Write property test — Property 12: Date Ordering Constraint
    - **Property 12: Date Ordering Constraint**
    - **Validates: Requirements 5.6**
    - Generate rows where `ship_date < order_date`; verify they are rejected and do not appear in `silver.sales_records`; generate rows where `ship_date >= order_date`; verify they pass
    - Located in `tests/properties/test_silver_properties.py`

- [ ] 8. Gold dbt models
  - [x] 8.1 Write `dbt/macros/generate_surrogate_key.sql`
    - Implement `generate_surrogate_key(column_list)` macro using `dbt_utils.generate_surrogate_key` pattern or `md5(coalesce(...))` fallback
    - Handle the `-1` / `Unknown` case by never hashing a NULL — coerce to `'Unknown'` before hashing
    - _Requirements: 4.4–4.9_

  - [~] 8.2 Write six dimension models (`dbt/models/gold/dim_*.sql`)
    - One model per dimension: `dim_country.sql`, `dim_region.sql`, `dim_branch.sql`, `dim_product.sql`, `dim_category.sql`, `dim_channel.sql`
    - Each model: SELECT from the corresponding seed (via `ref()`), generate surrogate key using the macro, include the `Unknown` sentinel row (natural key = `'Unknown'`, surrogate key = `-1`)
    - Materialize as `table` with `dbt_project.yml` config
    - _Requirements: 4.4, 4.5, 4.6, 4.7, 4.8, 4.9, 4.11_

  - [~] 8.3 Write `dbt/models/gold/fact_sales.sql`
    - JOIN `silver.sales_records` to each dimension using natural keys; resolve surrogate keys; fall back to `-1` for any unmatched natural key using `COALESCE(dim.surrogate_key, -1)`
    - Compute no derived columns (total_revenue and total_cost are `GENERATED ALWAYS AS ... STORED` in DDL)
    - Materialize as `table`; include all FK columns and measure columns
    - _Requirements: 4.1, 4.2, 4.3, 4.10, 4.11, 4.12, 4.13_

  - [ ] 8.4 Write property test — Property 10: Gold Revenue and Cost Arithmetic
    - **Property 10: Gold Revenue and Cost Arithmetic**
    - **Validates: Requirements 4.2, 4.3, 5.7**
    - Use `@given(st.integers(min_value=1, max_value=10000), st.decimals(min_value=Decimal('0.00'), max_value=Decimal('1000.00'), places=2))`
    - Assert `abs(total_revenue - units_sold * unit_price) < 0.01` and likewise for `total_cost`
    - Located in `tests/properties/test_gold_properties.py`

  - [ ] 8.5 Write property test — Property 11: Referential Integrity — No Orphan Facts
    - **Property 11: Referential Integrity — No Orphan Facts**
    - **Validates: Requirements 4.10, 4.11, 5.3**
    - Generate fact rows with random natural keys, some of which are absent from dimension tables; run the gold FK lookup logic; assert every output row has a valid surrogate key (either a real one or `-1`), and no NULL FK exists
    - Located in `tests/properties/test_gold_properties.py`

- [ ] 9. Data Quality dbt tests
  - [~] 9.1 Write generic dbt tests in `dbt/tests/generic/`
    - `not_null_order_id.sql`: assert `silver.sales_records.order_id IS NOT NULL` — _Requirements: 5.2_
    - `unique_order_id.sql`: assert uniqueness of `order_id` in `silver.sales_records` — _Requirements: 5.1_
    - `referential_integrity_fact_dims.sql`: assert all six FK columns in `gold.fact_sales` reference existing rows in their dimension tables — _Requirements: 5.3_
    - `units_sold_positive.sql`: assert `units_sold > 0` — _Requirements: 5.4_
    - `unit_price_non_negative.sql`: assert `unit_price >= 0` — _Requirements: 5.5_

  - [~] 9.2 Write singular dbt tests in `dbt/tests/singular/`
    - `assert_order_date_lte_ship_date.sql`: return rows where `ship_date < order_date`; zero rows = pass — _Requirements: 5.6_
    - `assert_revenue_equals_units_times_price.sql`: return rows where `abs(total_revenue - units_sold * unit_price) >= 0.01`; zero rows = pass — _Requirements: 5.7_
    - `test_watermark_advance.sql`: assert `watermark_ts > '1970-01-01'` after at least one pipeline run — _Requirements: 2.4_

  - [~] 9.3 Add dbt model contracts to Silver and Gold YAML configs
    - In `dbt/models/silver/schema.yml` add `contract: {enforced: true}` with column-level `data_type` and `constraints: [{type: not_null}]` for required fields
    - In `dbt/models/gold/schema.yml` add the same contract enforcement for `fact_sales` and all six `dim_*` models
    - _Requirements: 5.1, 5.2, 5.8_

  - [ ] 9.4 Write property test — Property 13: Data Quality Contract Halt
    - **Property 13: Data Quality Contract Halt**
    - **Validates: Requirements 5.8**
    - Simulate a dbt test failure by injecting a bad row into the test schema; run the DQ task and capture the Airflow task return code; assert downstream tasks (`dbt_run_gold`, `update_watermark`) are in `skipped` or `failed` state in the task instance log
    - Located in `tests/properties/test_dq_properties.py`

- [ ] 10. Airflow DAG
  - [~] 10.1 Write `airflow/dags/retail_sales_pipeline.py`
    - Define DAG with `dag_id='retail_sales_pipeline'`, `schedule_interval='0 2 * * *'`, `catchup=False`, `retries=3`, `retry_delay=timedelta(minutes=5)`, `on_failure_callback=ObservabilityAlertHook().send_alert`
    - Instantiate tasks in order: `ingest_bronze` (BronzeIngestOperator), `read_watermark` (PythonOperator calling `WatermarkHook.get_watermark`), `dbt_run_silver` (BashOperator: `dbt run --select silver.*`), `dbt_test_silver` (BashOperator: `dbt test --select silver.*`), `dbt_run_gold` (BashOperator: `dbt run --select gold.*`), `dbt_test_gold` (BashOperator: `dbt test --select gold.*`), `update_watermark` (PythonOperator calling `WatermarkHook.set_watermark`), `refresh_observability_metrics` (PythonOperator)
    - Wire dependencies: `ingest_bronze >> read_watermark >> dbt_run_silver >> dbt_test_silver >> dbt_run_gold >> dbt_test_gold >> update_watermark >> refresh_observability_metrics`
    - Read `dag_run.conf.get('full_refresh', False)` and append `--full-refresh` flag to dbt commands when true
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.7, 6.8, 2.6_

  - [ ] 10.2 Write unit tests for DAG structure
    - Load DAG via `DagBag`; assert task count is 8, dependency order is correct, `schedule_interval` equals `'0 2 * * *'`, `retries` equals 3
    - Assert `on_failure_callback` is not None
    - Located in `tests/unit/test_dag_structure.py`
    - _Requirements: 6.2, 6.3_

  - [ ] 10.3 Write unit tests for Source Simulator
    - Test CSV generation produces the expected number of columns and correct header
    - Test CLI parameter parsing and default fallback values
    - Test that `counter.json` is updated after each run
    - Located in `tests/unit/test_simulator.py`
    - _Requirements: 7.9, 7.10_

- [~] 11. Checkpoint — core pipeline passes locally
  - Ensure all tests pass, ask the user if questions arise.
  - Run `docker compose up` and verify all services start healthy
  - Run `dbt seed && dbt run && dbt test --target dev` and confirm zero failures

- [ ] 12. Observability hook and tables
  - [~] 12.1 Implement `airflow/plugins/hooks/observability_hook.py`
    - Extend `BaseHook`; expose methods:
      - `log_pipeline_run(dag_run_id, task_id, status, started_at, finished_at, error_message, records_processed)` → INSERT into `observability.pipeline_runs`
      - `log_layer_row_counts(layer, table_name, row_count)` → INSERT into `observability.layer_row_counts`
      - `log_dq_result(test_name, status, failure_count, details_dict)` → INSERT into `observability.dq_results`
      - `log_freshness(source_name, last_source_file_ts, gold_updated_at)` → compute `freshness_lag_hours`, INSERT into `observability.freshness_metrics`; if lag > 24 h, call `send_alert()`
      - `send_alert(message)` → HTTP POST to `ALERT_WEBHOOK_URL` or Airflow SMTP if webhook not set
    - _Requirements: 11.1, 11.2, 11.3, 11.4, 11.7_

  - [ ] 12.2 Write property test — Property 14: Observability Row Count Consistency
    - **Property 14: Observability Row Count Consistency**
    - **Validates: Requirements 11.3**
    - Generate test tables with random row counts using Hypothesis; call `log_layer_row_counts` with the known count; query `observability.layer_row_counts` and assert stored count equals the actual `SELECT COUNT(*)`
    - Located in `tests/properties/test_observability_properties.py`

- [ ] 13. GitHub Actions CI workflow
  - [~] 13.1 Write `.github/workflows/ci.yml`
    - Trigger: `on: pull_request` to `main`
    - Steps: checkout, setup Python 3.11, install pinned deps from `requirements.txt`, spin up `services: postgres:15`, run `dbt deps && dbt seed --target ci && dbt run --target ci && dbt test --target ci`, run `pytest tests/properties/ tests/unit/ -v --tb=short`, run `sqlfluff lint dbt/models/ --dialect postgres`
    - On any step failure: mark the PR check as failed
    - Post results as GitHub commit status check
    - _Requirements: 10.1, 10.2, 10.3, 10.4, 10.10_

- [ ] 14. GitHub Actions Deploy workflow
  - [~] 14.1 Write `.github/workflows/deploy.yml`
    - Trigger: `on: push` to `main`
    - Steps: run full dbt test suite against staging; on success run `dbt run --target prod`; run post-deploy `dbt test --target prod`; on test failure rollback by checking out the previous release tag and running `dbt run --target prod` against it; post deployment status as commit status check
    - _Requirements: 10.5, 10.6, 10.7, 10.8, 10.10_

- [ ] 15. Integration tests
  - [~] 15.1 Write `tests/integration/test_full_pipeline_run.py`
    - Tag: `@pytest.mark.integration`
    - Trigger the DAG via Airflow REST API; poll until completion; assert `bronze.raw_sales_records`, `silver.sales_records`, and `gold.fact_sales` all have row counts > 0
    - _Requirements: 6.1, 1.2, 3.8, 4.1_

  - [~] 15.2 Write `tests/integration/test_data_quality_halt.py`
    - Inject a record that violates a DQ contract (e.g., negative `units_sold`); trigger the DAG; assert the pipeline halts before `dbt_run_gold` executes and the `gold.fact_sales` row count is unchanged
    - _Requirements: 5.8_

  - [~] 15.3 Write `tests/integration/test_incremental_loading.py`
    - Run the pipeline once to establish a baseline row count; upload a second simulator file with new Order_IDs; run the pipeline again; assert that `silver.sales_records` gained exactly the new record count and no existing records were duplicated
    - _Requirements: 2.2, 2.3, 2.4, 3.5_

  - [~] 15.4 Write `tests/integration/test_freshness_alert.py`
    - Set `audit.watermarks.watermark_ts` to `now() - interval '25 hours'` directly in the test DB; trigger `refresh_observability_metrics`; assert `ObservabilityAlertHook.send_alert` was called with a stale-data message
    - _Requirements: 11.7_

- [ ] 16. GitHub Actions Infrastructure workflow
  - [~] 16.1 Write `.github/workflows/infra.yml`
    - Trigger: `workflow_dispatch` (manual only)
    - Require manual approval via a GitHub environment protection rule named `production`
    - Steps: checkout, setup Terraform, run `terraform init`, run `terraform validate`, run `terraform plan` and post plan output as a workflow summary, on approval run `terraform apply -auto-approve`
    - _Requirements: 10.9, 9.8_

- [ ] 17. Terraform modules
  - [~] 17.1 Write `terraform/modules/storage/main.tf`
    - Define Azure Blob Storage containers (or S3 buckets) for `source`, `bronze`, `silver`, `gold`
    - Output container URLs / bucket ARNs
    - _Requirements: 9.2_

  - [~] 17.2 Write `terraform/modules/warehouse/main.tf`
    - Define Snowflake database + schemas or Azure Synapse Analytics pool
    - Define compute sizing variables (defaulting to smallest tier suitable for demo)
    - Output connection string and warehouse endpoint
    - _Requirements: 9.3_

  - [~] 17.3 Write `terraform/modules/orchestration/main.tf`
    - Define Azure Data Factory pipelines or Airflow container instance
    - Reference storage and warehouse outputs via module inputs
    - _Requirements: 9.4_

  - [~] 17.4 Write `terraform/modules/monitoring/main.tf`
    - Define Azure Monitor alert rules or CloudWatch log groups and metric alarms
    - Configure alert channels (email / webhook) via variables
    - _Requirements: 9.5_

  - [~] 17.5 Write `terraform/modules/networking/main.tf`
    - Define VNet / VPC, NSG / security group rules allowing only required ports
    - _Requirements: 9.6_

  - [~] 17.6 Write `terraform/main.tf`, `variables.tf`, `outputs.tf`, and `backend.tf`
    - Compose all five modules; expose all connection strings and endpoints as root outputs
    - Configure remote state backend (Azure Storage Account blob or S3 bucket)
    - _Requirements: 9.1, 9.7, 9.10_

- [~] 18. Checkpoint — Terraform validates and CI green
  - Run `terraform validate` and `terraform plan` against a mock backend; confirm zero errors
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 19. Metabase dashboard
  - [~] 19.1 Configure Metabase container and database connection
    - Add a Metabase setup JSON or environment variables in `docker-compose.yml` to auto-connect Metabase to the PostgreSQL warehouse (host: `postgres`, port: `5432`, db: the gold schema database)
    - Verify Metabase UI is reachable on `http://localhost:3000`
    - _Requirements: 12.1, 12.11_

  - [~] 19.2 Create Metabase dashboard with required charts
    - Create a Question / chart for: total revenue by Country (bar), total revenue by Product (bar), total revenue by Sales Channel (pie or bar), monthly revenue trend (line chart), Units Sold over time (line chart)
    - Group charts into a single Dashboard named "Retail Sales Overview"
    - Add filters for date range, Country, Product, and Channel using Metabase dashboard filter widgets
    - Verify formatted currency display (two decimal places) on Revenue and Units Sold metrics
    - Export dashboard definition as a JSON file at `metabase/dashboard_export.json` for reproducibility
    - _Requirements: 12.2, 12.3, 12.4, 12.5, 12.6, 12.7, 12.8, 12.9_

- [ ] 20. Documentation
  - [~] 20.1 Write `README.md`
    - Include project overview paragraph, Mermaid architecture diagram (Bronze → Silver → Gold with all components), feature comparison table (legacy SSIS vs modern lakehouse), local development quick-start (`docker compose up`), cloud deployment quick-start (`terraform apply`), and GitHub repository tags list
    - _Requirements: 13.1, 13.2, 13.3, 13.4, 13.5, 13.10_

  - [~] 20.2 Write `DEMO.md`
    - Step-by-step walkthrough: run simulator, trigger DAG, observe Bronze/Silver/Gold row counts, inject a bad record to trigger DQ halt, view Metabase dashboard
    - Include placeholder image references for screenshots / GIFs at each key step
    - _Requirements: 13.6, 13.7_

  - [~] 20.3 Write `LESSONS_LEARNED.md`
    - Document at least three technical challenges with tradeoffs and solutions (e.g., incremental strategy + watermark coordination, surrogate key Unknown sentinel design, Hypothesis testing against dbt SQL logic)
    - _Requirements: 13.9_

  - [~] 20.4 Write `docs/data_dictionary.md`
    - Document every table (Bronze, Silver, Gold, Audit, Observability) with column names, data types, and descriptions
    - Add inline code comments to `silver_sales_records.sql`, `fact_sales.sql`, and `watermark_hook.py` explaining non-obvious logic
    - _Requirements: 13.8_

- [~] 21. Final checkpoint — full stack green
  - Run `docker compose up` → confirm all services healthy
  - Run `dbt seed && dbt run && dbt test --target dev` → zero failures
  - Run `pytest tests/properties/ tests/unit/ -v --tb=short` → zero failures
  - Run `sqlfluff lint dbt/models/ --dialect postgres` → zero violations
  - Ensure all tests pass, ask the user if questions arise.

---

## Notes

- Tasks marked with `*` are optional and can be skipped for a faster MVP delivery
- Each task references specific requirements for full traceability
- Property tests use Hypothesis with `max_examples=100` minimum per property
- dbt tests run in every CI pipeline execution; integration tests run in a separate nightly workflow
- The `Unknown` sentinel rows (surrogate key = -1) must be present in seeds before Gold models run
- Terraform modules are written for Azure by default but are parameterised for AWS via variable swap
- Metabase dashboard is reproducible via the exported JSON; no manual GUI steps are required after initial seeding
- All secrets flow through `.env` → Docker Compose env injection → Airflow/dbt connections; no secrets are committed to the repository

---

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1"] },
    { "id": 1, "tasks": ["1.2", "2.1"] },
    { "id": 2, "tasks": ["2.2", "3.1"] },
    { "id": 3, "tasks": ["3.2", "3.3"] },
    { "id": 4, "tasks": ["4.1", "6.1", "7.2", "8.1"] },
    { "id": 5, "tasks": ["4.2", "4.3", "4.4", "5.1", "6.3", "6.4", "7.1"] },
    { "id": 6, "tasks": ["5.2", "6.2", "7.3", "7.4", "7.5", "7.6", "7.7", "9.3"] },
    { "id": 7, "tasks": ["8.2", "8.3", "9.1", "9.2", "10.3"] },
    { "id": 8, "tasks": ["8.4", "8.5", "9.4", "10.1", "10.2", "12.1"] },
    { "id": 9, "tasks": ["12.2", "13.1", "14.1"] },
    { "id": 10, "tasks": ["15.1", "15.2", "15.3", "15.4", "16.1"] },
    { "id": 11, "tasks": ["17.1", "17.2", "17.3", "17.4", "17.5"] },
    { "id": 12, "tasks": ["17.6"] },
    { "id": 13, "tasks": ["19.1"] },
    { "id": 14, "tasks": ["19.2"] },
    { "id": 15, "tasks": ["20.1", "20.2", "20.3", "20.4"] }
  ]
}
```
