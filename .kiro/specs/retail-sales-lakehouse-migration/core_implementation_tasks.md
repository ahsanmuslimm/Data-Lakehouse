# Core Implementation Tasks: Retail Sales Lakehouse Migration

This document outlines only the core implementation tasks necessary to build the project, omitting unit/property/integration tests to focus strictly on building the target architecture.

## Tasks

- [x] 1. Repository scaffolding and local dev environment
  - [x] 1.1 Create top-level directory structure
  - [x] 1.2 Write `docker-compose.yml`

- [x] 2. Database schema initialization
  - [x] 2.1 Write `scripts/init_schemas.sql`
  - [x] 2.2 Wire `schema-init` Docker service to run `init_schemas.sql` on startup

- [x] 3. dbt project setup
  - [x] 3.1 Initialise dbt project and configure profiles
  - [x] 3.2 Add dimension seed CSV files and `Unknown` sentinel rows
  - [x] 3.3 Write staging model `dbt/models/staging/stg_bronze_sales.sql`

- [x] 4. Source Simulator script
  - [x] 4.1 Implement `scripts/simulate_feed.py`

- [ ] 5. Bronze Ingest Operator
  - [x] 5.1 Implement `airflow/plugins/operators/bronze_ingest_operator.py`

- [ ] 6. Watermark Manager hook
  - [x] 6.1 Implement `airflow/plugins/hooks/watermark_hook.py`

- [ ] 7. Silver dbt models
  - [x] 7.1 Write `dbt/models/silver/silver_sales_records.sql`
  - [x] 7.2 Write `dbt/macros/reject_to_error_table.sql`
  - [x] 7.3 Add dbt model contracts to Silver YAML configs (`dbt/models/silver/schema.yml`)

- [ ] 8. Gold dbt models
  - [x] 8.1 Write `dbt/macros/generate_surrogate_key.sql`
  - [x] 8.2 Write six dimension models (`dbt/models/gold/dim_*.sql`)
  - [ ] 8.3 Write `dbt/models/gold/fact_sales.sql`
  - [ ] 8.4 Add dbt model contracts to Gold YAML configs (`dbt/models/gold/schema.yml`)

- [ ] 9. Airflow DAG
  - [ ] 9.1 Write `airflow/dags/retail_sales_pipeline.py`

- [ ] 10. Observability hook and tables
  - [ ] 10.1 Implement `airflow/plugins/hooks/observability_hook.py`

- [ ] 11. GitHub Actions CI/CD workflows
  - [ ] 11.1 Write `.github/workflows/ci.yml`
  - [ ] 11.2 Write `.github/workflows/deploy.yml`
  - [ ] 11.3 Write `.github/workflows/infra.yml`

- [ ] 12. Terraform modules
  - [ ] 12.1 Write `terraform/modules/storage/main.tf`
  - [ ] 12.2 Write `terraform/modules/warehouse/main.tf`
  - [ ] 12.3 Write `terraform/modules/orchestration/main.tf`
  - [ ] 12.4 Write `terraform/modules/monitoring/main.tf`
  - [ ] 12.5 Write `terraform/modules/networking/main.tf`
  - [ ] 12.6 Write `terraform/main.tf`, `variables.tf`, `outputs.tf`, and `backend.tf`

- [ ] 13. Metabase dashboard
  - [ ] 13.1 Configure Metabase container and database connection
  - [ ] 13.2 Create Metabase dashboard with required charts

- [ ] 14. Documentation
  - [ ] 14.1 Write `README.md`
  - [ ] 14.2 Write `DEMO.md`
  - [ ] 14.3 Write `LESSONS_LEARNED.md`
  - [ ] 14.4 Write `docs/data_dictionary.md`
