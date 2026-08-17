# Requirements Document

## Introduction

The Retail Sales Lakehouse Migration project transforms a legacy SSIS/SSAS academic ETL pipeline into a modern, cloud-native data lakehouse demonstrating production-ready data engineering practices. The system implements a medallion architecture (Bronze/Silver/Gold layers) with incremental loading, data quality contracts, orchestration, observability, and infrastructure-as-code to create a portfolio-grade showcase of cloud data platform modernization.

## Glossary

- **Pipeline_Orchestrator**: Apache Airflow or Azure Data Factory component responsible for scheduling, executing, and monitoring data pipeline workflows
- **Bronze_Layer**: Immutable raw data landing zone in cloud blob storage (Azure Blob/S3) storing exact replicas of source CSV files
- **Silver_Layer**: Cleaned, typed, and validated data layer storing structured data with standardized schemas and data quality enforcement
- **Gold_Layer**: Analytics-optimized layer containing the star schema (fact and dimension tables) for business intelligence consumption
- **Data_Quality_Engine**: dbt test framework component that validates data integrity through uniqueness, not-null, referential integrity, and custom contract tests
- **Source_Simulator**: Script component that generates and uploads incremental daily sales CSV feeds to cloud blob storage
- **Warehouse**: PostgreSQL (local development) or Snowflake/Azure Synapse (cloud deployment) analytical database storing Silver and Gold layer data
- **Transformation_Engine**: dbt component that executes SQL-based transformations and materializes Silver and Gold layer tables
- **Watermark_Manager**: Component tracking last processed timestamps or sequence numbers to enable incremental data loading
- **Observability_System**: Logging, monitoring, and alerting infrastructure tracking pipeline runs, data freshness, and failure notifications
- **IaC_Provisioner**: Terraform component that provisions and manages cloud infrastructure resources
- **CI_CD_Pipeline**: GitHub Actions workflows that run automated tests on pull requests and deploy changes on merge to main branch
- **BI_Dashboard**: Power BI or Metabase visualization layer connected to Gold layer star schema
- **Star_Schema**: Dimensional model containing one fact table (sales transactions) and six dimension tables (Country, Region, Branch, Product, Category, Channel)

## Requirements

### Requirement 1: Bronze Layer Raw Data Ingestion

**User Story:** As a data engineer, I want to ingest raw CSV files into an immutable Bronze layer, so that I have an auditable record of all source data exactly as received.

#### Acceptance Criteria

1. WHEN a CSV file is uploaded to the source blob storage container, THE Pipeline_Orchestrator SHALL detect the new file within 5 minutes
2. WHEN a new source file is detected, THE Pipeline_Orchestrator SHALL copy the file to Bronze_Layer with original filename and timestamp suffix
3. THE Bronze_Layer SHALL store files in cloud blob storage with path pattern `bronze/sales/{YYYY}/{MM}/{DD}/{filename}_{timestamp}.csv`
4. THE Bronze_Layer SHALL preserve exact byte-for-byte copies of source files without any transformations
5. WHEN a file upload to Bronze_Layer fails, THE Pipeline_Orchestrator SHALL retry up to 3 times with exponential backoff
6. WHEN all retries are exhausted, THE Observability_System SHALL send failure alert notifications
7. FOR ALL successfully ingested files, THE Pipeline_Orchestrator SHALL record metadata (filename, size, timestamp, checksum) in an audit table

### Requirement 2: Incremental Data Loading with Watermarking

**User Story:** As a data engineer, I want to load only new or changed data using watermark-based incremental loading, so that I minimize processing time and computational costs.

#### Acceptance Criteria

1. THE Watermark_Manager SHALL maintain high-water-mark timestamps for each data source in the Warehouse
2. WHEN processing Bronze_Layer files, THE Transformation_Engine SHALL read the current watermark value for the data source
3. THE Transformation_Engine SHALL filter source data to include only records where Order_Date is greater than the watermark timestamp
4. WHEN incremental load completes successfully, THE Watermark_Manager SHALL update the watermark to the maximum Order_Date processed
5. IF the watermark table does not exist on first run, THEN THE Watermark_Manager SHALL initialize all watermarks to epoch timestamp (1970-01-01)
6. WHERE full refresh is explicitly requested, THE Transformation_Engine SHALL process all records regardless of watermark values
7. FOR ALL incremental loads, THE Transformation_Engine SHALL log the number of new records processed and the updated watermark value

### Requirement 3: Silver Layer Data Cleansing and Validation

**User Story:** As a data engineer, I want to transform raw Bronze data into cleaned, typed, and validated Silver layer tables, so that downstream analytics queries operate on high-quality structured data.

#### Acceptance Criteria

1. WHEN Bronze_Layer CSV files are processed, THE Transformation_Engine SHALL parse and validate each column against defined data types
2. THE Transformation_Engine SHALL convert Order_Date and Ship_Date text columns to DATE data type with format validation
3. THE Transformation_Engine SHALL convert Units_Sold to INTEGER, Unit_Price to DECIMAL(10,2), and Unit_Cost to DECIMAL(10,2)
4. WHEN a record contains invalid data types or unparseable values, THE Transformation_Engine SHALL log the record to an error table with rejection reason
5. THE Transformation_Engine SHALL remove duplicate records based on Order_ID uniqueness constraint
6. THE Transformation_Engine SHALL trim leading and trailing whitespace from all text columns
7. THE Transformation_Engine SHALL replace NULL or empty string values in required fields (Order_ID, Order_Date, Units_Sold) with rejection to error table
8. THE Transformation_Engine SHALL materialize cleansed data to Silver_Layer tables in the Warehouse with naming pattern `silver.sales_records`
9. FOR ALL Silver layer transformations, THE Data_Quality_Engine SHALL execute validation tests before materializing tables

### Requirement 4: Gold Layer Star Schema Creation

**User Story:** As a data analyst, I want analytics-optimized star schema tables in the Gold layer, so that I can efficiently query sales metrics with dimensional slicing and dicing.

#### Acceptance Criteria

1. THE Transformation_Engine SHALL create a fact table `gold.fact_sales` containing Order_ID, Order_Date, Ship_Date, Units_Sold, Unit_Price, Unit_Cost, Total_Revenue, Total_Cost, and foreign keys to dimension tables
2. THE Transformation_Engine SHALL derive Total_Revenue as Units_Sold multiplied by Unit_Price
3. THE Transformation_Engine SHALL derive Total_Cost as Units_Sold multiplied by Unit_Cost
4. THE Transformation_Engine SHALL create dimension table `gold.dim_country` with surrogate key country_key, natural key C_ID, and Country name
5. THE Transformation_Engine SHALL create dimension table `gold.dim_region` with surrogate key region_key and natural key Region_ID
6. THE Transformation_Engine SHALL create dimension table `gold.dim_branch` with surrogate key branch_key, natural key B_ID, and B_Name
7. THE Transformation_Engine SHALL create dimension table `gold.dim_product` with surrogate key product_key and natural key Item_Type
8. THE Transformation_Engine SHALL create dimension table `gold.dim_category` with surrogate key category_key, natural key C_ID, and C_Name
9. THE Transformation_Engine SHALL create dimension table `gold.dim_channel` with surrogate key channel_key and natural key Sales_Channel
10. THE Transformation_Engine SHALL perform surrogate key lookups to populate foreign keys in fact_sales based on natural keys from Silver layer
11. WHEN a natural key in fact records does not match any dimension record, THE Transformation_Engine SHALL insert a default dimension record with surrogate key -1 and label "Unknown"
12. THE Transformation_Engine SHALL create indexes on all foreign key columns in fact_sales
13. THE Transformation_Engine SHALL create indexes on all surrogate key columns in dimension tables

### Requirement 5: Data Quality Contract Enforcement

**User Story:** As a data engineer, I want automated data quality tests to fail the pipeline loudly when bad data appears, so that downstream consumers never receive corrupted or invalid datasets.

#### Acceptance Criteria

1. THE Data_Quality_Engine SHALL validate uniqueness of Order_ID in silver.sales_records table
2. THE Data_Quality_Engine SHALL validate not-null constraints on Order_ID, Order_Date, Units_Sold, Unit_Price, and Unit_Cost columns
3. THE Data_Quality_Engine SHALL validate referential integrity between fact_sales foreign keys and dimension table surrogate keys
4. THE Data_Quality_Engine SHALL validate that Units_Sold is greater than zero
5. THE Data_Quality_Engine SHALL validate that Unit_Price is greater than or equal to zero
6. THE Data_Quality_Engine SHALL validate that Order_Date is less than or equal to Ship_Date
7. THE Data_Quality_Engine SHALL validate that Total_Revenue equals Units_Sold multiplied by Unit_Price within 0.01 precision tolerance
8. WHEN any data quality test fails, THE Pipeline_Orchestrator SHALL halt the pipeline and prevent downstream table materialization
9. WHEN data quality tests fail, THE Observability_System SHALL send failure alert notifications with test failure details
10. THE Data_Quality_Engine SHALL log all test results (pass/fail counts, failure details) to a data quality metrics table

### Requirement 6: Pipeline Orchestration with Retry and Alerting

**User Story:** As a data engineer, I want orchestrated pipeline execution with automatic retries and failure alerting, so that transient issues are handled gracefully and permanent failures are escalated promptly.

#### Acceptance Criteria

1. THE Pipeline_Orchestrator SHALL execute pipeline tasks in dependency order: Bronze ingestion, then Silver transformation, then Gold transformation, then data quality tests
2. THE Pipeline_Orchestrator SHALL schedule pipeline execution daily at 2:00 AM UTC
3. WHEN a pipeline task fails, THE Pipeline_Orchestrator SHALL retry the failed task up to 3 times with 5-minute intervals
4. WHEN all retries are exhausted, THE Pipeline_Orchestrator SHALL mark the pipeline run as failed and halt execution
5. WHEN a pipeline run fails, THE Observability_System SHALL send alert notifications via email or webhook
6. THE Pipeline_Orchestrator SHALL record task execution metadata (start time, end time, status, error messages) in an orchestration log table
7. THE Pipeline_Orchestrator SHALL expose a REST API or UI for manual pipeline triggering
8. WHERE manual pipeline trigger is requested, THE Pipeline_Orchestrator SHALL accept optional parameters for full refresh mode

### Requirement 7: Source Data Simulation Script

**User Story:** As a data engineer, I want an automated script that simulates ongoing daily sales data feeds, so that I can demonstrate incremental loading and continuous pipeline operation.

#### Acceptance Criteria

1. THE Source_Simulator SHALL generate synthetic daily sales CSV files with realistic data distributions
2. THE Source_Simulator SHALL generate between 50 and 200 sales records per daily file
3. THE Source_Simulator SHALL assign sequential Order_IDs to ensure uniqueness across all generated files
4. THE Source_Simulator SHALL generate Order_Date values matching the current simulation date
5. THE Source_Simulator SHALL randomly select Country, Region, Branch, Product, Category, and Channel values from existing dimension seed data
6. THE Source_Simulator SHALL generate Units_Sold values between 100 and 10000
7. THE Source_Simulator SHALL generate Unit_Price values between 5.00 and 1000.00
8. THE Source_Simulator SHALL upload generated CSV files to source blob storage with filename pattern `sales_{YYYY-MM-DD}.csv`
9. THE Source_Simulator SHALL accept command-line parameters for simulation date and number of records to generate
10. WHERE Source_Simulator is executed without parameters, THE Source_Simulator SHALL default to current date and 150 records

### Requirement 8: Local Development Environment with Docker

**User Story:** As a developer, I want a fully containerized local development environment, so that I can run the entire pipeline locally without manual dependency installation.

#### Acceptance Criteria

1. THE development environment SHALL provide a Docker Compose configuration file defining all service containers
2. THE Docker Compose configuration SHALL include containers for PostgreSQL database, Airflow webserver, Airflow scheduler, Airflow worker, and MinIO object storage
3. WHEN `docker-compose up` is executed, THE development environment SHALL start all services and initialize databases within 2 minutes
4. THE development environment SHALL mount local dbt project directories into Airflow containers for live code editing
5. THE development environment SHALL expose Airflow web UI on localhost port 8080
6. THE development environment SHALL expose PostgreSQL database on localhost port 5432
7. THE development environment SHALL expose MinIO storage UI on localhost port 9001
8. THE development environment SHALL initialize PostgreSQL with database schemas for Bronze, Silver, and Gold layers
9. THE development environment SHALL seed dimension tables with initial master data from CSV files
10. WHEN `docker-compose down` is executed, THE development environment SHALL stop all containers and optionally preserve database volumes

### Requirement 9: Infrastructure as Code with Terraform

**User Story:** As a DevOps engineer, I want infrastructure provisioned and managed via Terraform, so that cloud resources are versioned, reproducible, and auditable.

#### Acceptance Criteria

1. THE IaC_Provisioner SHALL define Terraform modules for Azure or AWS cloud resource provisioning
2. THE IaC_Provisioner SHALL provision blob storage containers for Bronze, Silver, and Gold layers
3. THE IaC_Provisioner SHALL provision a Snowflake or Azure Synapse warehouse with appropriate compute sizing
4. THE IaC_Provisioner SHALL provision Azure Data Factory or equivalent orchestration service
5. THE IaC_Provisioner SHALL provision monitoring and logging services
6. THE IaC_Provisioner SHALL configure network security groups and firewall rules for secure access
7. THE IaC_Provisioner SHALL output connection strings and service endpoints as Terraform output variables
8. WHEN `terraform apply` is executed, THE IaC_Provisioner SHALL provision all resources and report successful creation
9. WHEN `terraform destroy` is executed, THE IaC_Provisioner SHALL tear down all provisioned resources
10. THE IaC_Provisioner SHALL store Terraform state files in remote backend storage for team collaboration

### Requirement 10: CI/CD Pipeline with Automated Testing

**User Story:** As a developer, I want automated testing on pull requests and deployment on merge, so that code quality is enforced and deployments are consistent.

#### Acceptance Criteria

1. THE CI_CD_Pipeline SHALL trigger on every pull request to the main branch
2. WHEN a pull request is opened, THE CI_CD_Pipeline SHALL execute dbt test suite against a test database
3. WHEN a pull request is opened, THE CI_CD_Pipeline SHALL execute SQL linting checks on all dbt model files
4. WHEN dbt tests or linting checks fail, THE CI_CD_Pipeline SHALL mark the pull request check as failed and block merge
5. WHEN a pull request is merged to main branch, THE CI_CD_Pipeline SHALL trigger deployment workflow
6. THE CI_CD_Pipeline deployment workflow SHALL apply dbt model changes to the production Warehouse
7. THE CI_CD_Pipeline deployment workflow SHALL execute full dbt test suite against production data
8. WHEN production dbt tests fail, THE CI_CD_Pipeline SHALL roll back model changes and send failure notifications
9. THE CI_CD_Pipeline SHALL require manual approval for infrastructure changes before applying Terraform modifications
10. THE CI_CD_Pipeline SHALL publish deployment status and test results as GitHub commit status checks

### Requirement 11: Data Observability and Monitoring

**User Story:** As a data engineer, I want comprehensive logging and monitoring of pipeline runs, so that I can troubleshoot failures and track data quality trends over time.

#### Acceptance Criteria

1. THE Observability_System SHALL log all pipeline task executions with timestamps, status, duration, and error messages
2. THE Observability_System SHALL track data freshness metrics measuring time between source data arrival and Gold layer availability
3. THE Observability_System SHALL track row counts for each Bronze, Silver, and Gold layer table after each pipeline run
4. THE Observability_System SHALL track data quality test pass/fail rates over time
5. THE Observability_System SHALL expose metrics via a dashboard showing pipeline run history for the last 30 days
6. THE Observability_System SHALL expose metrics showing data quality trends for the last 30 days
7. WHEN data freshness exceeds 24 hours, THE Observability_System SHALL send stale data alert notifications
8. WHEN pipeline execution duration exceeds 2 times the historical average, THE Observability_System SHALL send performance degradation alerts
9. THE Observability_System SHALL retain pipeline execution logs for 90 days
10. THE Observability_System SHALL export logs to cloud logging services for long-term retention

### Requirement 12: Business Intelligence Dashboard

**User Story:** As a business analyst, I want an interactive dashboard connected to the Gold layer star schema, so that I can analyze sales trends by country, product, channel, and time period.

#### Acceptance Criteria

1. THE BI_Dashboard SHALL connect to the Warehouse and query gold.fact_sales and dimension tables
2. THE BI_Dashboard SHALL display total revenue metrics aggregated by Country dimension
3. THE BI_Dashboard SHALL display total revenue metrics aggregated by Product dimension
4. THE BI_Dashboard SHALL display total revenue metrics aggregated by Sales Channel dimension
5. THE BI_Dashboard SHALL display total revenue trends over time with monthly granularity
6. THE BI_Dashboard SHALL provide interactive filters for date range selection
7. THE BI_Dashboard SHALL provide interactive filters for Country, Product, and Channel multi-select
8. THE BI_Dashboard SHALL display Units Sold and Total Revenue in formatted currency with two decimal places
9. THE BI_Dashboard SHALL refresh data on-demand when user clicks refresh button
10. WHERE BI_Dashboard is Power BI, THE BI_Dashboard SHALL publish to Power BI Service for web access
11. WHERE BI_Dashboard is Metabase, THE BI_Dashboard SHALL deploy as containerized service accessible via web browser

### Requirement 13: Documentation and Portfolio Presentation

**User Story:** As a job candidate, I want comprehensive documentation and architecture diagrams, so that recruiters and hiring managers can understand the project's technical depth and my engineering skills.

#### Acceptance Criteria

1. THE project repository SHALL include a README.md file with project overview, architecture diagram, and setup instructions
2. THE README.md SHALL include an architecture diagram illustrating Bronze/Silver/Gold layers, orchestration, and BI components
3. THE README.md SHALL include a feature comparison table contrasting legacy SSIS approach with modern lakehouse approach
4. THE project repository SHALL include setup instructions for local Docker development environment
5. THE project repository SHALL include setup instructions for cloud deployment via Terraform
6. THE project repository SHALL include a DEMO.md file with step-by-step walkthrough of key features
7. THE DEMO.md file SHALL include screenshots or animated GIFs demonstrating incremental loading, data quality failures, and dashboard visualizations
8. THE project repository SHALL include inline code comments explaining complex dbt transformations and orchestration logic
9. THE project repository SHALL include a LESSONS_LEARNED.md file documenting technical challenges, tradeoffs, and solutions implemented
10. THE project repository SHALL include a GitHub repository description and tags for discoverability (data-engineering, dbt, airflow, terraform, medallion-architecture)

## Notes

This requirements document defines a production-grade data lakehouse migration project suitable for portfolio demonstration. The implementation prioritizes cost-effective local development via Docker while maintaining cloud deployment capability for final showcase.

**Key Design Principles:**
- **Incremental Processing:** Watermark-based loading minimizes compute costs and processing time
- **Data Quality First:** Automated tests fail loudly to prevent bad data propagation
- **Observability:** Comprehensive logging and alerting for production-ready operations
- **Reproducibility:** Docker Compose enables zero-setup local development
- **Infrastructure as Code:** Terraform ensures versioned, auditable cloud provisioning
- **Separation of Concerns:** Medallion architecture cleanly separates raw, cleansed, and analytics-optimized data

**Portfolio Differentiation:**
This project demonstrates advanced data engineering skills beyond typical bootcamp projects through incremental loading, data quality contracts, IaC provisioning, CI/CD automation, and observability infrastructure—showcasing production-ready thinking and modern data platform expertise.
