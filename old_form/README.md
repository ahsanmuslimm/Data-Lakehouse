# ETL Pipeline Assignment

This project is an assignment for the Data Warehousing and Data Mining course (CSC-315), focusing on ETL (Extract, Transform, Load) pipeline development using Microsoft SQL Server Integration Services (SSIS) and SQL Server Analysis Services (SSAS).

## Objective

Develop an ETL pipeline to extract data from various sources (CSV files), apply transformations, and load into different destinations including CSV files, SQL Server databases, and OLAP cubes.

## Tasks Overview

### Task 1: Basic ETL from CSV to CSV
- Extract data from a single CSV file.
- Apply transformations.
- Load into another CSV file.

### Task 2: ETL from Multiple CSVs to CSV
- Extract data from multiple CSV files.
- Apply transformations.
- Load into a single CSV file.

### Task 3: ETL from Multiple CSVs to SQL Server
- Extract data from multiple CSV files.
- Apply transformations.
- Load into SQL Server database.

### Task 4: ETL with Scheduling
- Extract data from multiple CSVs and SQL Server.
- Apply transformations.
- Load into SQL Server using scheduled agents.

### Task 5: Data Warehouse Schema Creation
- Use the B1 dataset (multiple CSVs).
- Load data into SQL Server to create star or snowflake schema.

### Task 6: OLAP Cube Development
- Use Adventure Works Data Warehouse dataset.
- Develop OLAP cubes using SSAS.

### Task 7: Multidimensional Functions on OLAP Cubes
- Apply multidimensional functions on the developed OLAP cubes.

### Task 8: Data Analytics with OLAP Cubes
- Use developed OLAP cubes for data analytics (Power BI integration).

## Project Structure

- `ETL_Assignment_01.sln`: Main solution file for the ETL project.
- `ETL_Assignment_01.dtproj`: SSIS project file.
- `Task1.dtsx` to `Task5.dtsx`: SSIS packages for each ETL task.
- `AdventureWorkOLAP/`: SSAS project for OLAP cube development.
  - `AdventureWorkOLAP.dwproj`: SSAS project file.
  - Dimension files: `Dim *.dim`
  - Cube file: `Adventure Works DW2019.cube`
- `B1/`: Dataset folder containing CSV files for tasks.
  - `R1/` to `R6/`: Subfolders with regional CSV data.
  - `Branch.csv`, `Category.csv`, etc.: Master data files.
- `Task Screenshots/`: Screenshots demonstrating task implementations.
- `DW &DM - Assignment Report.docx`: Detailed assignment report.
- `Task8 (DataAnalytics).pbix`: Power BI file for Task 8 data analytics.

## Prerequisites

- Microsoft SQL Server (with SSIS and SSAS components)
- SQL Server Data Tools (SSDT) or Visual Studio with SQL Server extensions
- Power BI Desktop (for Task 8)

## How to Run

1. **Open the Solution:**
   - Open `ETL_Assignment_01.sln` in Visual Studio with SQL Server Data Tools.

2. **Configure Connections:**
   - Update connection managers in each SSIS package to point to your SQL Server instance and data sources.

3. **Execute ETL Packages:**
   - Right-click on each `.dtsx` file and select "Execute Package" or deploy to SSIS catalog.

4. **OLAP Cube:**
   - Open `AdventureWorkOLAP.dwproj` in Visual Studio.
   - Deploy the cube to your SSAS instance.
   - Process the cube to load data.

5. **Data Analytics:**
   - Open `Task8 (DataAnalytics).pbix` in Power BI Desktop.
   - Connect to the deployed OLAP cube.

## Data Sources

- CSV files in `B1/` folder (various sales and master data files).
- Adventure Works DW dataset for OLAP development.

## Outputs

- Transformed CSV files (Tasks 1-2).
- SQL Server databases/tables (Tasks 3-5).
- OLAP cubes and multidimensional analysis (Tasks 6-7).
- Power BI reports (Task 8).

## Screenshots

Refer to the `Task Screenshots/` folder for visual demonstrations of each task's implementation and output.

## Report

For detailed implementation steps, refer to `DW &DM - Assignment Report.docx`.
