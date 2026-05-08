# Fleet Analytics Data Engineering Pipeline

## Project Overview

This project implements an end-to-end data engineering pipeline that ingests raw fleet data, transforms it using Apache Spark, stores curated datasets in Parquet format, and loads analytics-ready fact and dimension tables into a PostgreSQL database using JDBC.

The pipeline mirrors real-world analytics platforms by separating ingestion, transformation, curation, and serving layers.

---

## Architecture

Raw CSV Files
|
v
Apache Spark (PySpark)

Data cleaning and validation
Type casting and date parsing
Aggregations and transformations
|
v
Curated Parquet Layer
(data_curated/)
dim_model
dim_car
fact_maintenance_summary
fact_active_issues
|
v
Spark JDBC Writer
|
v
PostgreSQL (Dockerized)
Database: fleet_analytics


### Technologies Used

- Apache Spark (PySpark)
- Parquet
- PostgreSQL
- Docker
- JDBC
- WSL 2

---

## Data Model

The analytics schema follows a dimensional (star-style) model, designed for efficient querying and reporting.

### Dimension Tables

#### dim_model
Stores unique vehicle models.

- `model_id`
- `model_name`
- `manufacturer`
- `fuel_type`

#### dim_car
Stores individual vehicles.

- `car_id`
- `model_id`
- `registration_number`
- `year`

---

### Fact Tables

#### fact_maintenance_summary
Aggregated maintenance activity per vehicle.

- `car_id`
- `maintenance_count`
- `avg_cost`

#### fact_active_issues
Active unresolved vehicle issues.

- `issue_id`
- `car_id`
- `issue_type`
- `severity`
- `opened_date`

---

## How to Run the Project

### 1. Start PostgreSQL Using Docker

```bash
docker run --name fleet-postgres \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=fleet_analytics \
  -p 5432:5432 \
  -d postgres:14


2. Load Curated Parquet Data into PostgreSQL
From the project root directory:
Shellspark-submit --jars postgresql-42.7.3.jar pyspark/load_to_postgres.pyShow more lines

3. Verify the Data Load
Shellpsql -h localhost -U postgres -d fleet_analyticsShow more lines
SQLSELECT COUNT(*) FROM dim_model;SELECT COUNT(*) FROM dim_car;SELECT COUNT(*) FROM fact_maintenance_summary;SELECT COUNT(*) FROM fact_active_issues;Show more lines

Example Analytics Queries
Analytics queries are stored in the sql/ directory.
Examples include:

Vehicles with the most maintenance events
Open issues by vehicle model
Issue severity distribution
Vehicles with the highest average maintenance cost


Key Learning Outcomes

Built production-style Spark ETL jobs
Applied dimensional modeling for analytics
Used Parquet as a curated lakehouse layer
Integrated Spark with PostgreSQL using JDBC
Dockerized the analytics database
Solved real-world WSL, networking, and filesystem challenges


Project Status
✅ Complete
✅ End-to-End Validated
✅ Analytics Ready

