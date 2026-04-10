[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/Nvxy3054)
# ETL Pipeline — Amman Digital Market

## Overview

This project implements an ETL (Extract, Transform, Load) pipeline for the Amman Digital Market database.

The pipeline extracts data from PostgreSQL, cleans and transforms it into customer-level analytics, validates data quality, and loads the results into both a database table and a CSV file.

It also includes advanced features such as outlier detection, data quality reporting, incremental processing, and logging.


## Setup

1. Start PostgreSQL container:
   ```bash
   docker run -d --name postgres-m3-int \
     -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres \
     -e POSTGRES_DB=amman_market \
     -p 5432:5432 -v pgdata_m3_int:/var/lib/postgresql/data \
     postgres:15-alpine
   ```
2. Load schema and data:
   ```bash
   psql -h localhost -U postgres -d amman_market -f schema.sql
   psql -h localhost -U postgres -d amman_market -f seed_data.sql
   ```
3. Install dependencies: `pip install -r requirements.txt`

## How to Run

```bash
python etl_pipeline.py
```

## Output

### 1. customer_analytics.csv

A customer-level analytics table containing:

- `customer_id`
- `customer_name`
- `city`
- `total_orders` — number of distinct orders per customer
- `total_revenue` — total revenue generated
- `avg_order_value` — average revenue per order
- `top_category` — highest revenue category per customer
- `is_outlier` — indicates statistical anomaly

## Quality Checks

The following validations are performed before loading:

- No null values in `customer_id` or `customer_name`
- All `total_revenue` values are positive
- No duplicate `customer_id`
- Each customer has at least one order

These checks ensure data integrity and prevent corrupted outputs.

## Advanced Features

###  Tier 1 — Outlier Detection

Customers are flagged as outliers if:
total_revenue > mean + 3 × standard deviation

This helps identify abnormal or extreme customer behavior.



### 🔹 Tier 2 — Incremental ETL

The pipeline processes only new records using:

- `etl_metadata` table  
- Last successful run timestamp  

This improves performance and scalability.



###  Tier 3 — Logging System

The pipeline uses Python's `logging` module to:

- Track execution steps  
- Provide timestamps and log levels  
- Replace basic print statements  

## Project Structure
├── etl_pipeline.py
├── schema.sql
├── seed_data.sql
├── requirements.txt
├── tests/
│   └── test_etl.py
├── output/
│   ├── customer_analytics.csv
│   └── quality_report.json

## License

This repository is provided for educational use only. See [LICENSE](LICENSE) for terms.

You may clone and modify this repository for personal learning and practice, and reference code you wrote here in your professional portfolio. Redistribution outside this course is not permitted.
