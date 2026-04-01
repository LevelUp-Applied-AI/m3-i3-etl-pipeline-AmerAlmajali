"""
ETL Pipeline — Amman Digital Market Customer Analytics

This version upgrades the pipeline into a reusable ETL framework:
- Tier 1: Data quality checks + outlier detection + JSON report
- Tier 2: Incremental loading + ETL metadata tracking
- Tier 3: Config-driven pipeline (no hardcoded logic)

The pipeline reads from a JSON config file to define:
tables, joins, filters, aggregations, validations, and outputs.
"""

from sqlalchemy import create_engine
import pandas as pd
import os
import json
import logging
from datetime import datetime


# =========================
# Logging (Tier 3)
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(stage)s] %(message)s",
)
logger = logging.getLogger(__name__)


def log(stage, message):
    """Helper function to standardize logging format with stage name."""
    logger.info(message, extra={"stage": stage})


# =========================
# Metadata helpers (Tier 2)
# =========================
def get_last_run_time(engine):
    """Get last successful ETL run timestamp from metadata table."""
    try:
        df = pd.read_sql(
            "SELECT MAX(end_time) AS last_run FROM etl_metadata WHERE status='success'",
            engine,
        )
        return df.iloc[0]["last_run"]
    except:
        return None


def log_etl_run(engine, start, end, rows, status):
    """Log ETL run metadata into etl_metadata table."""
    df = pd.DataFrame(
        [
            {
                "start_time": start,
                "end_time": end,
                "rows_processed": rows,
                "status": status,
            }
        ]
    )
    df.to_sql("etl_metadata", engine, if_exists="append", index=False)


# =========================
# EXTRACT
# =========================
def extract(engine, config):
    """Extract data from source tables using config-driven logic.

    Supports incremental extraction for orders table.
    """

    log("EXTRACT", "Starting extraction phase")

    data = {}
    last_run = get_last_run_time(engine)

    for table in config["tables"]:
        # Incremental logic (Tier 2)
        if table == "orders" and last_run:
            query = f"SELECT * FROM {table} WHERE order_date > '{last_run}'"
        else:
            query = f"SELECT * FROM {table}"

        df = pd.read_sql(query, engine)
        data[table] = df

        log("EXTRACT", f"{table}: {len(df)} rows extracted")

    return data


# =========================
# TRANSFORM
# =========================
def transform(data_dict, config=None):
    """Transform raw data into aggregated analytics based on config.

    Steps:
    1. Perform joins
    2. Apply filters
    3. Create calculated fields
    4. Aggregate results
    5. Detect outliers (if revenue exists)
    """

    # =========================
    # Default Config (Backward Compatibility for Tests)
    # =========================
    if config is None:
        config = {
            "base_table": "order_items",
            "joins": [
                {"table": "orders", "key": "order_id"},
                {"table": "products", "key": "product_id"},
                {"table": "customers", "key": "customer_id"},
            ],
            "filters": [
                "status != 'cancelled'",
                "quantity <= 100",
            ],
            "calculated_fields": {"line_total": "quantity * unit_price"},
            "group_by": ["customer_id"],
            "aggregations": {
                "order_id": "nunique",
                "line_total": "sum",
            },
            "rename": {
                "order_id": "total_orders",
                "line_total": "total_revenue",
            },
        }

    log("TRANSFORM", "Starting transformation phase")

    # Start from base table
    df = data_dict[config["base_table"]]

    # =========================
    # Joins (Config-driven)
    # =========================
    for join in config["joins"]:
        df = df.merge(
            data_dict[join["table"]],
            on=join["key"],
            how=join.get("type", "inner"),
        )

    # =========================
    # Filters
    # =========================
    for condition in config.get("filters", []):
        df = df.query(condition)

    # =========================
    # Feature Engineering
    # =========================
    for new_col, expr in config.get("calculated_fields", {}).items():
        df[new_col] = df.eval(expr)

    # =========================
    # Aggregation
    # =========================
    summary = df.groupby(config["group_by"]).agg(config["aggregations"]).reset_index()

    # Rename columns if needed
    summary.rename(columns=config.get("rename", {}), inplace=True)

    # =========================
    # Outlier Detection (Tier 1)
    # =========================
    if "total_revenue" in summary.columns:
        mean = summary["total_revenue"].mean()
        std = summary["total_revenue"].std()
        threshold = mean + 3 * std

        summary["is_outlier"] = summary["total_revenue"] > threshold

    return summary


# =========================
# VALIDATE
# =========================
def validate(df, config=None):
    """Run data quality checks defined in config.

    Raises:
        ValueError if any validation fails.
    """

    # =========================
    # Default Validations (Backward Compatibility for Tests)
    # =========================
    if config is None:
        config = {
            "validations": [
                {
                    "name": "no_null_customer_id",
                    "condition": "df['customer_id'].notna().all()",
                },
                {
                    "name": "no_null_customer_name",
                    "condition": "df['customer_name'].notna().all()",
                },
                {
                    "name": "positive_revenue",
                    "condition": "(df['total_revenue'] > 0).all()",
                },
                {
                    "name": "no_duplicate_customer_id",
                    "condition": "df['customer_id'].is_unique",
                },
                {
                    "name": "positive_total_orders",
                    "condition": "(df['total_orders'] > 0).all()",
                },
            ]
        }

    log("VALIDATE", "Running validation checks")

    results = {}

    for check in config["validations"]:
        name = check["name"]
        condition = check["condition"]

        result = eval(condition)
        results[name] = bool(result)

        log("VALIDATE", f"{name}: {'PASS' if result else 'FAIL'}")

    if not all(results.values()):
        raise ValueError("Data validation failed")

    return results


# =========================
# LOAD
# =========================
def load(df, engine, config):
    """Load results into PostgreSQL table and CSV output."""

    log("LOAD", "Starting load phase")

    table = config["output_table"]
    csv_path = config["output_csv"]

    os.makedirs(os.path.dirname(csv_path), exist_ok=True)

    df.to_sql(table, engine, if_exists="replace", index=False)
    df.to_csv(csv_path, index=False)

    log("LOAD", f"{len(df)} rows loaded successfully")


# =========================
# Quality Report (Tier 1)
# =========================
def generate_quality_report(df, checks, config):
    """Generate JSON data quality report including outliers."""

    outliers = df[df.get("is_outlier", False)]

    report = {
        "timestamp": datetime.now().isoformat(),
        "total_records": len(df),
        "checks": checks,
        "outliers": outliers.to_dict(orient="records"),
    }

    os.makedirs("output", exist_ok=True)

    with open("output/quality_report.json", "w") as f:
        json.dump(report, f, indent=4)

    log("REPORT", "Quality report generated")


# =========================
# MAIN PIPELINE
# =========================
def run_pipeline(config_path):
    """Orchestrate ETL pipeline using external JSON configuration."""

    with open(config_path) as f:
        config = json.load(f)

    engine = create_engine(config["db_url"])

    start_time = datetime.now()

    try:
        # Extract
        data = extract(engine, config)

        # Transform
        df = transform(data, config)

        # Validate
        checks = validate(df, config)

        # Quality report
        generate_quality_report(df, checks, config)

        # Load
        load(df, engine, config)

        end_time = datetime.now()

        # Log metadata (Tier 2)
        log_etl_run(engine, start_time, end_time, len(df), "success")

        log("MAIN", "ETL completed successfully")

    except Exception as e:
        log_etl_run(engine, start_time, datetime.now(), 0, "failed")
        log("ERROR", f"ETL failed: {e}")


if __name__ == "__main__":
    run_pipeline("config_customer.json")
