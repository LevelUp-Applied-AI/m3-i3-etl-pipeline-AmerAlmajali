"""ETL Pipeline — Amman Digital Market Customer Analytics

Extracts data from PostgreSQL, transforms it into customer-level summaries,
validates data quality, and loads results to a database table and CSV file.
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
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


# =========================
# Metadata helpers (Tier 2)
# =========================
def get_last_run_time(engine):
    """Get last successful ETL run timestamp."""
    try:
        df = pd.read_sql(
            "SELECT MAX(end_time) AS last_run FROM etl_metadata WHERE status='success'",
            engine,
        )
        return df.iloc[0]["last_run"]
    except:
        return None


def log_etl_run(engine, start, end, rows, status):
    """Log ETL run metadata."""
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


def extract(engine):
    """Extract all source tables from PostgreSQL into DataFrames.

    Args:
        engine: SQLAlchemy engine connected to the amman_market database

    Returns:
        dict: {"customers": df, "products": df, "orders": df, "order_items": df}
    """

    # Incremental logic (Tier 2)
    last_run = get_last_run_time(engine)

    if last_run is None:
        orders = pd.read_sql("SELECT * FROM orders", engine)
    else:
        orders = pd.read_sql(
            f"SELECT * FROM orders WHERE order_date > '{last_run}'", engine
        )

    customers = pd.read_sql("SELECT * FROM customers", engine)
    products = pd.read_sql("SELECT * FROM products", engine)
    order_items = pd.read_sql("SELECT * FROM order_items", engine)

    logger.info("Data extracted")

    return {
        "customers": customers,
        "products": products,
        "orders": orders,
        "order_items": order_items,
    }


def transform(data_dict):
    """Transform raw data into customer-level analytics summary.

    Steps:
    1. Join orders with order_items and products
    2. Compute line_total (quantity * unit_price)
    3. Filter out cancelled orders (status = 'cancelled')
    4. Filter out suspicious quantities (quantity > 100)
    5. Aggregate to customer level: total_orders, total_revenue,
       avg_order_value, top_category

    Args:
        data_dict: dict of DataFrames from extract()

    Returns:
        DataFrame: customer-level summary with columns:
            customer_id, customer_name, city, total_orders,
            total_revenue, avg_order_value, top_category
    """

    customers = data_dict["customers"]
    products = data_dict["products"]
    orders = data_dict["orders"]
    order_items = data_dict["order_items"]

    # Join tables
    df = (
        order_items.merge(orders, on="order_id")
        .merge(products, on="product_id")
        .merge(customers, on="customer_id")
    )

    # Filters
    df = df.query("status != 'cancelled'")
    df = df.query("quantity <= 100")

    # Feature Engineering
    df["line_total"] = df["quantity"] * df["unit_price"]

    # Aggregation
    summary = (
        df.groupby("customer_id")
        .agg(total_orders=("order_id", "nunique"), total_revenue=("line_total", "sum"))
        .reset_index()
    )

    summary["avg_order_value"] = summary["total_revenue"] / summary["total_orders"]

    summary = summary.merge(
        customers[["customer_id", "customer_name", "city"]], on="customer_id"
    )

    # Top category
    category_revenue = (
        df.groupby(["customer_id", "category"])["line_total"].sum().reset_index()
    )

    top_category = (
        category_revenue.sort_values(
            ["customer_id", "line_total"], ascending=[True, False]
        )
        .drop_duplicates("customer_id")
        .rename(columns={"category": "top_category"})
    )

    summary = summary.merge(top_category, on="customer_id")

    # =========================
    # Outlier Detection (Tier 1)
    # =========================
    mean = summary["total_revenue"].mean()
    std = summary["total_revenue"].std()
    threshold = mean + 3 * std

    summary["is_outlier"] = summary["total_revenue"] > threshold

    return summary


def validate(df):
    """Run data quality checks on the transformed DataFrame.

    Checks:
    - No nulls in customer_id or customer_name
    - total_revenue > 0 for all customers
    - No duplicate customer_ids
    - total_orders > 0 for all customers

    Args:
        df: transformed customer summary DataFrame

    Returns:
        dict: {check_name: bool} for each check

    Raises:
        ValueError: if any critical check fails
    """

    checks = {
        "no_null_customer_id": bool(df["customer_id"].notna().all()),
        "no_null_customer_name": bool(df["customer_name"].notna().all()),
        "positive_revenue": bool((df["total_revenue"] > 0).all()),
        "no_duplicate_customer_id": bool(df["customer_id"].is_unique),
        "positive_total_orders": bool((df["total_orders"] > 0).all()),
    }

    for name, result in checks.items():
        logger.info(f"{name}: {'PASS' if result else 'FAIL'}")

    if not all(checks.values()):
        raise ValueError("Data validation failed")

    return checks


def load(df, engine, csv_path):
    """Load customer summary to PostgreSQL table and CSV file.

    Args:
        df: validated customer summary DataFrame
        engine: SQLAlchemy engine
        csv_path: path for CSV output
    """

    os.makedirs(os.path.dirname(csv_path), exist_ok=True)

    df.to_sql("customer_analytics", engine, if_exists="replace", index=False)
    df.to_csv(csv_path, index=False)

    logger.info(f"Loaded {len(df)} rows")


# =========================
# Quality Report (Tier 1)
# =========================
def generate_quality_report(df, checks):
    """Generate JSON data quality report."""

    outliers = df[df["is_outlier"]][["customer_id", "total_revenue"]]

    report = {
        "timestamp": datetime.now().isoformat(),
        "total_records": len(df),
        "checks": checks,
        "outliers": outliers.to_dict(orient="records"),
    }

    os.makedirs("output", exist_ok=True)

    with open("output/quality_report.json", "w") as f:
        json.dump(report, f, indent=4)

    logger.info("Quality report generated")


def main():
    """Orchestrate the ETL pipeline: extract -> transform -> validate -> load."""

    engine = create_engine(
        "postgresql+psycopg://postgres:postgres@localhost:5432/amman_market"
    )

    start_time = datetime.now()

    try:
        # Extract
        data = extract(engine)

        # Transform
        summary = transform(data)

        # Validate
        checks = validate(summary)

        # Generate report (Tier 1)
        generate_quality_report(summary, checks)

        # Load
        load(summary, engine, "output/customer_analytics.csv")

        end_time = datetime.now()

        # Log metadata (Tier 2)
        log_etl_run(engine, start_time, end_time, len(summary), "success")

        logger.info("ETL completed successfully")

    except Exception as e:
        log_etl_run(engine, start_time, datetime.now(), 0, "failed")
        logger.error(f"ETL failed: {e}")


if __name__ == "__main__":
    main()

# """ETL Pipeline — Amman Digital Market Customer Analytics"""

# from sqlalchemy import create_engine
# import pandas as pd
# import os


# # =========================
# # EXTRACT
# # =========================
# def extract(engine):
#     tables = ["customers", "products", "orders", "order_items"]
#     data = {}

#     for table in tables:
#         df = pd.read_sql(f"SELECT * FROM {table}", engine)
#         print(f"[EXTRACT] {table}: {len(df)} rows")
#         data[table] = df

#     return data


# =========================
# TRANSFORM
# =========================
# def transform(data_dict):

#     customers = data_dict["customers"]
#     products = data_dict["products"]
#     orders = data_dict["orders"]
#     order_items = data_dict["order_items"]

#     # Join all tables
#     df = (
#         order_items.merge(orders, on="order_id", how="inner")
#         .merge(products, on="product_id", how="inner")
#         .merge(customers, on="customer_id", how="left")
#     )

#     print(f"[TRANSFORM] After joins: {len(df)} rows")

#     # -----------------------
#     # Cleaning
#     # -----------------------
#     before = len(df)

#     df = df[df["status"] != "cancelled"]
#     df = df[df["quantity"] <= 100]

#     print(f"[TRANSFORM] Removed {before - len(df)} invalid rows")

#     # -----------------------
#     # Feature Engineering
#     # -----------------------
#     df["line_total"] = df["quantity"] * df["unit_price"]

#     # -----------------------
#     # Aggregation
#     # -----------------------
#     summary = (
#         df.groupby("customer_id")
#         .agg(total_orders=("order_id", "nunique"), total_revenue=("line_total", "sum"))
#         .reset_index()
#     )

#     summary["avg_order_value"] = summary["total_revenue"] / summary["total_orders"]

#     # Add customer_name + city (FIXED HERE)
#     summary = summary.merge(
#         customers[["customer_id", "customer_name", "city"]],
#         on="customer_id",
#         how="left",
#     )

#     # -----------------------
#     # Top category
#     # -----------------------
#     category_revenue = (
#         df.groupby(["customer_id", "category"])["line_total"].sum().reset_index()
#     )

#     top_category = (
#         category_revenue.sort_values(
#             ["customer_id", "line_total"], ascending=[True, False]
#         )
#         .drop_duplicates("customer_id")
#         .rename(columns={"category": "top_category"})
#     )

#     summary = summary.merge(
#         top_category[["customer_id", "top_category"]], on="customer_id", how="left"
#     )

#     print(f"[TRANSFORM] Final rows: {len(summary)}")

#     return summary


# # =========================
# # VALIDATE
# # =========================
# def validate(df):

#     checks = {
#         "no_null_customer_id": df["customer_id"].notna().all(),
#         "no_null_customer_name": df["customer_name"].notna().all(),
#         "positive_revenue": (df["total_revenue"] > 0).all(),
#         "no_duplicate_customer_id": df["customer_id"].is_unique,
#         "positive_total_orders": (df["total_orders"] > 0).all(),
#     }

#     print("[VALIDATE] Running checks...")

#     for name, result in checks.items():
#         print(f"[{'PASS' if result else 'FAIL'}] {name}")

#     if not all(checks.values()):
#         raise ValueError("Data validation failed!")

#     print("[VALIDATE] All checks passed ✅")
#     return checks


# # =========================
# # LOAD
# # =========================
# def load(df, engine, csv_path):

#     os.makedirs(os.path.dirname(csv_path), exist_ok=True)

#     df.to_sql("customer_analytics", engine, if_exists="replace", index=False)

#     df.to_csv(csv_path, index=False)

#     print(f"[LOAD] Loaded {len(df)} rows")
#     print(f"[LOAD] CSV saved to: {csv_path}")


# # =========================
# # MAIN
# # =========================
# def main():

#     engine = create_engine(
#         "postgresql+psycopg://postgres:postgres@localhost:5432/amman_market"
#     )

#     print("🚀 Starting ETL pipeline...\n")

#     # Extract
#     print("Step 1: Extract")
#     data = extract(engine)
#     print()

#     # Transform
#     print("Step 2: Transform")
#     summary = transform(data)
#     print()

#     # Validate
#     print("Step 3: Validate")
#     validate(summary)
#     print()

#     # Load
#     print("Step 4: Load")
#     load(summary, engine, "output/customer_analytics.csv")
#     print()

#     print("✅ ETL pipeline completed successfully.")


# # =========================
# # ENTRY POINT
# # =========================
# if __name__ == "__main__":
#     main()
