"""Tests for the ETL pipeline.

Write at least 3 tests:
1. test_transform_filters_cancelled — cancelled orders excluded after transform
2. test_transform_filters_suspicious_quantity — quantities > 100 excluded
3. test_validate_catches_nulls — validate() raises ValueError on null customer_id
"""

import pandas as pd
import pytest
from etl_pipeline import transform, validate


def test_transform_filters_cancelled():
    """Create test DataFrames with a cancelled order. Confirm it's excluded."""

    customers = pd.DataFrame(
        {"customer_id": [1], "customer_name": ["Ali"], "city": ["Amman"]}
    )

    products = pd.DataFrame(
        {"product_id": [1], "category": ["Electronics"], "unit_price": [100]}
    )

    orders = pd.DataFrame(
        {
            "order_id": [1],
            "customer_id": [1],
            "status": ["cancelled"],  # should be filtered
        }
    )

    order_items = pd.DataFrame({"order_id": [1], "product_id": [1], "quantity": [2]})

    data = {
        "customers": customers,
        "products": products,
        "orders": orders,
        "order_items": order_items,
    }

    result = transform(data)

    assert result.empty


def test_transform_filters_suspicious_quantity():
    """Create test DataFrames with quantity > 100. Confirm it's excluded."""

    customers = pd.DataFrame(
        {"customer_id": [1], "customer_name": ["Ali"], "city": ["Amman"]}
    )

    products = pd.DataFrame(
        {"product_id": [1], "category": ["Electronics"], "unit_price": [100]}
    )

    orders = pd.DataFrame(
        {"order_id": [1], "customer_id": [1], "status": ["completed"]}
    )

    order_items = pd.DataFrame(
        {
            "order_id": [1],
            "product_id": [1],
            "quantity": [150],  # >100 → should be filtered
        }
    )

    data = {
        "customers": customers,
        "products": products,
        "orders": orders,
        "order_items": order_items,
    }

    result = transform(data)

    assert result.empty


def test_validate_catches_nulls():
    """Create a DataFrame with null customer_id. Confirm validate() raises ValueError."""

    df = pd.DataFrame(
        {
            "customer_id": [None],  # invalid
            "customer_name": ["Ali"],
            "total_orders": [1],
            "total_revenue": [100],
            "avg_order_value": [100],
            "top_category": ["Electronics"],
        }
    )

    with pytest.raises(ValueError):
        validate(df)
