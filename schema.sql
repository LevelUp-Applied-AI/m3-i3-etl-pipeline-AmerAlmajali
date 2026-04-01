-- Amman Digital Market — Database Schema
-- Module 3 Integration Task: ETL Pipeline

CREATE TABLE customers (
    customer_id    SERIAL PRIMARY KEY,
    customer_name  VARCHAR(100) NOT NULL,
    email          VARCHAR(150) UNIQUE NOT NULL,
    city           VARCHAR(50),
    registration_date DATE NOT NULL
);

CREATE TABLE products (
    product_id   SERIAL PRIMARY KEY,
    product_name VARCHAR(150) NOT NULL,
    category     VARCHAR(50) NOT NULL,
    unit_price   NUMERIC(10, 2) NOT NULL CHECK (unit_price > 0)
);

CREATE TABLE orders (
    order_id    SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(customer_id),
    order_date  DATE NOT NULL,
    status      VARCHAR(20) NOT NULL CHECK (status IN ('completed', 'shipped', 'processing', 'cancelled'))
);

CREATE TABLE order_items (
    item_id    SERIAL PRIMARY KEY,
    order_id   INTEGER NOT NULL REFERENCES orders(order_id),
    product_id INTEGER NOT NULL REFERENCES products(product_id),
    quantity   INTEGER NOT NULL CHECK (quantity > 0)
);
CREATE TABLE etl_metadata (
    run_id SERIAL PRIMARY KEY,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    rows_processed INT,
    status VARCHAR(20)
);