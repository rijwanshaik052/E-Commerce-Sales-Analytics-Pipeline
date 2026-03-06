CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.orders (
    order_id INT,
    customer_id INT,
    order_status TEXT,
    order_date TIMESTAMP,
    updated_at TIMESTAMP,

    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source TEXT
);
