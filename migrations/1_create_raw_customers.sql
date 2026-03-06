CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id INT,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    city TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source TEXT
);
