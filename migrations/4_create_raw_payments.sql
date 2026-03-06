CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.payments (
    payment_id INT,
    order_id INT,
    payment_method TEXT,
    payment_status TEXT,
    amount NUMERIC,
    payment_date TIMESTAMP,
    updated_at TIMESTAMP,

    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source TEXT
);
