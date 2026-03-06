CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.order_items (
    order_item_id INT,
    order_id INT,
    product_id INT,
    quantity INT,
    price NUMERIC,
    updated_at TIMESTAMP,

    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source TEXT
);
