CREATE TABLE raw.customers (

    customer_id TEXT,
    customer_unique_id TEXT,
    customer_zip_code_prefix INT,
    customer_city TEXT,
    customer_state TEXT,

    source_file TEXT,
    batch_id INT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE raw.orders (
    order_id TEXT,
    customer_id TEXT,
    order_status TEXT,
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,

    source_file TEXT,
    batch_id INT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE raw.order_items (
    order_id TEXT,
    order_item_id INT,
    product_id TEXT,
    seller_id TEXT,
    shipping_limit_date TIMESTAMP,
    price NUMERIC,
    freight_value NUMERIC,

    source_file TEXT,
    batch_id INT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE raw.order_payments (
    order_id TEXT,
    payment_sequential INT,
    payment_type TEXT,
    payment_installments INT,
    payment_value NUMERIC,

    source_file TEXT,
    batch_id INT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE raw.order_reviews (
    review_id TEXT,
    order_id TEXT,
    review_score INT,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date TIMESTAMP,
    review_answer_timestamp TIMESTAMP,

    source_file TEXT,
    batch_id INT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE raw.products (
    product_id TEXT,
    product_category_name TEXT,
    product_name_lenght INT,
    product_description_lenght INT,
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT,

    source_file TEXT,
    batch_id INT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE raw.sellers (
    seller_id TEXT,
    seller_zip_code_prefix INT,
    seller_city TEXT,
    seller_state TEXT,

    source_file TEXT,
    batch_id INT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE raw.geolocation (
    geolocation_zip_code_prefix INT,
    geolocation_lat NUMERIC,
    geolocation_lng NUMERIC,
    geolocation_city TEXT,
    geolocation_state TEXT,

    source_file TEXT,
    batch_id INT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE raw.product_category_translation (
    product_category_name TEXT,
    product_category_name_english TEXT,

    source_file TEXT,
    batch_id INT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


