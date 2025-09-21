CREATE SCHEMA IF NOT EXISTS dw;

-- 1. Orders
CREATE TABLE dw.fact_orders (
    order_id VARCHAR PRIMARY KEY,
    customer_id VARCHAR,
    order_status VARCHAR,
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);

-- 2. Customers
CREATE TABLE dw.dim_customers (
    customer_id VARCHAR PRIMARY KEY,
    customer_unique_id VARCHAR,
    customer_zip_code_prefix VARCHAR,
    customer_city VARCHAR,
    customer_state VARCHAR
);

-- 3. Order Items
CREATE TABLE dw.fact_order_items (
    order_id VARCHAR,
    order_item_id INT,
    product_id VARCHAR,
    seller_id VARCHAR,
    shipping_limit_date TIMESTAMP,
    price NUMERIC(10,2),
    freight_value NUMERIC(10,2),
    PRIMARY KEY(order_id, order_item_id)
);

-- 4. Payments
CREATE TABLE dw.fact_payments (
    order_id VARCHAR,
    payment_sequential INT,
    payment_type VARCHAR,
    payment_installments INT,
    payment_value NUMERIC(10,2),
    order_purchase_timestamp TIMESTAMP
);

-- 5. Reviews
CREATE TABLE dw.fact_reviews (
    review_id VARCHAR PRIMARY KEY,
    order_id VARCHAR,
    review_score INT,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date TIMESTAMP,
    review_answer_timestamp TIMESTAMP
);

-- 6. Products
CREATE TABLE dw.dim_products (
    product_id VARCHAR PRIMARY KEY,
    product_category VARCHAR,
    product_name_length INT,
    product_description_length INT,
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT
);

-- 7. Sellers
CREATE TABLE dw.dim_sellers (
    seller_id VARCHAR PRIMARY KEY,
    seller_zip_code_prefix VARCHAR,
    seller_city VARCHAR,
    seller_state VARCHAR
);

-- 8. Geolocation	
CREATE TABLE dw.dim_geography (
    geolocation_zip_code_prefix VARCHAR,
    geolocation_lat NUMERIC(10,6),
    geolocation_lng NUMERIC(10,6),
    geolocation_city VARCHAR,
    geolocation_state VARCHAR
);

-- 8. Sales	
CREATE TABLE dw.fact_sales (
    order_id VARCHAR PRIMARY KEY,
    customer_id VARCHAR,
    order_date DATE,
    total_price NUMERIC(10,2),
    total_freight_value NUMERIC(10,2),
    item_count INT,
    total_payment_value NUMERIC(10,2),
    payment_method_count INT,
    review_score INT
);

