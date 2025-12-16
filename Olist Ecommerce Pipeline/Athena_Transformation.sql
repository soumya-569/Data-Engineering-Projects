
-- I haven't done much things here, most of the things I did in Databricks & later in Power BI Dax

------------------------
--Initialize Environemnt
------------------------

CREATE DATABASE test;

------------------------------
--Customer With Location Table
------------------------------

CREATE EXTERNAL TABLE test.customer_geo
(
    customer_id VARCHAR(100),
    customer_city VARCHAR(50),
    customer_state VARCHAR(10),
    customer_zip_code_prefix VARCHAR(10),
    customer_lat DOUBLE,
    customer_lng DOUBLE
)
STORED AS PARQUET
LOCATION 's3://amz-s3-databricks-conn/Silver/customer_geo/'
TBLPROPERTIES('parquet.compress'='SNAPPY');

SELECT * FROM customer_geo;
DESCRIBE customer_geo;
DROP TABLE customer_geo;

---------------------
--Customer Table Only
---------------------
CREATE EXTERNAL TABLE test.customers
(
    customer_id VARCHAR(100),
    customer_unique_id VARCHAR(100),
    customer_city VARCHAR(50),
    customer_state VARCHAR(10),
    customer_zip_code_prefix VARCHAR(10)
)
STORED AS PARQUET
LOCATION 's3://amz-s3-databricks-conn/Silver/customer/'
TBLPROPERTIES('parquet.compress'='SNAPPY');

SELECT * FROM customers;
DESCRIBE customers;

--------------------
--Geo Location Table
--------------------

CREATE EXTERNAL TABLE test.geo_location
(
    geolocation_zip_code_prefix VARCHAR(10),
    geolocation_lat FLOAT,
    geolocation_lng FLOAT,
    geolocation_city VARCHAR(50),
    geolocation_state VARCHAR(10)
)
STORED AS PARQUET
LOCATION 's3://amz-s3-databricks-conn/Silver/geo/'
TBLPROPERTIES('parquet.compress'='SNAPPY');

SELECT * FROM geo_location;
DESCRIBE geo_location;
----------------------------------
--Create Order With Location Table
----------------------------------

CREATE EXTERNAL TABLE test.order_geo
(
    seller_id VARCHAR(100),
    customer_id VARCHAR(100),
    order_id VARCHAR(100),
    order_status VARCHAR(20),
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    order_item_id INT,
    product_id VARCHAR(100),
    shipping_limit_date TIMESTAMP,
    price FLOAT,
    freight_value FLOAT,
    shipping_date DATE,
    shipping_time VARCHAR(25),
    estimate_cost_price DOUBLE,
    profit_margin DOUBLE,
    margin_percent DOUBLE,
    customer_city VARCHAR(50),
    customer_state VARCHAR(10),
    customer_zip_code_prefix VARCHAR(10),
    customer_lat DOUBLE,
    customer_lng DOUBLE,
    seller_city VARCHAR(50),
    seller_state VARCHAR(10),
    seller_zip_code_prefix VARCHAR(10),
    seller_lat DOUBLE,
    seller_lng DOUBLE,
    distance FLOAT,
    freight_per_km DOUBLE
)
STORED AS PARQUET
LOCATION 's3://amz-s3-databricks-conn/Silver/order_geo/'
TBLPROPERTIES('parquet.compress'='SNAPPY');

SELECT * FROM order_geo;
DESCRIBE order_geo;

--------------------------
--Create Order Items Table
--------------------------

CREATE EXTERNAL TABLE test.order_item (
  order_id VARCHAR(100),
  order_item_id INT,
  product_id VARCHAR(100),
  seller_id VARCHAR(100),
  shipping_limit_date TIMESTAMP,
  price FLOAT,
  freight_value FLOAT,
  shipping_date DATE,
  shipping_time VARCHAR(25),
  estimate_cost_price DOUBLE,
  profit_margin DOUBLE,
  margin_percent DOUBLE
)
STORED AS PARQUET
LOCATION 's3://amz-s3-databricks-conn/Silver/order_item/'
TBLPROPERTIES('parquet.compress'='SNAPPY');

SELECT * FROM order_item;
DESCRIBE order_item;

---------------------------------------------
--Create Order Table With Delivery Enrichment
---------------------------------------------

CREATE EXTERNAL TABLE test.order_enriched
(
    order_id VARCHAR(100),
    customer_id VARCHAR(100),
    order_status VARCHAR(25),
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    delivery_days INT,
    delay_days INT,
    delivery_status VARCHAR(20),
    order_month INT,
    order_year INT,
    order_quarter INT,
    is_festive_season BOOLEAN
)
STORED AS PARQUET
LOCATION 's3://amz-s3-databricks-conn/Silver/order/'
TBLPROPERTIES('parquet.compress'='SNAPPY');

SELECT * FROM order_enriched;
DESCRIBE order_enriched;

-----------------------------
--Create Complete Order Table
-----------------------------

CREATE EXTERNAL TABLE test.complete_orders
(
    order_id VARCHAR(100),
    customer_id VARCHAR(100),
    order_status VARCHAR(25),
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    order_item_id INT,
    product_id VARCHAR(100),
    seller_id VARCHAR(100),
    shipping_limit_date TIMESTAMP,
    price FLOAT,
    freight_value FLOAT,
    shipping_date DATE,
    shipping_time VARCHAR(25),
    estimate_cost_price DOUBLE,
    profit_margin DOUBLE,
    margin_percent DOUBLE
)
STORED AS PARQUET
LOCATION 's3://amz-s3-databricks-conn/Silver/orders_whole/'
TBLPROPERTIES('parquet.compress'='SNAPPY');

SELECT * FROM complete_orders;
DESCRIBE complete_orders;

----------------------
--Create Payment Table
----------------------

CREATE EXTERNAL TABLE test.payment
(
    order_id VARCHAR(100),
    payment_sequential INT,
    payment_type VARCHAR(50),
    payment_installments INT,
    payment_value FLOAT
)
STORED AS PARQUET
LOCATION 's3://amz-s3-databricks-conn/Silver/payment/'
TBLPROPERTIES('parquet.compress'='SNAPPY');

SELECT * FROM payment;
DESCRIBE payment;

----------------------------------------
--Create Products Table With Translation
----------------------------------------

CREATE EXTERNAL TABLE test.products
(
    product_category_name VARCHAR(60),
    product_id VARCHAR(100),
    product_name_lenght INT,
    product_description_lenght INT,
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT,
    product_category_name_english VARCHAR(60)
)
STORED AS PARQUET
LOCATION 's3://amz-s3-databricks-conn/Silver/products/'
TBLPROPERTIES('parquet.compress'='SNAPPY');

SELECT * FROM products;
DESCRIBE products;

----------------------
--Create Reviews Table
----------------------

CREATE EXTERNAL TABLE test.reviews
(
    review_id VARCHAR(100),
    order_id VARCHAR(100),
    review_score INT,
    review_comment_title STRING,
    review_comment_message STRING,
    review_creation_date TIMESTAMP,
    review_answer_timestamp TIMESTAMP,
    sentiment_score FLOAT
)
STORED AS PARQUET
LOCATION 's3://amz-s3-databricks-conn/Silver/reviews/'
TBLPROPERTIES('parquet.compress'='SNAPPY');

SELECT * FROM reviews;
DESCRIBE reviews;

------------------
--Create RFM Table
------------------

CREATE EXTERNAL TABLE test.rfm
(
    customer_id VARCHAR(100),
    last_purchase_date TIMESTAMP,
    frequency INT,
    monetary DOUBLE,
    recency INT
)
STORED AS PARQUET
LOCATION 's3://amz-s3-databricks-conn/Silver/rfm/'
TBLPROPERTIES('parquet.compress'='SNAPPY');

SELECT * FROM rfm;
DESCRIBE rfm;

-----------------------------------
--Create Seller With Location Table
-----------------------------------

CREATE EXTERNAL TABLE test.seller_geo
(
    seller_id VARCHAR(100),
    seller_city VARCHAR(25),
    seller_state VARCHAR(10),
    seller_zip_code_prefix VARCHAR(25),
    seller_lat DOUBLE,
    seller_lng DOUBLE
)
STORED AS PARQUET
LOCATION 's3://amz-s3-databricks-conn/Silver/seller_geo/'
TBLPROPERTIES('parquet.compress'='SNAPPY');

SELECT * FROM seller_geo;
DESCRIBE seller_geo;

---------------------
--Create Seller Table
---------------------

CREATE EXTERNAL TABLE test.seller
(
    seller_id VARCHAR(100),
    seller_zip_code_prefix VARCHAR(25),
    seller_city VARCHAR(25),
    seller_state VARCHAR(10)
)
STORED AS PARQUET
LOCATION 's3://amz-s3-databricks-conn/Silver/seller/'
TBLPROPERTIES('parquet.compress'='SNAPPY');

SELECT * FROM seller;
DESCRIBE seller;

----------------
--Outer Analysis
----------------

CREATE TABLE reviews_enriched
WITH
(
    format = 'PARQUET',
    external_location = 's3://amz-s3-databricks-conn/athena_updated_tables/reviews_enriched/'
) AS
SELECT *,
CASE
    WHEN review_score > 3 THEN 0
    WHEN review_score = 3 THEN 40
    WHEN review_score = 2 THEN 50
    WHEN review_score = 1 THEN 100
END AS reputation_cost
FROM reviews;

SELECT * FROM reviews_enriched;

SELECT MIN(order_purchase_timestamp),MAX(order_purchase_timestamp) FROM order_enriched;
SELECT * FROM order_enriched;
SELECT COUNT( DISTINCT seller_id) FROM order_geo WHERE YEAR(order_purchase_timestamp) = 2016;

CREATE TABLE orders_loc
WITH
(
    format = 'PARQUET',
    external_location = 's3://amz-s3-databricks-conn/athena_updated_tables/orders_loc/'
) AS
SELECT *,
price - (price*0.9) AS olist_commison,
DATE(order_purchase_timestamp) AS order_date
FROM order_geo;

SELECT * FROM orders_loc;
