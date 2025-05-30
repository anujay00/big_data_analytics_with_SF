--Create a database, schema, and warehouse
CREATE SCHEMA streaming;
USE SCHEMA streaming;
USE WAREHOUSE COMPUTE_WH;

--Create a table for your dataset
CREATE TABLE raw_data (
    indexid NUMBER,
    user_id VARCHAR,
    gender VARCHAR,
    location VARCHAR,
    product_id VARCHAR,
    product_name VARCHAR,
    stock_date DATE,
    unit_price NUMBER,
    quantity NUMBER,
    total_price NUMBER,
    device_type VARCHAR,
    purchasing_type VARCHAR,
    purchase_date DATE
    
);

--Create a stream to track changes on the table
CREATE STREAM raw_data_stream ON TABLE raw_data;

SELECT * FROM raw_data;