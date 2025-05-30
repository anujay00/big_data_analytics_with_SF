-- Create a schema for transformed data
CREATE SCHEMA IF NOT EXISTS transformed;
USE SCHEMA transformed;

-- Create a cleaned and transformed version of the data
CREATE OR REPLACE TABLE clean_sales AS
WITH data_validation AS (
    SELECT
        indexid,
        user_id,
        -- Standardize gender values
        CASE 
            WHEN UPPER(gender) IN ('M', 'MALE') THEN 'Male'
            WHEN UPPER(gender) IN ('F', 'FEMALE') THEN 'Female'
            ELSE 'Other'
        END AS gender,
        -- Standardize location names
        INITCAP(TRIM(location)) AS location,
        product_id,
        product_name,
        stock_date,
        -- Ensure unit_price is positive
        ABS(unit_price) AS unit_price,
        -- Ensure quantity is at least 1
        CASE WHEN quantity < 1 THEN 1 ELSE quantity END AS quantity,
        -- Recalculate total_price for consistency
        CASE WHEN quantity < 1 THEN ABS(unit_price) ELSE ABS(unit_price) * quantity END AS total_price,
        -- Standardize device types
        CASE
            WHEN LOWER(device_type) LIKE '%mobile%' OR LOWER(device_type) LIKE '%phone%' THEN 'Mobile'
            WHEN LOWER(device_type) LIKE '%tablet%' THEN 'Tablet'
            WHEN LOWER(device_type) LIKE '%desktop%' OR LOWER(device_type) LIKE '%pc%' THEN 'Desktop'
            ELSE 'Other'
        END AS device_type,
        -- Standardize purchasing types
        CASE
            WHEN LOWER(purchasing_type) LIKE '%credit%' THEN 'Credit Card'
            WHEN LOWER(purchasing_type) LIKE '%debit%' THEN 'Debit Card'
            WHEN LOWER(purchasing_type) LIKE '%cash%' THEN 'Cash'
            WHEN LOWER(purchasing_type) LIKE '%online%' OR LOWER(purchasing_type) LIKE '%pay%' THEN 'Online Payment'
            ELSE purchasing_type
        END AS purchasing_type,
        purchase_date,
        -- Add derived columns
        DATEDIFF('day', stock_date, purchase_date) AS days_in_stock,
        DAYNAME(purchase_date) AS purchase_day,
        MONTHNAME(purchase_date) AS purchase_month,
        YEAR(purchase_date) AS purchase_year,
        CASE WHEN purchase_date = CURRENT_DATE() THEN 1 ELSE 0 END AS is_today_purchase
    FROM TEST5.STREAMING.RAW_DATA
    WHERE product_id IS NOT NULL -- Filter out rows with missing product_id
)
SELECT * FROM data_validation;

-- Create product category table by extracting categories from product names
CREATE OR REPLACE TABLE product_categories AS
SELECT DISTINCT
    product_id,
    product_name,
    -- Extract category from product name (this is a simplified example)
    CASE
        WHEN LOWER(product_name) LIKE '%Lightning Cable%' THEN 'Cables'
        WHEN LOWER(product_name) LIKE '%Power Adapter20W%' THEN 'Adapters'
        WHEN LOWER(product_name) LIKE '%TypeC Cable%' THEN 'Cables'
        WHEN LOWER(product_name) LIKE '%Power Bank%' THEN 'Electronics'
        WHEN LOWER(product_name) LIKE '%Smart Watch%' THEN 'Electronics'
        ELSE 'Other'
    END AS product_category
FROM clean_sales;

-- Create a table for user demographics
CREATE OR REPLACE TABLE user_demographics AS
SELECT 
    user_id,
    gender,
    location,
    COUNT(DISTINCT purchase_date) AS purchase_days,
    MIN(purchase_date) AS first_purchase_date,
    MAX(purchase_date) AS last_purchase_date,
    SUM(total_price) AS total_spent,
    AVG(total_price) AS avg_purchase_amount,
    LISTAGG(DISTINCT device_type, ', ') AS used_devices,
    LISTAGG(DISTINCT purchasing_type, ', ') AS payment_methods
FROM clean_sales
GROUP BY user_id, gender, location;