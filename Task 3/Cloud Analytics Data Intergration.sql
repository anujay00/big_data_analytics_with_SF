-- Sales Trends Analysis
CREATE OR REPLACE VIEW sales_trends AS
SELECT 
    DATE_TRUNC('month', purchase_date) AS month,
    COUNT(*) AS total_orders,
    COUNT(DISTINCT user_id) AS unique_customers,
    SUM(total_price) AS total_revenue,
    AVG(total_price) AS avg_order_value,
    SUM(quantity) AS total_items_sold,
    SUM(total_price) / COUNT(DISTINCT user_id) AS revenue_per_customer
FROM transformed.clean_sales
GROUP BY month
ORDER BY month;


--Product Performance Analysis
CREATE OR REPLACE VIEW product_performance AS
SELECT 
    p.product_id,
    p.product_name,
    p.product_category,
    COUNT(*) AS total_orders,
    SUM(s.quantity) AS total_quantity_sold,
    SUM(s.total_price) AS total_revenue,
    AVG(s.unit_price) AS avg_unit_price,
    MIN(s.purchase_date) AS first_purchase_date,
    MAX(s.purchase_date) AS last_purchase_date,
    DATEDIFF('day', MIN(s.purchase_date), MAX(s.purchase_date)) AS sales_period_days,
    SUM(s.total_price) / NULLIF(DATEDIFF('day', MIN(s.purchase_date), MAX(s.purchase_date)), 0) AS revenue_per_day
FROM transformed.clean_sales s
JOIN transformed.product_categories p ON s.product_id = p.product_id
GROUP BY p.product_id, p.product_name, p.product_category
ORDER BY total_revenue DESC;

--Location-based Analysis
CREATE OR REPLACE VIEW location_analysis AS
SELECT 
    location,
    COUNT(DISTINCT user_id) AS unique_customers,
    COUNT(*) AS total_orders,
    SUM(total_price) AS total_revenue,
    AVG(total_price) AS avg_order_value,
    SUM(total_price) / COUNT(DISTINCT user_id) AS revenue_per_customer,
    SUM(CASE WHEN purchasing_type = 'Cash' THEN total_price ELSE 0 END) / NULLIF(SUM(total_price), 0) * 100 AS cash_payment_percentage
FROM transformed.clean_sales
GROUP BY location
ORDER BY total_revenue DESC;

--Device and Purchasing Type Analysis
CREATE OR REPLACE VIEW device_purchasing_analysis AS
SELECT 
    device_type,
    purchasing_type,
    COUNT(*) AS total_orders,
    COUNT(DISTINCT user_id) AS unique_customers,
    SUM(total_price) AS total_revenue,
    AVG(total_price) AS avg_order_value,
    SUM(quantity) AS total_items_sold
FROM transformed.clean_sales
GROUP BY device_type, purchasing_type
ORDER BY total_revenue DESC;

--Time-based Analysis
CREATE OR REPLACE VIEW time_analysis AS
SELECT 
    purchase_day,
    purchase_month,
    purchase_year,
    COUNT(*) AS total_orders,
    SUM(total_price) AS total_revenue,
    AVG(total_price) AS avg_order_value
FROM transformed.clean_sales
GROUP BY purchase_day, purchase_month, purchase_year
ORDER BY total_revenue DESC;

--Customer Segmentation
CREATE OR REPLACE VIEW customer_segments AS
WITH customer_metrics AS (
    SELECT 
        user_id,
        gender,
        location,
        COUNT(*) AS total_orders,
        SUM(total_price) AS total_spent,
        AVG(total_price) AS avg_order_value,
        COUNT(DISTINCT purchase_date) AS purchase_days,
        MAX(purchase_date) AS last_purchase_date,
        DATEDIFF('day', MIN(purchase_date), MAX(purchase_date)) AS customer_tenure_days
    FROM transformed.clean_sales
    GROUP BY user_id, gender, location
)
SELECT 
    user_id,
    gender,
    location,
    total_orders,
    total_spent,
    avg_order_value,
    purchase_days,
    last_purchase_date,
    customer_tenure_days,
    CASE 
        WHEN total_spent > 1000 AND purchase_days > 5 THEN 'High Value'
        WHEN total_spent > 500 OR purchase_days > 3 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS customer_value_segment,
    CASE
        WHEN DATEDIFF('day', last_purchase_date, CURRENT_DATE()) < 30 THEN 'Active'
        WHEN DATEDIFF('day', last_purchase_date, CURRENT_DATE()) < 90 THEN 'At Risk'
        ELSE 'Churned'
    END AS activity_status
FROM customer_metrics;