--About Heatmaps
--Heatmaps in Snowflake help visualize trends by showing patterns in sales, customer behavior, and product performance.
--Generate the heeatmap by setting the X-axis, Y-axis, and color intensity based on the data.
--To identifying Sales Trends Over Time.
--Which months have the highest sales and revenue.
--Trend Analysis in Heatmap
    --If color intensity increases over time, sales are growing.
    --If some months are darker (higher revenue/orders), it shows seasonal trends.
    --If revenue fluctuates, it suggests demand varies throughout the year.
--Snowsight Heatmap Setup
    --X-axis: month
    --Y-axis: total_revenue
    --Color Intensity: Revenue
SELECT 
    DATE_TRUNC('month', purchase_date) AS month,
    COUNT(*) AS total_orders,
    SUM(total_price) AS total_revenue
FROM transformed.clean_sales
GROUP BY month
ORDER BY month;

--Product Performance Trends.
--Which products are performing well over time.
--Trend Analysis in Heatmap:
    --If certain categories consistently have dark color, they are top-selling.
    --If some products spike in revenue in specific months, they might be seasonal.
    --A drop in color intensity might indicate declining interest in a product.
--Snowsight Heatmap Setup
    --X-axis: product_name
    --Y-axis: product_category
    --Color Intensity: Revenue
SELECT 
    p.product_name,
    p.product_category,
    SUM(s.total_price) AS total_revenue,
    SUM(s.quantity) AS total_quantity_sold
FROM transformed.clean_sales s
JOIN transformed.product_categories p ON s.product_id = p.product_id
GROUP BY p.product_name, p.product_category
ORDER BY total_revenue DESC;

--Location-Based Sales Trends
--Which locations contribute the most revenue and how it changes over time.
--Trend Analysis in Heatmap
    --If some locations consistently have darker colors, they drive the most sales.
    --A change in color intensity over months shows regions with increasing or decreasing demand.
    --New dark spots might indicate new emerging markets.
--Snowsight Heatmap Setup
    --X-axis: location
    --Y-axis: total_orders or total_revenue
    --Color Intensity: Revenue
SELECT 
    DATE_TRUNC('month', purchase_date) AS month,
    location,
    SUM(total_price) AS total_revenue
FROM transformed.clean_sales
GROUP BY month, location
ORDER BY month, total_revenue DESC;

--Device & Payment Method Trends
--Identify purchasing behavior trends by device and payment method.
--Trend Analysis in Heatmap
    --If mobile device sales increase over months, mobile shopping is growing.
    --If cash payments decline and online transactions rise, digital payments are becoming more popular.
    --Identifying which devices dominate sales per period helps in marketing strategies.
--Snowsight Heatmap Setup
    --X-axis: month
    --Y-axis: product_category
    --Color Intensity: Revenue
SELECT 
    DATE_TRUNC('month', s.purchase_date) AS month,
    p.product_category,
    SUM(s.total_price) AS total_revenue
FROM transformed.clean_sales s
JOIN transformed.product_categories p ON s.product_id = p.product_id
GROUP BY month, p.product_category
ORDER BY month, total_revenue DESC;

--Snowsight Heatmap Setup
    --X-axis: device_type
    --Y-axis: purchasing_type
    --Color Intensity: Revenue
SELECT 
    device_type,
    purchasing_type,
    COUNT(*) AS total_orders,
    SUM(total_price) AS total_revenue
FROM transformed.clean_sales
GROUP BY device_type, purchasing_type
ORDER BY total_revenue DESC;

--Customer Segmentation Trends
--Track how customer behavior changes over time.
--Trend Analysis in Heatmap:
    --If the number of "High Value" customers increases over months, the business is gaining loyal users.
    --If the "Churned" segment grows, customer retention is declining.
    --Seasonal spikes may indicate customers spending more during holidays.
--Snowsight Heatmap Setup
    --X-axis:month
    --Y-axis:customer_value_segment 
    --Color Intensity:customer_count 
WITH customer_metrics AS (
    SELECT 
        user_id,
        COUNT(*) AS total_orders,
        SUM(total_price) AS total_spent,
        MAX(purchase_date) AS last_purchase_date
    FROM transformed.clean_sales
    GROUP BY user_id
)
SELECT 
    DATE_TRUNC('month', last_purchase_date) AS month,
    CASE 
        WHEN total_spent > 1000 THEN 'High Value'
        WHEN total_spent > 500 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS customer_value_segment,
    COUNT(*) AS customer_count
FROM customer_metrics
GROUP BY month, customer_value_segment
ORDER BY month, customer_value_segment;

