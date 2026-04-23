-- 1. Monthly Revenue Growth Rate (Using Window Functions)
WITH MonthlyRevenue AS (
    SELECT 
        order_month,
        SUM(total_price_inr) as monthly_revenue
    FROM processed_retail_data
    GROUP BY order_month
)
SELECT 
    order_month,
    monthly_revenue,
    LAG(monthly_revenue) OVER (ORDER BY order_month) as prev_month_revenue,
    (monthly_revenue - LAG(monthly_revenue) OVER (ORDER BY order_month)) / LAG(monthly_revenue) OVER (ORDER BY order_month) * 100 as growth_percentage
FROM MonthlyRevenue;

-- 2. Top 5 Products by Sales Volume within their Category (Using Partition By)
WITH ProductRankings AS (
    SELECT 
        product_category_name,
        product_id,
        COUNT(order_id) as sales_count,
        RANK() OVER (PARTITION BY product_category_name ORDER BY COUNT(order_id) DESC) as category_rank
    FROM processed_retail_data
    GROUP BY product_category_name, product_id
)
SELECT * FROM ProductRankings WHERE category_rank <= 5;

-- 3. High-Value Customers (Revenue > Average Revenue)
SELECT 
    customer_id,
    customer_city,
    SUM(total_price_inr) as customer_spend
FROM processed_retail_data
GROUP BY customer_id, customer_city
HAVING customer_spend > (SELECT AVG(total_price_inr) FROM processed_retail_data)
ORDER BY customer_spend DESC;
