-- Top 10 customers by total spend
CREATE OR REPLACE VIEW top_customers AS
SELECT c.customer_id,
       c.full_name,
       SUM(o.total_amount) AS total_spend
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
WHERE o.status IN ('placed','shipped')
GROUP BY c.customer_id, c.full_name
ORDER BY total_spend DESC
LIMIT 10;

-- Monthly revenue trend
CREATE OR REPLACE VIEW monthly_revenue AS
SELECT DATE_TRUNC('month', o.order_ts) AS month,
       SUM(o.total_amount) AS revenue
FROM orders o
WHERE o.status = 'shipped'
GROUP BY DATE_TRUNC('month', o.order_ts)
ORDER BY month;

-- Product category breakdown
CREATE OR REPLACE VIEW category_sales AS
SELECT oi.category,
       SUM(oi.quantity * oi.unit_price) AS total_sales
FROM order_items oi
JOIN orders o ON oi.order_id = o.order_id
WHERE o.status = 'shipped'
GROUP BY oi.category
ORDER BY total_sales DESC;

-- Order status distribution
CREATE OR REPLACE VIEW order_status_summary AS
SELECT status,
       COUNT(*) AS order_count
FROM orders
GROUP BY status
ORDER BY order_count DESC;

