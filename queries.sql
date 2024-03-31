
-- 1. Most Preferred Communication Method
SELECT communication_method, COUNT(*) AS total_customers
FROM customers
GROUP BY communication_method
ORDER BY total_customers DESC;

-- 2. Average Transaction Activity
SELECT ROUND(AVG(transaction_activity), 2)AS avg_transaction_activity
FROM customers;

-- 3. Customer Retention Rate
SELECT COUNT(DISTINCT customer_id) AS total_customers,
       COUNT(DISTINCT CASE WHEN transaction_activity > 0 THEN customer_id END) AS active_customers,
       (COUNT(DISTINCT CASE WHEN transaction_activity > 0 THEN customer_id END)::numeric / COUNT(DISTINCT customer_id)::numeric) * 100 AS retention_rate
FROM customers;


-- 4. Customer Segmentation by Transaction Activity
SELECT CASE
           WHEN transaction_activity >= 500 THEN 'High Activity'
           WHEN transaction_activity >= 100 AND transaction_activity < 500 THEN 'Medium Activity'
           ELSE 'Low Activity'
       END AS activity_segment,
       COUNT(*) AS total_customers
FROM customers
GROUP BY activity_segment
ORDER BY total_customers DESC;


-- 5. Channel Performance Evaluation
SELECT communication_method,
       ROUND(AVG(transaction_activity), 2) AS avg_usage
FROM customers
GROUP BY communication_method
ORDER BY avg_usage DESC;

-- 6. Top 10 Customers by Transaction Activity
SELECT customer_id, name,  transaction_activity as number_of_transactions
FROM customers
ORDER BY transaction_activity DESC
LIMIT 10;

-- 7. Inactive Customers
SELECT COUNT(DISTINCT name) AS inactive_customers
FROM public.customers
WHERE transaction_activity = 0;

-- 8. Transaction Volume by Geography
SELECT address, COUNT(transaction_activity) AS total_transactions
FROM customers
GROUP BY address
ORDER BY total_transactions DESC;
