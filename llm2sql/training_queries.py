"""30 training queries for GRPO fine-tuning. Separate from the 20 eval queries."""

TRAINING_QUERIES = [
    # --- Easy (1-12): single joins, simple filters, basic aggregations ---
    {
        "id": 1,
        "question": "List all active customers and their country.",
        "difficulty": "easy",
        "sql": """
SELECT name, country FROM customers WHERE status = 'active' ORDER BY name;
""",
    },
    {
        "id": 2,
        "question": "How many orders are in each status?",
        "difficulty": "easy",
        "sql": """
SELECT status, COUNT(*) AS order_count FROM orders GROUP BY status ORDER BY order_count DESC;
""",
    },
    {
        "id": 3,
        "question": "What is the average list price per product type?",
        "difficulty": "easy",
        "sql": """
SELECT product_type, ROUND(AVG(list_price), 2) AS avg_price
FROM products
GROUP BY product_type
ORDER BY avg_price DESC;
""",
    },
    {
        "id": 4,
        "question": "Show all equipment products sorted by list price descending.",
        "difficulty": "easy",
        "sql": """
SELECT name, category, list_price
FROM products
WHERE product_type = 'equipment'
ORDER BY list_price DESC;
""",
    },
    {
        "id": 5,
        "question": "Which account managers are in the EMEA region?",
        "difficulty": "easy",
        "sql": """
SELECT name, email FROM account_managers WHERE region = 'EMEA' ORDER BY name;
""",
    },
    {
        "id": 6,
        "question": "How many customers are in each segment?",
        "difficulty": "easy",
        "sql": """
SELECT segment, COUNT(*) AS customer_count
FROM customers
WHERE status = 'active'
GROUP BY segment
ORDER BY customer_count DESC;
""",
    },
    {
        "id": 7,
        "question": "List all decommissioned machines with customer name.",
        "difficulty": "easy",
        "sql": """
SELECT m.serial_number, c.name AS customer_name, m.installation_date
FROM machines m
JOIN customers c ON c.id = m.customer_id
WHERE m.status = 'decommissioned'
ORDER BY m.installation_date;
""",
    },
    {
        "id": 8,
        "question": "What is the total value of all completed orders?",
        "difficulty": "easy",
        "sql": """
SELECT ROUND(SUM(total_value), 2) AS total_completed_revenue
FROM orders
WHERE status = 'completed';
""",
    },
    {
        "id": 9,
        "question": "List customers who were acquired in 2023.",
        "difficulty": "easy",
        "sql": """
SELECT name, country, acquisition_date
FROM customers
WHERE strftime('%Y', acquisition_date) = '2023'
ORDER BY acquisition_date;
""",
    },
    {
        "id": 10,
        "question": "Show all expired service contracts with customer and machine.",
        "difficulty": "easy",
        "sql": """
SELECT c.name AS customer_name, m.serial_number, sc.start_date, sc.end_date, sc.annual_value
FROM service_contracts sc
JOIN customers c ON c.id = sc.customer_id
JOIN machines m ON m.id = sc.machine_id
WHERE sc.status = 'expired'
ORDER BY sc.end_date;
""",
    },
    {
        "id": 11,
        "question": "Which products have been included in order lines most frequently?",
        "difficulty": "easy",
        "sql": """
SELECT p.name, COUNT(ol.id) AS times_ordered
FROM order_lines ol
JOIN products p ON p.id = ol.product_id
GROUP BY p.id, p.name
ORDER BY times_ordered DESC;
""",
    },
    {
        "id": 12,
        "question": "How many quotes are in each status?",
        "difficulty": "easy",
        "sql": """
SELECT status, COUNT(*) AS quote_count
FROM quotes
GROUP BY status
ORDER BY quote_count DESC;
""",
    },
    # --- Medium (13-22): multi-join, GROUP BY + HAVING, subqueries ---
    {
        "id": 13,
        "question": "Which customers have more than 2 active machines?",
        "difficulty": "medium",
        "sql": """
SELECT c.name, COUNT(m.id) AS active_machines
FROM customers c
JOIN machines m ON m.customer_id = c.id AND m.status = 'active'
GROUP BY c.id, c.name
HAVING COUNT(m.id) > 2
ORDER BY active_machines DESC;
""",
    },
    {
        "id": 14,
        "question": "Total order value by account manager for completed orders.",
        "difficulty": "medium",
        "sql": """
SELECT am.name, COUNT(o.id) AS total_orders, ROUND(SUM(o.total_value), 2) AS total_revenue
FROM orders o
JOIN account_managers am ON am.id = o.account_manager_id
WHERE o.status = 'completed'
GROUP BY am.id, am.name
ORDER BY total_revenue DESC;
""",
    },
    {
        "id": 15,
        "question": "Average order value per customer segment.",
        "difficulty": "medium",
        "sql": """
SELECT c.segment, ROUND(AVG(o.total_value), 2) AS avg_order_value
FROM orders o
JOIN customers c ON c.id = o.customer_id
WHERE o.status = 'completed'
GROUP BY c.segment
ORDER BY avg_order_value DESC;
""",
    },
    {
        "id": 16,
        "question": "Which customers have at least one active service contract and at least one machine without a contract?",
        "difficulty": "medium",
        "sql": """
SELECT c.name,
    COUNT(DISTINCT sc.machine_id) AS contracted_machines,
    COUNT(DISTINCT m.id) - COUNT(DISTINCT sc.machine_id) AS uncontracted_machines
FROM customers c
JOIN machines m ON m.customer_id = c.id AND m.status = 'active'
LEFT JOIN service_contracts sc ON sc.machine_id = m.id AND sc.status = 'active'
GROUP BY c.id, c.name
HAVING COUNT(DISTINCT sc.machine_id) > 0
    AND COUNT(DISTINCT m.id) > COUNT(DISTINCT sc.machine_id)
ORDER BY uncontracted_machines DESC;
""",
    },
    {
        "id": 17,
        "question": "Total annual service contract value per customer.",
        "difficulty": "medium",
        "sql": """
SELECT c.name, SUM(sc.annual_value) AS total_contract_value
FROM service_contracts sc
JOIN customers c ON c.id = sc.customer_id
WHERE sc.status = 'active'
GROUP BY c.id, c.name
ORDER BY total_contract_value DESC;
""",
    },
    {
        "id": 18,
        "question": "Customers whose most recent order was a spare part.",
        "difficulty": "medium",
        "sql": """
WITH latest_order AS (
    SELECT o.customer_id, o.id AS order_id, o.order_date,
        ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY o.order_date DESC) AS rn
    FROM orders o
    WHERE o.status = 'completed'
)
SELECT c.name, lo.order_date
FROM latest_order lo
JOIN customers c ON c.id = lo.customer_id
WHERE lo.rn = 1
AND EXISTS (
    SELECT 1 FROM order_lines ol
    JOIN products p ON p.id = ol.product_id
    WHERE ol.order_id = lo.order_id AND p.product_type = 'spare_part'
)
ORDER BY lo.order_date DESC;
""",
    },
    {
        "id": 19,
        "question": "Number of orders per month in 2025.",
        "difficulty": "medium",
        "sql": """
SELECT strftime('%Y-%m', order_date) AS month, COUNT(*) AS order_count
FROM orders
WHERE strftime('%Y', order_date) = '2025' AND status = 'completed'
GROUP BY month
ORDER BY month;
""",
    },
    {
        "id": 20,
        "question": "Which account managers have customers in more than one country?",
        "difficulty": "medium",
        "sql": """
SELECT am.name, COUNT(DISTINCT c.country) AS countries
FROM account_managers am
JOIN customers c ON c.account_manager_id = am.id AND c.status = 'active'
GROUP BY am.id, am.name
HAVING COUNT(DISTINCT c.country) > 1
ORDER BY countries DESC;
""",
    },
    {
        "id": 21,
        "question": "Show the most expensive order line for each order.",
        "difficulty": "medium",
        "sql": """
SELECT o.id AS order_id, c.name AS customer_name, p.name AS product_name, ol.line_value
FROM order_lines ol
JOIN orders o ON o.id = ol.order_id
JOIN customers c ON c.id = o.customer_id
JOIN products p ON p.id = ol.product_id
WHERE ol.line_value = (
    SELECT MAX(ol2.line_value) FROM order_lines ol2 WHERE ol2.order_id = ol.order_id
)
ORDER BY ol.line_value DESC;
""",
    },
    {
        "id": 22,
        "question": "Customers with active machines but no completed orders in 2025 or 2026.",
        "difficulty": "medium",
        "sql": """
SELECT c.name, COUNT(m.id) AS active_machines
FROM customers c
JOIN machines m ON m.customer_id = c.id AND m.status = 'active'
WHERE c.status = 'active'
AND NOT EXISTS (
    SELECT 1 FROM orders o
    WHERE o.customer_id = c.id AND o.status = 'completed'
    AND strftime('%Y', o.order_date) IN ('2025', '2026')
)
GROUP BY c.id, c.name
ORDER BY active_machines DESC;
""",
    },
    # --- Hard (23-30): CTEs, window functions, complex logic ---
    {
        "id": 23,
        "question": "Rank customers by total lifetime spend and show their rank.",
        "difficulty": "hard",
        "sql": """
SELECT c.name,
    ROUND(SUM(o.total_value), 2) AS lifetime_spend,
    RANK() OVER (ORDER BY SUM(o.total_value) DESC) AS spend_rank
FROM orders o
JOIN customers c ON c.id = o.customer_id
WHERE o.status = 'completed'
GROUP BY c.id, c.name
ORDER BY spend_rank;
""",
    },
    {
        "id": 24,
        "question": "For each product category, show the number of distinct customers who ordered it.",
        "difficulty": "hard",
        "sql": """
SELECT p.category, COUNT(DISTINCT o.customer_id) AS distinct_customers
FROM order_lines ol
JOIN products p ON p.id = ol.product_id
JOIN orders o ON o.id = ol.order_id
WHERE o.status = 'completed'
GROUP BY p.category
ORDER BY distinct_customers DESC;
""",
    },
    {
        "id": 25,
        "question": "Show each customer's first and last completed order dates and the number of days between them.",
        "difficulty": "hard",
        "sql": """
SELECT c.name,
    MIN(o.order_date) AS first_order,
    MAX(o.order_date) AS last_order,
    CAST(JULIANDAY(MAX(o.order_date)) - JULIANDAY(MIN(o.order_date)) AS INTEGER) AS days_span
FROM orders o
JOIN customers c ON c.id = o.customer_id
WHERE o.status = 'completed'
GROUP BY c.id, c.name
HAVING COUNT(o.id) > 1
ORDER BY days_span DESC;
""",
    },
    {
        "id": 26,
        "question": "Which customers have machines from 3 or more different product categories?",
        "difficulty": "hard",
        "sql": """
SELECT c.name, COUNT(DISTINCT p.category) AS machine_categories
FROM customers c
JOIN machines m ON m.customer_id = c.id AND m.status = 'active'
JOIN products p ON p.id = m.product_id
GROUP BY c.id, c.name
HAVING COUNT(DISTINCT p.category) >= 3
ORDER BY machine_categories DESC;
""",
    },
    {
        "id": 27,
        "question": "Running total of completed order revenue by month.",
        "difficulty": "hard",
        "sql": """
WITH monthly AS (
    SELECT strftime('%Y-%m', order_date) AS month, SUM(total_value) AS monthly_revenue
    FROM orders
    WHERE status = 'completed'
    GROUP BY month
)
SELECT month, ROUND(monthly_revenue, 2) AS monthly_revenue,
    ROUND(SUM(monthly_revenue) OVER (ORDER BY month), 2) AS running_total
FROM monthly
ORDER BY month;
""",
    },
    {
        "id": 28,
        "question": "For each account manager, show their customer count and total active machines.",
        "difficulty": "hard",
        "sql": """
WITH am_customers AS (
    SELECT c.account_manager_id, COUNT(DISTINCT c.id) AS customer_count
    FROM customers c
    WHERE c.status = 'active'
    GROUP BY c.account_manager_id
),
am_machines AS (
    SELECT c.account_manager_id, COUNT(m.id) AS machine_count
    FROM customers c
    JOIN machines m ON m.customer_id = c.id AND m.status = 'active'
    WHERE c.status = 'active'
    GROUP BY c.account_manager_id
)
SELECT am.name,
    COALESCE(ac.customer_count, 0) AS customers,
    COALESCE(amm.machine_count, 0) AS active_machines
FROM account_managers am
LEFT JOIN am_customers ac ON ac.account_manager_id = am.id
LEFT JOIN am_machines amm ON amm.account_manager_id = am.id
ORDER BY customers DESC;
""",
    },
    {
        "id": 29,
        "question": "For each product type, show the total quantity sold and total revenue from completed orders.",
        "difficulty": "hard",
        "sql": """
SELECT p.product_type,
    SUM(ol.quantity) AS total_quantity,
    ROUND(SUM(ol.line_value), 2) AS total_revenue,
    COUNT(DISTINCT o.customer_id) AS distinct_buyers
FROM order_lines ol
JOIN products p ON p.id = ol.product_id
JOIN orders o ON o.id = ol.order_id
WHERE o.status = 'completed'
GROUP BY p.product_type
ORDER BY total_revenue DESC;
""",
    },
    {
        "id": 30,
        "question": "For each customer segment, the average number of active machines per customer.",
        "difficulty": "hard",
        "sql": """
WITH customer_machines AS (
    SELECT c.id, c.segment, COUNT(m.id) AS machine_count
    FROM customers c
    LEFT JOIN machines m ON m.customer_id = c.id AND m.status = 'active'
    WHERE c.status = 'active'
    GROUP BY c.id, c.segment
)
SELECT segment, ROUND(AVG(machine_count), 1) AS avg_machines_per_customer
FROM customer_machines
GROUP BY segment
ORDER BY avg_machines_per_customer DESC;
""",
    },
]
