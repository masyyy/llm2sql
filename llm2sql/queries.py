"""20 benchmark queries with ground-truth SQL."""

QUERIES = [
    {
        "id": 1,
        "question": "List all at-risk accounts.",
        "difficulty": "easy",
        "sql": """
SELECT c.id, c.name, c.country, MAX(o.order_date) AS last_order_date
FROM customers c
LEFT JOIN orders o ON o.customer_id = c.id AND o.status = 'completed'
WHERE c.status = 'active'
GROUP BY c.id, c.name, c.country
HAVING (MAX(o.order_date) < DATE('now', '-180 days') OR MAX(o.order_date) IS NULL)
  AND EXISTS (
    SELECT 1 FROM machines m WHERE m.customer_id = c.id AND m.status = 'active'
  );
""",
    },
    {
        "id": 2,
        "question": "Show all stale quotes with customer name and total value.",
        "difficulty": "easy",
        "sql": """
SELECT q.id, c.name AS customer_name, q.sent_date, q.total_value
FROM quotes q
JOIN customers c ON c.id = q.customer_id
WHERE q.status = 'sent' AND q.sent_date < DATE('now', '-45 days')
ORDER BY q.sent_date ASC;
""",
    },
    {
        "id": 3,
        "question": "Which customers are cross-sell candidates for service contracts?",
        "difficulty": "easy",
        "sql": """
SELECT c.id, c.name, COUNT(m.id) AS active_machines
FROM customers c
JOIN machines m ON m.customer_id = c.id AND m.status = 'active'
WHERE NOT EXISTS (
  SELECT 1 FROM service_contracts sc
  WHERE sc.customer_id = c.id AND sc.status = 'active'
)
GROUP BY c.id, c.name
ORDER BY active_machines DESC;
""",
    },
    {
        "id": 4,
        "question": "Total revenue by customer this year.",
        "difficulty": "easy",
        "sql": """
SELECT c.name, SUM(o.total_value) AS revenue_ytd
FROM orders o
JOIN customers c ON c.id = o.customer_id
WHERE o.status = 'completed'
  AND strftime('%Y', o.order_date) = strftime('%Y', 'now')
GROUP BY c.id, c.name
ORDER BY revenue_ytd DESC;
""",
    },
    {
        "id": 5,
        "question": "Which products haven't been ordered in the last 6 months?",
        "difficulty": "easy",
        "sql": """
SELECT p.id, p.name, p.product_type
FROM products p
WHERE NOT EXISTS (
  SELECT 1 FROM order_lines ol
  JOIN orders o ON o.id = ol.order_id
  WHERE ol.product_id = p.id AND o.status = 'completed'
    AND o.order_date >= DATE('now', '-6 months')
)
ORDER BY p.name;
""",
    },
    {
        "id": 6,
        "question": "List high-value accounts.",
        "difficulty": "easy",
        "sql": """
WITH customer_revenue AS (
  SELECT customer_id, SUM(total_value) AS lifetime_value
  FROM orders WHERE status = 'completed'
  GROUP BY customer_id
),
ranked AS (
  SELECT customer_id, lifetime_value,
    NTILE(5) OVER (ORDER BY lifetime_value ASC) AS quintile
  FROM customer_revenue
)
SELECT c.name, r.lifetime_value
FROM ranked r
JOIN customers c ON c.id = r.customer_id
WHERE r.quintile = 5
ORDER BY r.lifetime_value DESC;
""",
    },
    {
        "id": 7,
        "question": "How many active machines does each customer have?",
        "difficulty": "easy",
        "sql": """
SELECT c.name, COUNT(m.id) AS active_machines
FROM customers c
LEFT JOIN machines m ON m.customer_id = c.id AND m.status = 'active'
GROUP BY c.id, c.name
ORDER BY active_machines DESC;
""",
    },
    {
        "id": 8,
        "question": "Service contract penetration per customer.",
        "difficulty": "medium",
        "sql": """
SELECT
  c.name,
  COUNT(m.id) AS total_machines,
  COUNT(sc.id) AS contracted_machines,
  ROUND(COUNT(sc.id) * 100.0 / NULLIF(COUNT(m.id), 0), 1) AS penetration_pct
FROM customers c
JOIN machines m ON m.customer_id = c.id AND m.status = 'active'
LEFT JOIN service_contracts sc ON sc.machine_id = m.id AND sc.status = 'active'
GROUP BY c.id, c.name
ORDER BY penetration_pct ASC;
""",
    },
    {
        "id": 9,
        "question": "Which customers have a churn signal?",
        "difficulty": "medium",
        "sql": """
WITH recent AS (
  SELECT customer_id, COUNT(*) AS orders_recent
  FROM orders
  WHERE status = 'completed'
    AND order_date >= DATE('now', '-6 months')
  GROUP BY customer_id
),
prior AS (
  SELECT customer_id, COUNT(*) AS orders_prior
  FROM orders
  WHERE status = 'completed'
    AND order_date >= DATE('now', '-12 months')
    AND order_date < DATE('now', '-6 months')
  GROUP BY customer_id
)
SELECT c.name, COALESCE(r.orders_recent, 0) AS recent, COALESCE(p.orders_prior, 0) AS prior
FROM customers c
LEFT JOIN recent r ON r.customer_id = c.id
LEFT JOIN prior p ON p.customer_id = c.id
WHERE COALESCE(r.orders_recent, 0) < COALESCE(p.orders_prior, 0)
ORDER BY (COALESCE(p.orders_prior, 0) - COALESCE(r.orders_recent, 0)) DESC;
""",
    },
    {
        "id": 10,
        "question": "Expansion potential score per customer.",
        "difficulty": "medium",
        "sql": """
WITH penetration AS (
  SELECT
    m.customer_id,
    COUNT(m.id) AS total_machines,
    COUNT(sc.id) AS contracted,
    ROUND(COUNT(sc.id) * 1.0 / NULLIF(COUNT(m.id), 0), 4) AS pen_ratio
  FROM machines m
  LEFT JOIN service_contracts sc ON sc.machine_id = m.id AND sc.status = 'active'
  WHERE m.status = 'active'
  GROUP BY m.customer_id
)
SELECT c.name, p.total_machines,
  ROUND(p.total_machines * (1 - p.pen_ratio), 2) AS expansion_potential
FROM penetration p
JOIN customers c ON c.id = p.customer_id
ORDER BY expansion_potential DESC;
""",
    },
    {
        "id": 11,
        "question": "Pipeline by account manager — total open quote value.",
        "difficulty": "medium",
        "sql": """
SELECT am.name, COUNT(q.id) AS open_quotes, SUM(q.total_value) AS pipeline_value
FROM quotes q
JOIN account_managers am ON am.id = q.account_manager_id
WHERE q.status IN ('draft', 'sent')
GROUP BY am.id, am.name
ORDER BY pipeline_value DESC;
""",
    },
    {
        "id": 12,
        "question": "Customers who bought equipment but never bought a spare part.",
        "difficulty": "medium",
        "sql": """
SELECT DISTINCT c.id, c.name
FROM customers c
WHERE EXISTS (
  SELECT 1 FROM orders o
  JOIN order_lines ol ON ol.order_id = o.id
  JOIN products p ON p.id = ol.product_id
  WHERE o.customer_id = c.id AND p.product_type = 'equipment' AND o.status = 'completed'
)
AND NOT EXISTS (
  SELECT 1 FROM orders o
  JOIN order_lines ol ON ol.order_id = o.id
  JOIN products p ON p.id = ol.product_id
  WHERE o.customer_id = c.id AND p.product_type = 'spare_part' AND o.status = 'completed'
);
""",
    },
    {
        "id": 13,
        "question": "Total revenue at risk from at-risk accounts.",
        "difficulty": "medium",
        "sql": """
WITH at_risk AS (
  SELECT c.id
  FROM customers c
  LEFT JOIN orders o ON o.customer_id = c.id AND o.status = 'completed'
  WHERE c.status = 'active'
  GROUP BY c.id
  HAVING (MAX(o.order_date) < DATE('now', '-180 days') OR MAX(o.order_date) IS NULL)
    AND EXISTS (SELECT 1 FROM machines m WHERE m.customer_id = c.id AND m.status = 'active')
),
annual_spend AS (
  SELECT customer_id, SUM(total_value) / 1.0 AS avg_annual
  FROM orders
  WHERE status = 'completed'
    AND order_date >= DATE('now', '-12 months')
  GROUP BY customer_id
)
SELECT ROUND(SUM(COALESCE(a.avg_annual, 0)), 2) AS revenue_at_risk
FROM at_risk r
LEFT JOIN annual_spend a ON a.customer_id = r.id;
""",
    },
    {
        "id": 14,
        "question": "Quote win rate by account manager.",
        "difficulty": "medium",
        "sql": """
SELECT
  am.name,
  COUNT(*) AS total_closed,
  SUM(CASE WHEN q.status = 'won' THEN 1 ELSE 0 END) AS won,
  ROUND(SUM(CASE WHEN q.status = 'won' THEN 1.0 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100, 1) AS win_rate_pct
FROM quotes q
JOIN account_managers am ON am.id = q.account_manager_id
WHERE q.status IN ('won', 'lost')
GROUP BY am.id, am.name
ORDER BY win_rate_pct DESC;
""",
    },
    {
        "id": 15,
        "question": "For each at-risk account: last order, installed base count, and total installed base list price.",
        "difficulty": "hard",
        "sql": """
WITH at_risk AS (
  SELECT c.id, c.name, MAX(o.order_date) AS last_order
  FROM customers c
  LEFT JOIN orders o ON o.customer_id = c.id AND o.status = 'completed'
  WHERE c.status = 'active'
  GROUP BY c.id, c.name
  HAVING (MAX(o.order_date) < DATE('now', '-180 days') OR MAX(o.order_date) IS NULL)
    AND EXISTS (SELECT 1 FROM machines m WHERE m.customer_id = c.id AND m.status = 'active')
)
SELECT
  ar.name, ar.last_order,
  COUNT(m.id) AS machines,
  SUM(p.list_price) AS installed_base_value
FROM at_risk ar
JOIN machines m ON m.customer_id = ar.id AND m.status = 'active'
JOIN products p ON p.id = m.product_id
GROUP BY ar.id, ar.name, ar.last_order
ORDER BY installed_base_value DESC;
""",
    },
    {
        "id": 16,
        "question": "Accounts with highest cross-sell potential ranked by uncontracted installed base value.",
        "difficulty": "hard",
        "sql": """
SELECT
  c.name,
  COUNT(m.id) AS total_machines,
  COUNT(sc.id) AS contracted,
  COUNT(m.id) - COUNT(sc.id) AS uncontracted,
  SUM(CASE WHEN sc.id IS NULL THEN p.list_price ELSE 0 END) AS uncontracted_base_value
FROM customers c
JOIN machines m ON m.customer_id = c.id AND m.status = 'active'
JOIN products p ON p.id = m.product_id
LEFT JOIN service_contracts sc ON sc.machine_id = m.id AND sc.status = 'active'
GROUP BY c.id, c.name
HAVING uncontracted > 0
ORDER BY uncontracted_base_value DESC;
""",
    },
    {
        "id": 17,
        "question": "Average time from machine installation to first spare part order, per product.",
        "difficulty": "hard",
        "sql": """
SELECT
  p.name,
  ROUND(AVG(
    JULIANDAY(first_spare.order_date) - JULIANDAY(m.installation_date)
  ), 0) AS avg_days_to_first_spare
FROM machines m
JOIN products p ON p.id = m.product_id
JOIN (
  SELECT ol.order_id, o.customer_id, o.order_date,
    ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY o.order_date) AS rn
  FROM order_lines ol
  JOIN orders o ON o.id = ol.order_id
  JOIN products sp ON sp.id = ol.product_id
  WHERE sp.product_type = 'spare_part' AND o.status = 'completed'
) first_spare ON first_spare.customer_id = m.customer_id AND first_spare.rn = 1
  AND first_spare.order_date > m.installation_date
GROUP BY p.id, p.name
ORDER BY avg_days_to_first_spare ASC;
""",
    },
    {
        "id": 18,
        "question": "Customers with declining order frequency for 3+ consecutive quarters.",
        "difficulty": "hard",
        "sql": """
WITH quarterly AS (
  SELECT
    customer_id,
    strftime('%Y', order_date) || '-Q' || ((CAST(strftime('%m', order_date) AS INT) - 1) / 3 + 1) AS quarter,
    COUNT(*) AS order_count
  FROM orders
  WHERE status = 'completed'
  GROUP BY customer_id, quarter
),
with_lag AS (
  SELECT *,
    LAG(order_count, 1) OVER (PARTITION BY customer_id ORDER BY quarter) AS prev1,
    LAG(order_count, 2) OVER (PARTITION BY customer_id ORDER BY quarter) AS prev2
  FROM quarterly
)
SELECT DISTINCT c.name
FROM with_lag w
JOIN customers c ON c.id = w.customer_id
WHERE w.order_count < w.prev1 AND w.prev1 < w.prev2;
""",
    },
    {
        "id": 19,
        "question": "For each account manager: at-risk accounts, stale quotes, and pipeline value.",
        "difficulty": "hard",
        "sql": """
WITH at_risk AS (
  SELECT c.account_manager_id, COUNT(*) AS at_risk_count
  FROM customers c
  LEFT JOIN orders o ON o.customer_id = c.id AND o.status = 'completed'
  WHERE c.status = 'active'
  GROUP BY c.id, c.account_manager_id
  HAVING (MAX(o.order_date) < DATE('now', '-180 days') OR MAX(o.order_date) IS NULL)
    AND EXISTS (SELECT 1 FROM machines m WHERE m.customer_id = c.id AND m.status = 'active')
),
at_risk_summary AS (
  SELECT account_manager_id, SUM(at_risk_count) AS at_risk_accounts FROM at_risk GROUP BY account_manager_id
),
stale AS (
  SELECT account_manager_id, COUNT(*) AS stale_quotes, SUM(total_value) AS stale_value
  FROM quotes
  WHERE status = 'sent' AND sent_date < DATE('now', '-45 days')
  GROUP BY account_manager_id
),
pipeline AS (
  SELECT account_manager_id, SUM(total_value) AS pipeline_value
  FROM quotes WHERE status IN ('draft','sent')
  GROUP BY account_manager_id
)
SELECT
  am.name,
  COALESCE(ar.at_risk_accounts, 0) AS at_risk_accounts,
  COALESCE(s.stale_quotes, 0) AS stale_quotes,
  COALESCE(p.pipeline_value, 0) AS pipeline_value
FROM account_managers am
LEFT JOIN at_risk_summary ar ON ar.account_manager_id = am.id
LEFT JOIN stale s ON s.account_manager_id = am.id
LEFT JOIN pipeline p ON p.account_manager_id = am.id
ORDER BY at_risk_accounts DESC;
""",
    },
    {
        "id": 20,
        "question": "Segment accounts by health: healthy / watch / at-risk using multiple signals.",
        "difficulty": "hard",
        "sql": """
WITH signals AS (
  SELECT
    c.id, c.name, c.segment,
    CASE WHEN MAX(o.order_date) < DATE('now', '-180 days') OR MAX(o.order_date) IS NULL THEN 1 ELSE 0 END AS no_recent_order,
    CASE WHEN COUNT(q_stale.id) > 0 THEN 1 ELSE 0 END AS has_stale_quote,
    CASE WHEN
      (SELECT COUNT(*) FROM orders o2 WHERE o2.customer_id = c.id AND o2.status='completed' AND o2.order_date >= DATE('now','-6 months'))
      <
      (SELECT COUNT(*) FROM orders o3 WHERE o3.customer_id = c.id AND o3.status='completed' AND o3.order_date >= DATE('now','-12 months') AND o3.order_date < DATE('now','-6 months'))
    THEN 1 ELSE 0 END AS declining_frequency
  FROM customers c
  LEFT JOIN orders o ON o.customer_id = c.id AND o.status = 'completed'
  LEFT JOIN quotes q_stale ON q_stale.customer_id = c.id AND q_stale.status = 'sent' AND q_stale.sent_date < DATE('now', '-45 days')
  WHERE c.status = 'active'
  GROUP BY c.id, c.name, c.segment
)
SELECT
  name, segment,
  no_recent_order + has_stale_quote + declining_frequency AS signal_count,
  CASE
    WHEN no_recent_order + has_stale_quote + declining_frequency = 0 THEN 'healthy'
    WHEN no_recent_order + has_stale_quote + declining_frequency = 1 THEN 'watch'
    ELSE 'at-risk'
  END AS health_status
FROM signals
ORDER BY signal_count DESC, name;
""",
    },
]
