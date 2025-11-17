SELECT
    p.product_name,
    d.month,
    CASE WHEN d.day_of_week IN ('Saturday','Sunday') THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    SUM(f.total_amount) AS revenue
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
JOIN dim_date d ON f.date_id = d.date_id
WHERE d.year = 2025
GROUP BY p.product_name, d.month, day_type
ORDER BY d.month, revenue DESC
LIMIT 5;

SELECT
    c.gender,
    c.age,
    c.city_category,
    SUM(f.total_amount) AS total_purchase
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
GROUP BY c.gender, c.age, c.city_category;

SELECT
    c.occupation,
    p.category,
    SUM(f.total_amount) AS total_sales
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY c.occupation, p.category;

SELECT
    c.gender,
    c.age_group,
    d.quarter,
    SUM(f.total_amount) AS total_purchase
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
JOIN dim_date d ON f.date_id = d.date_id
WHERE d.year = 2025
GROUP BY c.gender, c.age_group, d.quarter;

SELECT
    p.category,
    c.occupation,
    SUM(f.total_amount) AS total_sales
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY p.category, c.occupation
ORDER BY p.category, total_sales DESC
LIMIT 5;

SELECT
    c.city_category,
    c.marital_status,
    d.month,
    SUM(f.total_amount) AS total_sales
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
JOIN dim_date d ON f.date_id = d.date_id
WHERE d.date >= CURRENT_DATE - INTERVAL '6 months'
GROUP BY c.city_category, c.marital_status, d.month;

SELECT
    c.gender,
    c.years_in_city,
    AVG(f.total_amount) AS avg_purchase
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
GROUP BY c.gender, c.years_in_city;

SELECT
    s.city,
    p.category,
    SUM(f.total_amount) AS revenue
FROM fact_sales f
JOIN dim_store s ON f.store_id = s.store_id
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY s.city, p.category
ORDER BY revenue DESC
LIMIT 5;

WITH monthly AS (
    SELECT
        p.category,
        d.month,
        SUM(f.total_amount) AS revenue
    FROM fact_sales f
    JOIN dim_product p ON f.product_id = p.product_id
    JOIN dim_date d ON f.date_id = d.date_id
    WHERE d.year = 2025
    GROUP BY p.category, d.month
)
SELECT
    category,
    month,
    revenue,
    (revenue - LAG(revenue) OVER (PARTITION BY category ORDER BY month))
        / NULLIF(LAG(revenue) OVER (PARTITION BY category ORDER BY month), 0) * 100
        AS growth_percentage
FROM monthly;

SELECT
    c.age_group,
    CASE WHEN d.day_of_week IN ('Saturday','Sunday') THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    SUM(f.total_amount) AS total_sales
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
JOIN dim_date d ON f.date_id = d.date_id
WHERE d.year = 2025
GROUP BY c.age_group, day_type;

SELECT
    p.product_name,
    d.month,
    CASE WHEN d.day_of_week IN ('Saturday','Sunday') THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    SUM(f.total_amount) AS revenue
FROM fact_sales f
JOIN dim_product p ON p.product_id = f.product_id
JOIN dim_date d ON d.date_id = f.date_id
GROUP BY p.product_name, d.month, day_type
ORDER BY revenue DESC
LIMIT 5;

WITH q AS (
    SELECT
        s.store_name,
        d.quarter,
        SUM(f.total_amount) AS revenue
    FROM fact_sales f
    JOIN dim_store s ON f.store_id = s.store_id
    JOIN dim_date d ON f.date_id = d.date_id
    WHERE d.year = 2017
    GROUP BY s.store_name, d.quarter
)
SELECT
    store_name,
    quarter,
    revenue,
    (revenue - LAG(revenue) OVER (PARTITION BY store_name ORDER BY quarter))
        / NULLIF(LAG(revenue) OVER (PARTITION BY store_name ORDER BY quarter), 0) * 100
        AS growth_rate
FROM q;

SELECT
    s.store_name,
    sup.supplier_name,
    p.product_name,
    SUM(f.total_amount) AS total_sales
FROM fact_sales f
JOIN dim_store s ON f.store_id = s.store_id
JOIN dim_product p ON f.product_id = p.product_id
JOIN dim_supplier sup ON p.supplier_id = sup.supplier_id
GROUP BY s.store_name, sup.supplier_name, p.product_name
ORDER BY s.store_name, sup.supplier_name, total_sales DESC;

SELECT
    p.product_name,
    d.season,
    SUM(f.total_amount) AS total_sales
FROM fact_sales f
JOIN dim_product p ON p.product_id = f.product_id
JOIN dim_date d ON d.date_id = f.date_id
GROUP BY p.product_name, d.season;

WITH monthly AS (
    SELECT
        s.store_name,
        sup.supplier_name,
        d.month,
        SUM(f.total_amount) AS revenue
    FROM fact_sales f
    JOIN dim_store s ON f.store_id = s.store_id
    JOIN dim_product p ON f.product_id = p.product_id
    JOIN dim_supplier sup ON p.supplier_id = sup.supplier_id
    JOIN dim_date d ON f.date_id = d.date_id
    GROUP BY s.store_name, sup.supplier_name, d.month
)
SELECT
    store_name,
    supplier_name,
    month,
    revenue,
    (revenue - LAG(revenue) OVER (PARTITION BY store_name, supplier_name ORDER BY month))
        / NULLIF(LAG(revenue) OVER (PARTITION BY store_name, supplier_name ORDER BY month), 0) * 100
        AS volatility_percentage
FROM monthly;

SELECT
    p1.product_name AS product_A,
    p2.product_name AS product_B,
    COUNT(*) AS frequency
FROM fact_sales f1
JOIN fact_sales f2 ON f1.transaction_id = f2.transaction_id AND f1.product_id < f2.product_id
JOIN dim_product p1 ON f1.product_id = p1.product_id
JOIN dim_product p2 ON f2.product_id = p2.product_id
GROUP BY p1.product_name, p2.product_name
ORDER BY frequency DESC
LIMIT 5;

SELECT
    s.store_name,
    sup.supplier_name,
    p.product_name,
    SUM(f.total_amount) AS total_revenue
FROM fact_sales f
JOIN dim_store s ON f.store_id = s.store_id
JOIN dim_product p ON f.product_id = p.product_id
JOIN dim_supplier sup ON p.supplier_id = sup.supplier_id
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY ROLLUP (s.store_name, sup.supplier_name, p.product_name);

SELECT
    p.product_name,
    SUM(CASE WHEN d.month BETWEEN 1 AND 6 THEN f.total_amount END) AS revenue_h1,
    SUM(CASE WHEN d.month BETWEEN 7 AND 12 THEN f.total_amount END) AS revenue_h2,
    SUM(f.total_amount) AS revenue_total,
    
    SUM(CASE WHEN d.month BETWEEN 1 AND 6 THEN f.quantity END) AS qty_h1,
    SUM(CASE WHEN d.month BETWEEN 7 AND 12 THEN f.quantity END) AS qty_h2,
    SUM(f.quantity) AS qty_total
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY p.product_name;

WITH daily AS (
    SELECT
        p.product_name,
        d.date,
        SUM(f.total_amount) AS daily_sales
    FROM fact_sales f
    JOIN dim_product p ON f.product_id = p.product_id
    JOIN dim_date d ON f.date_id = d.date_id
    GROUP BY p.product_name, d.date
),
avg_sales AS (
    SELECT
        product_name,
        AVG(daily_sales) AS avg_sales
    FROM daily
    GROUP BY product_name
)
SELECT
    d.product_name,
    d.date,
    d.daily_sales,
    a.avg_sales,
    CASE WHEN d.daily_sales > 2 * a.avg_sales THEN 'SPIKE' ELSE 'Normal' END AS anomaly_flag
FROM daily d
JOIN avg_sales a USING (product_name)
ORDER BY d.product_name, d.date;

CREATE OR REPLACE VIEW STORE_QUARTERLY_SALES AS
SELECT
    s.store_name,
    d.year,
    d.quarter,
    SUM(f.total_amount) AS quarterly_sales
FROM fact_sales f
JOIN dim_store s ON f.store_id = s.store_id
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY s.store_name, d.year, d.quarter
ORDER BY s.store_name, d.year, d.quarter;
