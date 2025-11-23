-- Q1. Top Revenue-Generating Products on Weekdays and Weekends with Monthly DrillDown
SELECT
    p.Product_Name,
    d.Month,
    CASE WHEN d.Day_Of_Week IN ('Friday','Saturday','Sunday') THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    SUM(f.Purchase_Amount) AS revenue
FROM FactSales f
JOIN DimProduct p ON f.Product_ID = p.Product_ID
JOIN DimDate d ON f.Date_ID = d.Date_ID
WHERE d.Year = 2015
GROUP BY p.Product_Name, d.Month, day_type
ORDER BY d.Month, revenue DESC
LIMIT 10;

-- Q2. Customer Demographics by Purchase Amount with City Category Breakdown
SELECT
    c.Gender,
    c.Age,
    c.City_Category,
    SUM(f.Purchase_Amount) AS total_purchase
FROM FactSales f
JOIN DimCustomer c ON f.Customer_ID = c.Customer_ID
GROUP BY c.Gender, c.Age, c.City_Category;

-- Q3. Product Category Sales by Occupation
SELECT
    c.Occupation,
    p.Product_Category AS category,
    SUM(f.Purchase_Amount) AS total_sales
FROM FactSales f
JOIN DimCustomer c ON f.Customer_ID = c.Customer_ID
JOIN DimProduct p ON f.Product_ID = p.Product_ID
GROUP BY c.Occupation, p.Product_Category;

-- Q4. Total Purchases by Gender and Age Group with Quarterly Trend
SELECT
    c.Gender,
    c.Age AS age_group,
    d.Quarter,
    SUM(f.Purchase_Amount) AS total_purchase
FROM FactSales f
JOIN DimCustomer c ON f.Customer_ID = c.Customer_ID
JOIN DimDate d ON f.Date_ID = d.Date_ID
WHERE d.Year = 2020
GROUP BY c.Gender, c.Age, d.Quarter;

-- Q5. Top Occupations by Product Category Sales
SELECT
    p.Product_Category AS category,
    c.Occupation,
    SUM(f.Purchase_Amount) AS total_sales
FROM FactSales f
JOIN DimCustomer c ON f.Customer_ID = c.Customer_ID
JOIN DimProduct p ON f.Product_ID = p.Product_ID
GROUP BY p.Product_Category, c.Occupation
ORDER BY p.Product_Category, total_sales DESC
LIMIT 5;

-- Q6. City Category Performance by Marital Status with Monthly Breakdown
SELECT
    c.City_Category,
    c.Marital_Status,
    d.Month,
    SUM(f.Purchase_Amount) AS total_sales
FROM FactSales f
JOIN DimCustomer c ON f.Customer_ID = c.Customer_ID
JOIN DimDate d ON f.Date_ID = d.Date_ID
GROUP BY c.City_Category, c.Marital_Status, d.Month;
--WHERE d.Full_Date >= DATE_SUB(CURDATE(), INTERVAL 6 MONTH)
-- Q7. Average Purchase Amount by Stay Duration and Gender
SELECT
    c.Gender,
    c.Stay_In_Current_City_Years AS years_in_city,
    AVG(f.Purchase_Amount) AS avg_purchase
FROM FactSales f
JOIN DimCustomer c ON f.Customer_ID = c.Customer_ID
GROUP BY c.Gender, c.Stay_In_Current_City_Years;

-- Q8. Top 5 Revenue-Generating Cities by Product Category
SELECT
    c.City_Category AS city,
    p.Product_Category AS category,
    SUM(f.Purchase_Amount) AS revenue
FROM FactSales f
JOIN DimCustomer c ON f.Customer_ID = c.Customer_ID
JOIN DimProduct p ON f.Product_ID = p.Product_ID
GROUP BY c.City_Category, p.Product_Category
ORDER BY revenue DESC
LIMIT 5;

-- Q9. Monthly Sales Growth by Product Category
WITH monthly AS (
    SELECT
        p.Product_Category AS category,
        d.Month,
        SUM(f.Purchase_Amount) AS revenue
    FROM FactSales f
    JOIN DimProduct p ON f.Product_ID = p.Product_ID
    JOIN DimDate d ON f.Date_ID = d.Date_ID
    WHERE d.Year = 2020
    GROUP BY p.Product_Category, d.Month
)
SELECT
    category,
    Month,
    revenue,
    (revenue - LAG(revenue) OVER (PARTITION BY category ORDER BY Month))
        / NULLIF(LAG(revenue) OVER (PARTITION BY category ORDER BY Month), 0) * 100
        AS growth_percentage
FROM monthly;

-- Q10. Weekend vs. Weekday Sales by Age Group
SELECT
    c.Age AS age_group,
    CASE WHEN d.Day_Of_Week IN ('Saturday','Sunday') THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    SUM(f.Purchase_Amount) AS total_sales
FROM FactSales f
JOIN DimCustomer c ON f.Customer_ID = c.Customer_ID
JOIN DimDate d ON f.Date_ID = d.Date_ID
WHERE d.Year = 2020
GROUP BY c.Age, day_type;

-- Q11. Top Revenue-Generating Products on Weekdays and Weekends with Monthly DrillDown
SELECT
    p.Product_Name,
    d.Month,
    CASE WHEN d.Day_Of_Week IN ('Saturday','Sunday') THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    SUM(f.Purchase_Amount) AS revenue
FROM FactSales f
JOIN DimProduct p ON p.Product_ID = f.Product_ID
JOIN DimDate d ON d.Date_ID = f.Date_ID
GROUP BY p.Product_Name, d.Month, day_type
ORDER BY revenue DESC
LIMIT 5;

-- Q12. Trend Analysis of Store Revenue Growth Rate Quarterly for 2017
WITH q AS (
    SELECT
        s.Store_Name,
        d.Quarter,
        SUM(f.Purchase_Amount) AS revenue
    FROM FactSales f
    JOIN DimStore s ON f.Store_ID = s.Store_ID
    JOIN DimDate d ON f.Date_ID = d.Date_ID
    WHERE d.Year = 2017
    GROUP BY s.Store_Name, d.Quarter
)
SELECT
    Store_Name,
    Quarter,
    revenue,
    (revenue - LAG(revenue) OVER (PARTITION BY Store_Name ORDER BY Quarter))
        / NULLIF(LAG(revenue) OVER (PARTITION BY Store_Name ORDER BY Quarter), 0) * 100
        AS growth_rate
FROM q;

-- Q13. Detailed Supplier Sales Contribution by Store and Product Name
SELECT
    s.Store_Name,
    sup.Supplier_Name,
    p.Product_Name,
    SUM(f.Purchase_Amount) AS total_sales
FROM FactSales f
JOIN DimStore s ON f.Store_ID = s.Store_ID
JOIN DimProduct p ON f.Product_ID = p.Product_ID
JOIN DimSupplier sup ON p.Supplier_ID = sup.Supplier_ID
GROUP BY s.Store_Name, sup.Supplier_Name, p.Product_Name
ORDER BY s.Store_Name, sup.Supplier_Name, total_sales DESC;

-- Q14. Seasonal Analysis of Product Sales Using Dynamic Drill-Down
SELECT
    p.Product_Name,
    d.Season,
    SUM(f.Purchase_Amount) AS total_sales
FROM FactSales f
JOIN DimProduct p ON p.Product_ID = f.Product_ID
JOIN DimDate d ON d.Date_ID = f.Date_ID
GROUP BY p.Product_Name, d.Season;

-- Q15. Store-Wise and Supplier-Wise Monthly Revenue Volatility
WITH monthly AS (
    SELECT
        s.Store_Name,
        sup.Supplier_Name,
        d.Month,
        SUM(f.Purchase_Amount) AS revenue
    FROM FactSales f
    JOIN DimStore s ON f.Store_ID = s.Store_ID
    JOIN DimProduct p ON f.Product_ID = p.Product_ID
    JOIN DimSupplier sup ON p.Supplier_ID = sup.Supplier_ID
    JOIN DimDate d ON f.Date_ID = d.Date_ID
    GROUP BY s.Store_Name, sup.Supplier_Name, d.Month
)
SELECT
    Store_Name,
    Supplier_Name,
    Month,
    revenue,
    (revenue - LAG(revenue) OVER (PARTITION BY Store_Name, Supplier_Name ORDER BY Month))
        / NULLIF(LAG(revenue) OVER (PARTITION BY Store_Name, Supplier_Name ORDER BY Month), 0) * 100
        AS volatility_percentage
FROM monthly;

-- Q16. Top 5 Products Purchased Together by Same Customer (Product Affinity Analysis)
WITH multi_product_days AS (
    SELECT Customer_ID, Date_ID
    FROM FactSales
    WHERE Customer_ID IS NOT NULL
    GROUP BY Customer_ID, Date_ID
    HAVING COUNT(DISTINCT Product_ID) > 1
),
customer_products AS (
    SELECT DISTINCT
        f.Customer_ID,
        f.Date_ID,
        f.Product_ID
    FROM FactSales f
    INNER JOIN multi_product_days mpd 
        ON f.Customer_ID = mpd.Customer_ID 
        AND f.Date_ID = mpd.Date_ID
),
product_pairs AS (
    SELECT
        cp1.Product_ID AS product_A_id,
        cp2.Product_ID AS product_B_id,
        COUNT(DISTINCT cp1.Customer_ID) AS customer_count,
        COUNT(*) AS frequency
    FROM customer_products cp1
    INNER JOIN customer_products cp2 
        ON cp1.Customer_ID = cp2.Customer_ID
        AND cp1.Date_ID = cp2.Date_ID
        AND cp1.Product_ID < cp2.Product_ID
    GROUP BY cp1.Product_ID, cp2.Product_ID
)
SELECT
    p1.Product_Name AS product_A,
    p2.Product_Name AS product_B,
    pp.customer_count,
    pp.frequency
FROM product_pairs pp
JOIN DimProduct p1 ON pp.product_A_id = p1.Product_ID
JOIN DimProduct p2 ON pp.product_B_id = p2.Product_ID
ORDER BY pp.frequency DESC, pp.customer_count DESC
LIMIT 5;

-- Q17. Yearly Revenue Trends by Store, Supplier, and Product with ROLLUP
SELECT
    s.Store_Name,
    sup.Supplier_Name,
    p.Product_Name,
    SUM(f.Purchase_Amount) AS total_revenue
FROM FactSales f
JOIN DimStore s ON f.Store_ID = s.Store_ID
JOIN DimProduct p ON f.Product_ID = p.Product_ID
JOIN DimSupplier sup ON p.Supplier_ID = sup.Supplier_ID
JOIN DimDate d ON f.Date_ID = d.Date_ID
GROUP BY ROLLUP (s.Store_Name, sup.Supplier_Name, p.Product_Name);

-- Q18. Revenue and Volume-Based Sales Analysis for Each Product for H1 and H2
SELECT
    p.Product_Name,
    SUM(CASE WHEN d.Month BETWEEN 1 AND 6 THEN f.Purchase_Amount END) AS revenue_h1,
    SUM(CASE WHEN d.Month BETWEEN 7 AND 12 THEN f.Purchase_Amount END) AS revenue_h2,
    SUM(f.Purchase_Amount) AS revenue_total,
    SUM(CASE WHEN d.Month BETWEEN 1 AND 6 THEN f.Quantity END) AS qty_h1,
    SUM(CASE WHEN d.Month BETWEEN 7 AND 12 THEN f.Quantity END) AS qty_h2,
    SUM(f.Quantity) AS qty_total
FROM FactSales f
JOIN DimProduct p ON f.Product_ID = p.Product_ID
JOIN DimDate d ON f.Date_ID = d.Date_ID
GROUP BY p.Product_Name;

-- Q19. Identify High Revenue Spikes in Product Sales and Highlight Outliers
WITH daily AS (
    SELECT
        p.Product_Name,
        d.Full_Date AS date,
        SUM(f.Purchase_Amount) AS daily_sales
    FROM FactSales f
    JOIN DimProduct p ON f.Product_ID = p.Product_ID
    JOIN DimDate d ON f.Date_ID = d.Date_ID
    GROUP BY p.Product_Name, d.Full_Date
),
avg_sales AS (
    SELECT
        Product_Name,
        AVG(daily_sales) AS avg_sales
    FROM daily
    GROUP BY Product_Name
)
SELECT
    d.Product_Name,
    d.date,
    d.daily_sales,
    a.avg_sales,
    CASE WHEN d.daily_sales > 2 * a.avg_sales THEN 'SPIKE' ELSE 'Normal' END AS anomaly_flag
FROM daily d
JOIN avg_sales a ON d.Product_Name = a.Product_Name
ORDER BY d.Product_Name, d.date;

-- Q20. Create a View STORE_QUARTERLY_SALES for Optimized Sales Analysis
CREATE OR REPLACE VIEW STORE_QUARTERLY_SALES AS
SELECT
    s.Store_Name,
    d.Year,
    d.Quarter,
    SUM(f.Purchase_Amount) AS quarterly_sales
FROM FactSales f
JOIN DimStore s ON f.Store_ID = s.Store_ID
JOIN DimDate d ON f.Date_ID = d.Date_ID
GROUP BY s.Store_Name, d.Year, d.Quarter
ORDER BY s.Store_Name, d.Year, d.Quarter;
