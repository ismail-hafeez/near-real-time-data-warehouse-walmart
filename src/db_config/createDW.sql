-- SQL Script To Create Data Warehouse --

-- Drop existing tables if they exist (in correct order due to FK constraints)
DROP TABLE IF EXISTS FactSales;
DROP TABLE IF EXISTS DimCustomer;
DROP TABLE IF EXISTS DimProduct;
DROP TABLE IF EXISTS DimDate;
DROP TABLE IF EXISTS DimStore;
DROP TABLE IF EXISTS DimSupplier;

-- ============================================================
-- DIMENSION TABLES
-- ============================================================

-- DimCustomer: Customer demographic information
CREATE TABLE DimCustomer (
    Customer_ID INT PRIMARY KEY,
    Gender VARCHAR(1),
    Age VARCHAR(10),
    Occupation INT,
    City_Category VARCHAR(1),
    Stay_In_Current_City_Years VARCHAR(10),
    Marital_Status INT
);

-- DimProduct: Product details including categories and supplier
CREATE TABLE DimProduct (
    Product_ID VARCHAR(20) PRIMARY KEY,
    Product_Category_1 INT,
    Product_Category_2 INT,
    Product_Category_3 INT,
    Product_Name VARCHAR(100),
    Supplier_ID INT,
    Supplier_Name VARCHAR(100)
);

-- DimDate: Date dimension with time hierarchies
CREATE TABLE DimDate (
    Date_ID INT PRIMARY KEY,
    Full_Date DATE NOT NULL UNIQUE,
    Day INT,
    Month INT,
    Month_Name VARCHAR(20),
    Quarter INT,
    Year INT,
    Day_Of_Week VARCHAR(10),
    Is_Weekend BOOLEAN,
    Season VARCHAR(10),
    Week_Of_Year INT
);

-- DimStore: Store information
CREATE TABLE DimStore (
    Store_ID INT PRIMARY KEY,
    Store_Name VARCHAR(100),
    Store_City_Category VARCHAR(1),
    Store_Region VARCHAR(50)
);

-- ============================================================
-- FACT TABLE
-- ============================================================

-- FactSales: Central fact table containing sales transactions
CREATE TABLE FactSales (
    Sale_ID INT PRIMARY KEY AUTO_INCREMENT,
    Customer_ID INT NOT NULL,
    Product_ID VARCHAR(20) NOT NULL,
    Date_ID INT NOT NULL,
    Store_ID INT NOT NULL,
    Purchase_Amount DECIMAL(10, 2) NOT NULL,
    Quantity INT DEFAULT 1,
    
    -- Foreign Key Constraints
    CONSTRAINT fk_customer FOREIGN KEY (Customer_ID) 
        REFERENCES DimCustomer(Customer_ID)
        ON DELETE RESTRICT
        ON UPDATE CASCADE,
    
    CONSTRAINT fk_product FOREIGN KEY (Product_ID) 
        REFERENCES DimProduct(Product_ID)
        ON DELETE RESTRICT
        ON UPDATE CASCADE,
    
    CONSTRAINT fk_date FOREIGN KEY (Date_ID) 
        REFERENCES DimDate(Date_ID)
        ON DELETE RESTRICT
        ON UPDATE CASCADE,
    
    CONSTRAINT fk_store FOREIGN KEY (Store_ID) 
        REFERENCES DimStore(Store_ID)
        ON DELETE RESTRICT
        ON UPDATE CASCADE
);

-- ============================================================
-- INDEXES FOR PERFORMANCE OPTIMIZATION
-- ============================================================

-- Indexes on Dimension Tables
CREATE INDEX idx_customer_gender ON DimCustomer(Gender);
CREATE INDEX idx_customer_age ON DimCustomer(Age);
CREATE INDEX idx_customer_occupation ON DimCustomer(Occupation);
CREATE INDEX idx_customer_city ON DimCustomer(City_Category);

CREATE INDEX idx_product_cat1 ON DimProduct(Product_Category_1);
CREATE INDEX idx_product_supplier ON DimProduct(Supplier_ID);

CREATE INDEX idx_date_year ON DimDate(Year);
CREATE INDEX idx_date_month ON DimDate(Month);
CREATE INDEX idx_date_quarter ON DimDate(Quarter);
CREATE INDEX idx_date_weekend ON DimDate(Is_Weekend);

CREATE INDEX idx_store_city ON DimStore(Store_City_Category);

-- Indexes on Fact Table (composite indexes for common query patterns)
CREATE INDEX idx_fact_customer ON FactSales(Customer_ID);
CREATE INDEX idx_fact_product ON FactSales(Product_ID);
CREATE INDEX idx_fact_date ON FactSales(Date_ID);
CREATE INDEX idx_fact_store ON FactSales(Store_ID);
CREATE INDEX idx_fact_date_product ON FactSales(Date_ID, Product_ID);
CREATE INDEX idx_fact_customer_date ON FactSales(Customer_ID, Date_ID);

-- ============================================================
-- MATERIALIZED VIEW FOR Q20
-- ============================================================

-- View for Store Quarterly Sales (Q20 requirement)
CREATE VIEW STORE_QUARTERLY_SALES AS
SELECT 
    s.Store_ID,
    s.Store_Name,
    d.Year,
    d.Quarter,
    SUM(f.Purchase_Amount) AS Total_Sales,
    COUNT(f.Sale_ID) AS Transaction_Count
FROM FactSales f
JOIN DimStore s ON f.Store_ID = s.Store_ID
JOIN DimDate d ON f.Date_ID = d.Date_ID
GROUP BY s.Store_ID, s.Store_Name, d.Year, d.Quarter
ORDER BY s.Store_Name, d.Year, d.Quarter;

-- ============================================================
-- END OF DDL SCRIPT
-- ============================================================

-- Verification Queries
-- SELECT COUNT(*) FROM DimCustomer;
-- SELECT COUNT(*) FROM DimProduct;
-- SELECT COUNT(*) FROM DimDate;
-- SELECT COUNT(*) FROM DimStore;
-- SELECT COUNT(*) FROM FactSales;