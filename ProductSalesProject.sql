--- Comapany Sales and Products analysis project

--- Create Agents table
CREATE TABLE STAGE_DW.Agents (
	AGENT_ID INT,
	AGENT_EXPERIENCE_LEVEL NVARCHAR(50),
	AGENT_EDUCATION_LEVEL NVARCHAR(50));
GO
-- used Import wizard option to insert data from csv to  AGENT_TABLE in dbo schema.

--- Create all the tables needed for the new database
CREATE TABLE [STAGE_DW].[Promotions](
	[PROMO_ID] [int] NOT NULL PRIMARY KEY,
	[PROMO_NAME] [varchar](30) NOT NULL,
	[PROMO_SUBCATEGORY] [varchar](30) NOT NULL,
	[PROMO_CATEGORY] [varchar](30) NOT NULL,
	[PROMO_COST] [decimal](8, 2) NOT NULL,
	[PROMO_BEGIN_DATE] [datetime] NOT NULL,
	[PROMO_END_DATE] [datetime] NOT NULL
) 
GO

CREATE TABLE [STAGE_DW].[Countries](
	[COUNTRY_ID] [int] NOT NULL PRIMARY KEY,
	[COUNTRY_NAME] [varchar](50) NULL,
	[COUNTRY_SUBREGION] [varchar](30) NULL,
	[COUNTRY_REGION] [varchar](20) NULL
) 
GO

CREATE TABLE [STAGE_DW].[Customers](
	[CUST_ID] [int] NOT NULL PRIMARY KEY,
	[CUST_NAME] [varchar](60) NOT NULL,
	[CUST_GENDER] [char](1) NULL,
	[CUST_MAIN_PHONE_NUMBER] [varchar](25) NULL,
	[CUST_EMAIL] [varchar](30) NULL,
	[CUST_STREET_ADDRESS] [varchar](40) NOT NULL,
	[CUST_POSTAL_CODE] [int] NOT NULL,
	[CUST_CITY] [varchar](30) NOT NULL,
    [COUNTRY_ID] [int] NOT NULL,
	[COUNTRY_NAME] [varchar](50) NULL,
	[COUNTRY_SUBREGION] [varchar](30) NULL,
	[COUNTRY_REGION] [varchar](20) NULL,
	[CUST_STATE_PROVINCE] [varchar](40) NULL,
    [CUST_EDUCATION] [int] NULL,
	[CUST_MARITAL_STATUS] [varchar](20) NULL,
    [RACE] [int] NULL,
	[INCOME] [float] NULL,
    [NETWORTH] [int] NULL
) 
GO

GO

CREATE TABLE [STAGE_DW].[Products](
	[PROD_ID] [int] NOT NULL,
	[PROD_NAME] [varchar](50) NOT NULL,
	[PROD_DESC] [varchar](4000) NOT NULL,
	[PROD_WEIGHT_CLASS] [smallint] NULL,
	[PROD_UNIT_OF_MEASURE] [char](1) NULL,
	[PROD_PACK_SIZE] [char](1) NULL,
	[SUPPLIER_ID] [int] NULL,
	[PROD_SUBCATEGORY] [varchar](50) NOT NULL,
	[PROD_SUBCAT_DESC] [varchar](2000) NOT NULL,
	[PROD_CATEGORY] [varchar](50) NOT NULL,
	[PROD_CAT_DESC] [varchar](2000) NOT NULL,
	[PROD_STATUS] [varchar](20) NOT NULL,
	[PROD_LIST_PRICE] [decimal](8, 2) NOT NULL,
	[PROD_MIN_PRICE] [decimal](8, 2) NOT NULL,
)
GO

ALTER TABLE [STAGE_DW].[Products]
ADD CONSTRAINT PK_Products PRIMARY KEY ([PROD_ID]);
GO

CREATE TABLE [STAGE_DW].[Channels](
	[CHANNEL_ID] [int] NOT NULL PRIMARY KEY,
	[CHANNEL_DESC] [varchar](20) NOT NULL,
	[CHANNEL_CLASS] [varchar](20) NULL,
) 
GO

DROP TABLE IF EXISTS [STAGE_DW].[SALES];
GO 
CREATE TABLE [STAGE_DW].[SALES](
	[SALESTRANS_ID] [int] NOT NULL PRIMARY KEY,
	[ORDER_ID] [int] NULL,
	[PROD_ID] [int] NULL,
	[CUST_ID] [int] NULL,
	[DateID] INT NOT NULL,
	[CHANNEL_ID] [int] NULL,
	[PROMO_ID] [int] NULL,
	[SHIPPING_DATE] [datetime] NULL,
	[QUANTITY_SOLD] [int] NULL,
	[AMOUNT_SOLD] [decimal](8, 2) NULL,
	[UNIT_PRICE] [decimal](8, 2) NULL,
	[SALE_DATE] [datetime] NULL,
	[PAYMENT_DATE] [datetime] NULL,
) 
GO

-- Manually Importing the data set from the source database into my own database schema


INSERT INTO [STAGE_DW].[Customers] (
    CUST_ID, CUST_NAME, CUST_GENDER, CUST_MAIN_PHONE_NUMBER, CUST_EMAIL, 
    CUST_STREET_ADDRESS, CUST_POSTAL_CODE, CUST_CITY, COUNTRY_ID, COUNTRY_NAME, 
    COUNTRY_SUBREGION, COUNTRY_REGION, CUST_STATE_PROVINCE, CUST_EDUCATION, 
    CUST_MARITAL_STATUS, RACE, INCOME, NETWORTH
)
SELECT 
    ci.CUST_ID, 
    ci.CUST_NAME, 
    ci.CUST_GENDER, 
    ci.CUST_MAIN_PHONE_NUMBER,
    ci.CUST_EMAIL, 
    ci.CUST_STREET_ADDRESS,
    ci.CUST_POSTAL_CODE, 
    ci.CUST_CITY, 
    ci.COUNTRY_ID,
    co.COUNTRY_NAME,
    co.COUNTRY_SUBREGION,
    co.COUNTRY_REGION,
    ci.CUST_STATE_PROVINCE,
    ce.CUST_EDUCATION,
    ce.CUST_MARITAL_STATUS,
    ce.RACE,
    ce.INCOME,
    ce.NETWORTH
FROM 
    [IST346].[dbo].[T_Customers_Int] AS ci
JOIN 
    [IST346].[dbo].[T_Customers_Ext] AS ce ON ci.CUST_ID = ce.CUST_ID
LEFT JOIN 
    [IST346].[dbo].[T_Countries] AS co ON ci.COUNTRY_ID = co.COUNTRY_ID;


INSERT INTO [STAGE_DW].[Products] (
    PROD_ID, PROD_NAME, PROD_DESC, PROD_WEIGHT_CLASS, PROD_UNIT_OF_MEASURE, 
    PROD_PACK_SIZE, SUPPLIER_ID, PROD_SUBCATEGORY, PROD_SUBCAT_DESC, 
    PROD_CATEGORY, PROD_CAT_DESC, PROD_STATUS, PROD_LIST_PRICE, PROD_MIN_PRICE
)
SELECT 
    p.PROD_ID, 
    p.PROD_NAME, 
    p.PROD_DESC, 
    p.PROD_WEIGHT_CLASS,
    p.PROD_UNIT_OF_MEASURE, 
    p.PROD_PACK_SIZE, 
    p.SUPPLIER_ID, 
    sc.[PROD_SUBCATEGORY],
	sc.PROD_SUBCAT_DESC,
    c.PROD_CATEGORY,
    c.PROD_CAT_DESC,
    p.PROD_STATUS,
    p.PROD_LIST_PRICE AS PROD_LIST_PRICE,
    p.PROD_MIN_PRICE AS PROD_MIN_PRICE
FROM 
    [IST346].[dbo].[T_Products] AS p
JOIN 
    [IST346].[dbo].[T_Subcategories] AS sc ON p.PROD_SUBCATEGORY_ID = sc.PROD_SUBCATEGORY_ID
JOIN 
    [IST346].[dbo].[T_Categories] AS c ON sc.PROD_CATEGORY_ID = c.PROD_CATEGORY_ID;

-- creating a CTE to create a combined table to import the New Sales table,
-- found only in the destination database schema
WITH CombinedData AS (
    SELECT 
        o.ORDER_ID,
        oi.PROD_ID,
        o.CUST_ID,
        CONVERT(INT, FORMAT(o.SALE_DATE, 'yyyyMMdd')) AS DateID,
        o.CHANNEL_ID,
        o.PROMO_ID,
        oi.SHIPPING_DATE,
        oi.QUANTITY_SOLD,
        oi.AMOUNT_SOLD,
        oi.UNIT_PRICE,
        o.SALE_DATE,
        o.PAYMENT_DATE
    FROM 
        [IST346].[dbo].[T_Orders_12_13] AS o
    JOIN 
        [IST346].[dbo].[T_Order_Items_12_13] AS oi ON o.ORDER_ID = oi.ORDER_ID

    UNION ALL

    SELECT 
        o.ORDER_ID,
        oi.PROD_ID,
        o.CUST_ID,
        CONVERT(INT, FORMAT(o.SALE_DATE, 'yyyyMMdd')) AS DateID,
        o.CHANNEL_ID,
        o.PROMO_ID,
        oi.SHIPPING_DATE,
        oi.QUANTITY_SOLD,
        oi.AMOUNT_SOLD,
        oi.UNIT_PRICE,
        o.SALE_DATE,
        o.PAYMENT_DATE
    FROM 
        [IST346].[dbo].[T_Orders_14] AS o
    JOIN 
        [IST346].[dbo].[T_Order_Items_14] AS oi ON o.ORDER_ID = oi.ORDER_ID
)

INSERT INTO [STAGE_DW].[SALES] (
    SALESTRANS_ID, ORDER_ID, PROD_ID, CUST_ID, DateID, CHANNEL_ID, PROMO_ID, 
    SHIPPING_DATE, QUANTITY_SOLD, AMOUNT_SOLD, UNIT_PRICE, SALE_DATE, PAYMENT_DATE
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY ORDER_ID) AS SALESTRANS_ID,
    ORDER_ID,
    PROD_ID,
    CUST_ID,
    DateID,
    CHANNEL_ID,
    PROMO_ID,
    SHIPPING_DATE,
    QUANTITY_SOLD,
    AMOUNT_SOLD,
    UNIT_PRICE,
    SALE_DATE,
    PAYMENT_DATE
FROM CombinedData;


INSERT INTO [STAGE_DW].[Promotions] (
    PROMO_ID, PROMO_NAME, PROMO_SUBCATEGORY, PROMO_CATEGORY, 
    PROMO_COST, PROMO_BEGIN_DATE, PROMO_END_DATE
)
SELECT 
    PROMO_ID,
    PROMO_NAME,
    PROMO_SUBCATEGORY,
    PROMO_CATEGORY,
    PROMO_COST,
    PROMO_BEGIN_DATE,
    PROMO_END_DATE
FROM 
    IST346.[dbo].[T_Promotions];

INSERT INTO [STAGE_DW].[Dim_Date] (
    DateID, DateValue, Year, Quarter, Month, Week, Day, DayName, FiscalYear, FiscalQuarter
)
SELECT 
    CAST(CONVERT(CHAR(8), d.DateValue, 112) AS INT) AS DateID,
    d.DateValue,
    YEAR(d.DateValue) AS Year,
    DATEPART(QUARTER, d.DateValue) AS Quarter,
    MONTH(d.DateValue) AS Month,
    DATEPART(WEEK, d.DateValue) AS Week,
    DAY(d.DateValue) AS Day,
    DATENAME(WEEKDAY, d.DateValue) AS DayName,
    YEAR(d.DateValue) AS FiscalYear, 
    DATEPART(QUARTER, d.DateValue) AS FiscalQuarter
FROM 
    (SELECT TOP 3650 
        DATEADD(DAY, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1, CAST('2020-01-01' AS DATE)) AS DateValue
     FROM master.dbo.spt_values) AS d;


-- Creation of Summary tables to implement query optimization and frequent user queries 
--- Products Summary Table
CREATE TABLE [STAGE_DW].[Product_Sales_Summary] (
    PROD_ID INT NOT NULL,
    Year INT NOT NULL,
    Quarter INT NOT NULL,
    Total_Quantity_Sold INT NOT NULL,
    Total_Amount_Sold DECIMAL(12, 2) NOT NULL,
    PRIMARY KEY (PROD_ID, Year, Quarter)
);

----- Customer Sales Summary table
CREATE TABLE [STAGE_DW].[Customer_Sales_Summary] (
    CUST_ID INT NOT NULL,
    Year INT NOT NULL,
    Total_Sales DECIMAL(12, 2) NOT NULL,
    Total_Orders INT NOT NULL,
    PRIMARY KEY (CUST_ID, Year)
);

-- Populating the two summary tables for vizualization later
INSERT INTO [STAGE_DW].[Product_Sales_Summary] (
    PROD_ID, 
    Year, 
    Quarter, 
    Total_Quantity_Sold, 
    Total_Amount_Sold
)
SELECT 
    s.PROD_ID,
    YEAR(s.SALE_DATE) AS Year,
    DATEPART(QUARTER, s.SALE_DATE) AS Quarter,
    SUM(s.QUANTITY_SOLD) AS Total_Quantity_Sold,
    SUM(s.AMOUNT_SOLD) AS Total_Amount_Sold
FROM [STAGE_DW].[SALES] s
GROUP BY 
    s.PROD_ID, 
    YEAR(s.SALE_DATE), 
    DATEPART(QUARTER, s.SALE_DATE);

-- Customer summary table population
INSERT INTO [STAGE_DW].[Customer_Sales_Summary] (
    CUST_ID, 
    Year, 
    Total_Sales, 
    Total_Orders
)
SELECT 
    s.CUST_ID,
    YEAR(s.SALE_DATE) AS Year,
    SUM(s.AMOUNT_SOLD) AS Total_Sales,
    COUNT(DISTINCT s.ORDER_ID) AS Total_Orders
FROM [STAGE_DW].[SALES] s
GROUP BY 
    s.CUST_ID, 
    YEAR(s.SALE_DATE);
-------------------------------------------------
--- Querying the result of the summary tables 
SELECT 
    Year,
    COUNT(*) as Products_Each_Year,
    sum(Total_Quantity_Sold) as Total_Quantity_Sold,
    SUM(Total_Amount_Sold) as Total_Amount_Sold
FROM 
    [STAGE_DW].[Product_Sales_Summary]
	GROUP BY [Year]
ORDER BY 
    Year ASC;

SELECT 
    Year,
	COUNT(*) as new_customers,
    SUM(Total_Sales) as Total_Sales,
    SUM(Total_Orders) as Total_Orders
FROM 
    [STAGE_DW].[Customer_Sales_Summary]
GROUP BY [Year]
ORDER BY 
    Year ASC;
-------------------------------------------------
-- Altering Agent table to include Order_ID feature to track Agents performance
ALTER TABLE [STAGE_DW].[Agents]
ADD Orders_ID [int] NULL;  -- Change datatype as needed

UPDATE ag
SET ag.Orders_ID = ags.ORDER_ID
FROM [STAGE_DW].[Agents] ag
JOIN [IST346].[dbo].[T_Agent_Sales] ags
    ON ag.AGENT_ID = ags.AGENT_ID;  -- Ensure a matching key

---------- Creating indexes for query optimization and server resource allocation
CREATE INDEX idx_agent_sales_order ON [STAGE_DW].[Agents] (Orders_ID);
CREATE INDEX idx_customer_region ON [STAGE_DW].[Customers] (CUST_ID, COUNTRY_REGION);
GO
-------------------------------------------------------
----- Creating a Sales View to track Agent sales performance over time
CREATE OR ALTER VIEW [STAGE_DW].[TOP_SALES_REPS] AS
SELECT 
    a.AGENT_ID,
    cu.COUNTRY_REGION AS TERRITORY,
	SUM(SUM(COALESCE(s.AMOUNT_SOLD, 0))) OVER (PARTITION BY a.AGENT_ID) AS AGENT_TOTAL_SALES  -- Total per Agent
FROM [STAGE_DW].[SALES] s
JOIN [STAGE_DW].[Agents] a 
    ON s.ORDER_ID = a.Orders_ID  
JOIN [STAGE_DW].[Customers] cu 
    ON s.CUST_ID = cu.CUST_ID
GROUP BY 
    a.AGENT_ID,
    cu.COUNTRY_REGION;
GO
------- querying view 
SELECT * FROM [STAGE_DW].[TOP_SALES_REPS]
-- GROUP BY TERRITORY
ORDER BY AGENT_TOTAL_SALES DESC
GO

-------- Creating View to track Promotion performance on sales

CREATE VIEW [STAGE_DW].V_MARKETING_CAMPAIGNS AS
SELECT 
    pr.PROMO_NAME,
    pr.PROMO_COST,
    SUM(s.AMOUNT_SOLD) AS TOTAL_SALES
FROM [STAGE_DW].[SALES] s
JOIN [STAGE_DW].[Promotions] pr 
    ON s.PROMO_ID = pr.PROMO_ID
GROUP BY 
    pr.PROMO_NAME,
    pr.PROMO_COST;
GO