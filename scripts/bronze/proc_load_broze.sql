--Full project script

/*
================================================================================
Data Warehouse Initialization Script
================================================================================
Purpose:
    This script creates a new database named 'DataWarehouse_Project'.
    If the database already exists, it will be safely dropped and recreated.

    After creating the database, the script sets up three schemas that represent
    the typical layers of a modern Data Warehouse architecture:
        - bronze : Raw data layer
        - silver : Cleaned and transformed data layer
        - gold   : Business-ready, curated data layer

Warning:
    Running this script will DROP the entire 'DataWarehouse_Project' database 
    if it already exists. Make sure you have backups before executing.
================================================================================
*/

USE master;
GO

-- Drop the database if it already exists
IF EXISTS (SELECT * FROM sys.databases WHERE name = 'DataWarehouse_Project')
BEGIN
    ALTER DATABASE DataWarehouse_Project 
        SET SINGLE_USER WITH ROLLBACK IMMEDIATE;

    DROP DATABASE DataWarehouse_Project;
END;
GO

-- Create the Data Warehouse database
CREATE DATABASE DataWarehouse_Project;
GO

-- Switch to the new database
USE DataWarehouse_Project;
GO

-- Create schemas representing the DW layers
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO


-- Broze Schema
-- Exists check
IF OBJECT_ID ('bronze.crm_cust_info','U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;
GO
-- Create crm Table 
--cust_info
CREATE TABLE bronze.crm_cust_info (
    cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_marital_status  NVARCHAR(50),
    cst_gndr            NVARCHAR(50),
    cst_create_date NVARCHAR(50)
);
GO

-- Exists check
IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;
GO

CREATE TABLE bronze.crm_prd_info (
    prd_id       INT,
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME
);
GO

-- Exists check
IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
GO

CREATE TABLE bronze.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);
GO

--Create erp table
-- Exist check
IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE bronze.erp_loc_a101;
GO

CREATE TABLE bronze.erp_loc_a101 (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50)
);
GO

-- Exist check
IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE bronze.erp_cust_az12;
GO

CREATE TABLE bronze.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  NVARCHAR(50),
    gen    NVARCHAR(50)
);
GO

-- Exist check

IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2;
GO

CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50)
);
GO

-- upload data from csv to the table
-- TO STORE PROCEDURE THE CODE
/* 
===========================================================
DDl script create broze tables
===========================================================
purpose of the script:
    create new tables or drope the existing tables if exist and createing 
    the new data
    and make procedure to run it without right the script every time
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    
    DECLARE @starttime DATETIME, @endtime DATETIME , @brozestarttime DATETIME , @bronzeendtime DATETIME
    BEGIN TRY
        SET @brozestarttime = GETDATE ();
        PRINT '============================';
        PRINT 'Loading bronze layer';
        PRINT '============================';

        PRINT '-----------------------';
        PRINT 'Load CRM Tables';
        PRINT '-----------------------';
        SET @starttime = GETDATE();
        PRINT ' >> Truncate table ...'
        TRUNCATE TABLE [bronze].[crm_cust_info]
        BULK INSERT bronze.crm_cust_info
        FROM 'D:\wadah\SQL\baraa\sql-ultimate-course-main\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH
        (
	        FIRSTROW = 2,
	        FIELDTERMINATOR = ',', 
	        TABLOCK
        );
        SET @endtime = GETDATE();
        PRINT '>>>> Load duration : ' + CAST(DATEDIFF(second, @starttime,@endtime) AS NVARCHAR) + ' s'
        SET @starttime = GETDATE ();
        PRINT ' >> Truncate table ...'
        TRUNCATE TABLE  [bronze].[crm_prd_info]
        BULK INSERT [bronze].[crm_prd_info]
        FROM 'D:\wadah\SQL\baraa\sql-ultimate-course-main\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
            );
        SET @endtime = GETDATE ();
        PRINT ' >>>> Load duration : ' + CAST (DATEDIFF ( SECOND , @starttime, @endtime) AS NVARCHAR) + ' s'
    
        PRINT ' >> Truncate table ...'
        TRUNCATE TABLE  [bronze].[crm_sales_details]
        BULK INSERT [bronze].[crm_sales_details]
        FROM 'D:\wadah\SQL\baraa\sql-ultimate-course-main\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
    
        PRINT '-----------------------';
        PRINT 'Load ERP Tables';
        SET @starttime = GETDATE();
        PRINT ' >> Truncate table ...'
        TRUNCATE TABLE  [bronze].[erp_cust_az12]
        BULK INSERT [bronze].[erp_cust_az12]
        FROM 'D:\wadah\SQL\baraa\sql-ultimate-course-main\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @endtime = GETDATE();
        PRINT ' >>>> Load duration : ' + CAST ( DATEDIFF ( SECOND, @starttime, @endtime) AS NVARCHAR);
        SET @starttime = GETDATE();
        PRINT ' >> Truncate table ...'
        TRUNCATE TABLE  [bronze].[erp_loc_a101]
        BULK INSERT [bronze].[erp_loc_a101]
        FROM 'D:\wadah\SQL\baraa\sql-ultimate-course-main\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
            );
        SET @starttime = GETDATE ();
        PRINT ' >> Truncate table ...'
        TRUNCATE TABLE  [bronze].[erp_px_cat_g1v2]
        BULK INSERT [bronze].[erp_px_cat_g1v2]
        FROM 'D:\wadah\SQL\baraa\sql-ultimate-course-main\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @endtime = GETDATE ();
        PRINT ' >>> Load duration: ' + CAST ( DATEDIFF ( SECOND , @starttime , @endtime) AS nvarchar) + ' s'
        SET @bronzeendtime = GETDATE();
        PRINT ' >>> Load duration for broze lyer : ' + CAST (DATEDIFF ( SECOND , @brozestarttime , @bronzeendtime) AS NVARCHAR) + ' s'
        PRINT ' ..........'
    END TRY
    BEGIN CATCH
        PRINT '===============================';
        PRINT ' ERROR OCCURED DURING LOAD BRONZE LAYER';
        PRINT ' ERROR MESSAGE :' + ERROR_MESSAGE();
        PRINT 'ERROR NUMBER : ' + CAST ( ERROR_NUMBER () AS NVARCHAR);
        PRINT 'ERROR NUMBER : ' + CAST ( ERROR_STATE () AS NVARCHAR);
        PRINT '===============================';
    END CATCH
   
    
END
/* to run the stored procedures

EXEC bronze.load_bronze
*/

