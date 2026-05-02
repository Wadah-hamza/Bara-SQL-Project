/* 
===========================================================
DDl script creates browse tables
===========================================================
Purpose of the script:
    create new tables or drop the existing tables if they exist, and create 
    the new data
    and make a procedure to run it without running the script every time
    ===========================================================
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
/* to run the stored procedures, just highlight the EXEC

EXEC bronze.load_bronze
*/
