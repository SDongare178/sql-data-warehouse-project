/*
STORED PROCEDURE - Load Bronze Layer from Source to Bronze
=================================================
- This is a stored procedure to load data into bronxe schema from csv files.
-It truncates the table before loadind data so no duplication happens
-we use 'BULK INSERT' command to load data in a single row rather than one row at a time.

Parameters - 
-None
-This stored procedure does not accept any parameters or retun any value

TO USE - 
   EXEC bronze.load_bronze;
===================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS

BEGIN
DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		
		SET @batch_start_time = GETDATE();
		PRINT 'loadig Beonze layer';
		PRINT'+===================';
		PRINT 'Laodind CRM TABLE';
		PRINT'====================';

		SET @start_time = GETDATE(); 
		PRINT'>> TRUNCATING TABLE:cust_info';
		TRUNCATE TABLE bronze.crm_cust_info --to delete entries avoid duplicates
		PRINT'>> INSERTING TABLE:cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'D:\Projects\P-1 Data Warehouse\datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK    
		);
		SET @end_time = GETDATE(); 
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'; 
		PRINT'================'


		SET @start_time = GETDATE();
		PRINT'>> TRUNCATING TABLE:prod_info';
		TRUNCATE TABLE bronze.crm_prd_info --to delete entries avoid duplicates
		PRINT'>> INSERTING TABLE:prod_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\Projects\P-1 Data Warehouse\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK    
		);
		SET @end_time = GETDATE(); 
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'; 
		PRINT'================'

		SET @start_time = GETDATE(); 
		PRINT'>> TRUNCATING TABLE:sales_details';
		TRUNCATE TABLE bronze.crm_sales_details --to delete entries avoid duplicates
		PRINT'>> INSERTING TABLE:sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\Projects\P-1 Data Warehouse\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK    
		);
		SET @end_time = GETDATE(); 
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'; 
		PRINT'================'



		PRINT 'Laoding ERP TABLE';
		SET @start_time = GETDATE(); 
		PRINT'>> TRUNCATING TABLE: cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12 --to delete entries avoid duplicates
		PRINT'>> INSERTING TABLE: cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\Projects\P-1 Data Warehouse\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK    
		);
		SET @end_time = GETDATE(); 
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'; 
		PRINT'================'

		SET @start_time = GETDATE(); 
		PRINT'>> TRUNCATING TABLE:erp loc';
		TRUNCATE TABLE bronze.erp_loc_a101 --to delete entries avoid duplicates
		PRINT'>> INSERTING TABLE:erp loc';
		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\Projects\P-1 Data Warehouse\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK    
		);
		SET @end_time = GETDATE(); 
		PRINT'>> Load Duration: '  + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds'; 
		PRINT'================'


		SET @start_time = GETDATE(); 
		PRINT'>> TRUNCATING TABLE:px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2 --to delete entries avoid duplicates
		PRINT'>> INSERTING TABLE:px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\Projects\P-1 Data Warehouse\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK    
		);
		SET @end_time = GETDATE(); 
		PRINT'>> Load Duration:'  + CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR) + 'seconds'; 
	
		PRINT'=========================='
		SET @batch_end_time = GETDATE();
		PRINT'=========================='
		PRINT'Loading Bronze layer is Complete';
		PRINT'     - Total Load Duration:' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT'=========================='
	END TRY
	BEGIN CATCH
		PRINT'===========================';
		PRINT'ERROR DURING LOADING BRONZE LAYER';
		PRINT'Error Message + ERROR_MESSAGE ()';
		PRINT'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);

		PRINT'===========================';
	END CATCH
END




