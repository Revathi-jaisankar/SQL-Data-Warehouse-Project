/* ==========================================================================================
Stored Procedure: Load Bronze Layer(Source -> Bronze)
==============================================================================================
Script Purpose:

This Stored Procedure loads data into the bronze schema from external csv files.
It performs following tasks;
  1.Truncates the bronze table before load the data.
  2. Uses the 'Bulk Insert' command to load data from csv files to bronze tables.

Parameter:
  None.
This Stored Procedure does not accept any parameter or return any values.

Usage Example:
  Exec Bronze.load_
================================================================================================
*/

-- To create a Stored Procedures

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
		Declare @start_time DATETIME, @end_Time DATETIME, @batch_start_Time DATETIME, @batch_end_time DATETIME
		BEGIN TRY
				SET @batch_start_Time = GETDATE();
				Print '=============================================================';
				Print 'Loading Bronze Layer';
				Print '=============================================================';

				Print '-------------------------------------------------------------';
				Print 'Loading CRM Tables';
				Print '--------------------------------------------------------------';

				SET @start_time = GETDATE();
				Print '>>: Truncate bronze.crm_cust_info';
				Truncate Table bronze.crm_cust_info

				Print '>>: Insert bronze.crm_cust_info';
				BULK INSERT bronze.crm_cust_info
				From 'E:\4. SQL (Codebasics)\Data with Baraa\Projects_MyNotes\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
				with (
					Firstrow = 2,
					FieldTerminator = ',',
					Tablock
				); --Full load means Truncate the table and load the full data (select count(*) from bronze.crm_cust_info;)
				SET @end_time = GETDATE();
				Print '>> Load Duration: ' + Cast(DATEDIFF (Second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
				Print '-----------------';

				SET @start_time = GETDATE();
				Print '>>: Truncate bronze.crm_prd_info';
				Truncate Table bronze.crm_prd_info

				Print '>>: Insert bronze.crm_prd_info';
				BULK INSERT bronze.crm_prd_info
				From 'E:\4. SQL (Codebasics)\Data with Baraa\Projects_MyNotes\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
				with (
					Firstrow = 2,
					FieldTerminator = ',',
					Tablock
				); --Full load means Truncate the table and load the full data (select count(*) from bronze.crm_prd_info;)
				SET @end_time = GETDATE();
				Print '>> Load Duration: ' + Cast(DATEDIFF (Second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
				Print '-----------------';

				SET @start_time = GETDATE();
				Print '>>: Truncate bronze.crm_sales_details';
				Truncate Table bronze.crm_sales_details

				Print '>>: Insert bronze.crm_sales_details';
				BULK INSERT bronze.crm_sales_details
				From 'E:\4. SQL (Codebasics)\Data with Baraa\Projects_MyNotes\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
				with (
					Firstrow = 2,
					FieldTerminator = ',',
					Tablock
				); --Full load means Truncate the table and load the full data (select count(*) from bronze.crm_sales_details;)
				SET @end_time = GETDATE();
				Print '>> Load Duration: ' + Cast(DATEDIFF (Second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
				Print '-----------------';


				Print '-------------------------------------------------------------';
				Print 'Loading ERP Tables';
				Print '--------------------------------------------------------------';

				SET @start_time = GETDATE();
				Print '>>: Truncate bronze.erp_cust_az12';
				Truncate Table bronze.erp_cust_az12

				Print '>>: Insert bronze.erp_cust_az12';
				BULK INSERT bronze.erp_cust_az12
				From 'E:\4. SQL (Codebasics)\Data with Baraa\Projects_MyNotes\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
				with (
					Firstrow = 2,
					FieldTerminator = ',',
					Tablock
				); --Full load means Truncate the table and load the full data (select count(*) from bronze.erp_cust_az12;)
				SET @end_time = GETDATE();
				Print '>> Load Duration: ' + Cast(DATEDIFF (Second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
				Print '-----------------';


				SET @start_time = GETDATE();
				Print '>>: Truncate bronze.erp_loc_a101';
				Truncate Table bronze.erp_loc_a101

				Print '>>: Insert bronze.erp_loc_a101';
				BULK INSERT bronze.erp_loc_a101
				From 'E:\4. SQL (Codebasics)\Data with Baraa\Projects_MyNotes\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
				with (
					Firstrow = 2,
					FieldTerminator = ',',
					Tablock
				); --Full load means Truncate the table and load the full data (select count(*) from bronze.erp_loc_a101;)
				SET @end_time = GETDATE();
				Print '>> Load Duration: ' + Cast(DATEDIFF (Second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
				Print '-----------------';


				SET @start_time = GETDATE();
				Print '>>: Truncate bronze.erp_px_cat_g1v2';
				Truncate Table bronze.erp_px_cat_g1v2

				Print '>>: Insert bronze.erp_px_cat_g1v2';
				BULK INSERT bronze.erp_px_cat_g1v2
				From 'E:\4. SQL (Codebasics)\Data with Baraa\Projects_MyNotes\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
				with (
					Firstrow = 2,
					FieldTerminator = ',',
					Tablock
				); --Full load means Truncate the table and load the full data (select count(*) from bronze.erp_px_cat_g1v2;)
				SET @end_time = GETDATE();

				Print '>> Load Duration: ' + Cast(DATEDIFF (Second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
				Print '>> -----------------';

				SET @batch_end_Time = GETDATE();
				Print '================================================================';
				Print 'Loading Bronze Layer is complted';
				Print '- Total Load Duration: ' + Cast(DATEDIFF (Second, @batch_start_Time, @batch_end_Time) AS NVARCHAR) + ' seconds';
				Print '================================================================';

			END TRY 
			BEGIN CATCH
					Print '================================================================';
					Print 'ERROR OCCURED DURING LOADING BRONZE LAYER';
					Print 'Error Message' + Error_Message();
					Print 'Error Message' + CAST (Error_Number() as NVARCHAR);
					Print 'Error Message' + CAST (Error_STATE() as NVARCHAR);
					PRINT '================================================================';
			END CATCH

END


--TO check the stored Procedure
