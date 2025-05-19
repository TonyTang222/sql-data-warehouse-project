/*
===============================================================================
Stored Procedure: Load Silver Layer (Source -> Silver)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'silver' schema from external CSV files. 
    It performs the following actions:
    - Truncates the silver tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to silver tables.

Parameters:
    None. 
      This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
    BEGIN TRY
        SET @batch_start_time = GETDATE()
        PRINT '==================================';
        PRINT 'Loading Silver Layer';
        PRINT '==================================';

        PRINT '----------------------------------';
        PRINT 'Loading CRM Table';
        PRINT '----------------------------------';

        SET @start_time = GETDATE()
        PRINT '<<<<<Truncating Table:silver.crm_cust_info>>>>>';
        TRUNCATE TABLE silver.crm_cust_info;

        PRINT '<<<<<Inserting Data:silver.crm_cust_info>>>>>';
        WITH most_recent_cus AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rank_flag
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
        )
        INSERT INTO silver.crm_cust_info (
            cst_id, 
			cst_key, 
			cst_firstname, 
			cst_lastname, 
			cst_marital_status, 
			cst_gndr,
			cst_create_date
        )
        SELECT 
        cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname) AS cst_lastname,
        CASE
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'N/A'
        END AS cst_marital_status,
        CASE
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            ELSE 'N/A'
        END AS cst_gndr,
        cst_create_date
        FROM most_recent_cus
        WHERE rank_flag = 1;
        SET @end_time = GETDATE()
        PRINT 'Loading Time: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';

        PRINT '----------------------------------';
        PRINT 'Loading CRM Table';
        PRINT '----------------------------------';

        SET @start_time = GETDATE()
        PRINT '<<<<<Truncating Table:silver.crm_prd_info>>>>>';
        TRUNCATE TABLE silver.crm_prd_info;

        PRINT '<<<<<Inserting Data:silver.crm_prd_info>>>>>'
        INSERT INTO silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_name,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt 
        )
        SELECT 
        prd_id,
        replace(substring(prd_key,1,5),'-','_') AS cat_id,
        substring(prd_key,7,len(prd_key)) AS prd_key,
        prd_name,
        ISNULL(prd_cost,0) AS prd_cost,
        CASE
                WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
                WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
                WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other sales'
                WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
                ELSE 'N/A'
            END AS prd_line,
        CAST(prd_start_dt AS DATE) AS prd_start_dt,
        CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS DATE) AS prd_end_dt
        FROM bronze.crm_prd_info;
        SET @end_time = GETDATE()
        PRINT 'Loading Time: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';

        PRINT '----------------------------------';
        PRINT 'Loading CRM Table';
        PRINT '----------------------------------';

        SET @start_time = GETDATE()
        PRINT '<<<<<Truncating Table:silver.crm_sales_details>>>>>';
        TRUNCATE TABLE silver.crm_sales_details;

        PRINT '<<<<<Inserting Data:silver.crm_sales_details>>>>>'
        INSERT INTO silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
        )
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE 
                WHEN sls_order_dt = 0 or LEN(sls_order_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
            END AS sls_order_dt,
            CASE 
                WHEN sls_ship_dt = 0 or LEN(sls_ship_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
            END AS sls_ship_dt,
            CASE 
                WHEN sls_due_dt = 0 or LEN(sls_due_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
            END AS sls_due_dt,
            CASE
                WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price) THEN ABS(sls_price) * sls_quantity
                ELSE sls_sales
            END AS sls_sales,
            sls_quantity,
            CASE
                WHEN sls_price IS NULL OR sls_price <=0 THEN sls_sales / NULLIF(sls_quantity,0)
                ELSE sls_price
            END AS sls_price
        FROM bronze.crm_sales_details;
        SET @end_time = GETDATE()
        PRINT 'Loading Time: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';

        PRINT '----------------------------------';
        PRINT 'Loading ERP Table';
        PRINT '----------------------------------';

        SET @start_time = GETDATE()
        PRINT '<<<<<Truncating Table:silver.erp_cust_az12>>>>>';
        TRUNCATE TABLE silver.erp_cust_az12;

        PRINT '<<<<<Inserting Data:silver.erp_cust_az12>>>>>'
        INSERT INTO silver.erp_cust_az12 (
                    cid,
                    bdate,
                    gen
        )
        SELECT
            CASE
                WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
                ELSE cid
            END AS cid,
            CASE
                WHEN bdate > GETDATE() THEN NULL
                ELSE bdate
            END AS bdate,
            CASE
            WHEN REPLACE(LTRIM(RTRIM(UPPER(gen))), CHAR(13), '') = 'M' 
                OR REPLACE(LTRIM(RTRIM(UPPER(gen))), CHAR(13), '') = 'MALE' THEN 'Male'
            WHEN REPLACE(LTRIM(RTRIM(UPPER(gen))), CHAR(13), '') = 'F' 
                OR REPLACE(LTRIM(RTRIM(UPPER(gen))), CHAR(13), '') = 'FEMALE' THEN 'Female'
            ELSE 'N/A'
        END AS gen
        FROM bronze.erp_cust_az12;
        SET @end_time = GETDATE()
        PRINT 'Loading Time: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';

        PRINT '----------------------------------';
        PRINT 'Loading ERP Table';
        PRINT '----------------------------------';

        SET @start_time = GETDATE()
        PRINT '<<<<<Truncating Table:silver.erp_loc_a101>>>>>';
        TRUNCATE TABLE silver.erp_loc_a101;

        PRINT '<<<<<Inserting Data:silver.erp_loc_a101>>>>>'
        INSERT INTO silver.erp_loc_a101 (
                    cid,
                    cntry
        )
        SELECT
            REPLACE(cid, '-', '') AS cid, 
            CASE
                WHEN REPLACE(REPLACE(TRIM(cntry), CHAR(10), ''), CHAR(13), '') = 'DE' THEN 'Germany'
                WHEN REPLACE(REPLACE(TRIM(cntry), CHAR(10), ''), CHAR(13), '') IN ('US', 'USA') THEN 'United States'
                WHEN REPLACE(REPLACE(TRIM(cntry), CHAR(10), ''), CHAR(13), '') = '' OR cntry IS NULL THEN 'N/A'
                ELSE REPLACE(REPLACE(TRIM(cntry), CHAR(10), ''), CHAR(13), '')
            END AS cntry
        FROM bronze.erp_loc_a101;
        SET @end_time = GETDATE()
        PRINT 'Loading Time: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';

        PRINT '----------------------------------';
        PRINT 'Loading ERP Table';
        PRINT '----------------------------------';

        SET @start_time = GETDATE()
        PRINT '<<<<<Truncating Table:silver.erp_px_cat_g1v2>>>>>';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        PRINT '<<<<<Inserting Data:silver.erp_px_cat_g1v2>>>>>'
        INSERT INTO silver.erp_px_cat_g1v2 (
			id,
			cat,
			subcat,
			maintenance
        )
        SELECT 
        id,
        cat,
        subcat,
        CASE
            WHEN REPLACE(REPLACE(TRIM(maintenance), CHAR(10), ''), CHAR(13), '') = 'YES' THEN 'Yes'
            WHEN REPLACE(REPLACE(TRIM(maintenance), CHAR(10), ''), CHAR(13), '') = 'NO' THEN 'No'
            ELSE maintenance
        END AS maintenance
        FROM bronze.erp_px_cat_g1v2;
        SET @end_time = GETDATE()
        PRINT 'Loading Time: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
        SET @batch_end_time = GETDATE()
        PRINT '-----------------------------------'
        PRINT 'Loading Silver Layer is Completed';
        PRINT 'Total Load Duration: ' + CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '-----------------------------------'
    END TRY
    BEGIN CATCH
        PRINT '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!'
        PRINT 'Error occured during loading silver layer'
        PRINT '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!'
    END CATCH
END