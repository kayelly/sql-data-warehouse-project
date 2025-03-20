/*
===================================================================================
Quality Checks
===================================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy,
    and standardization across the 'silver' schemas. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after loading Silver Layer. 
    - Investigate and resolve any discrepancies found during the checks.
===================================================================================
*/


===================================================================================
  Checking 'silver.crm_cust_info'
===================================================================================


-- Check for NULLS or Duplicates in Primary Key 
-- Expectation: No Result

SELECT 
cst_id, 
COUNT(*) 
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1
OR cst_id IS NULL;

--------------------------------------
SELECT 
*
FROM silver.crm_cust_info
WHERE cst_id = 29466;


--Remove duplicates
SELECT 
* 
FROM (
		SELECT 
		*,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM bronze.crm_cust_info
		) t
WHERE flag_last = 1


--Check for unwanted spaces
-- Expectation: No Results 
SELECT 
cst_firstname
FROM
silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)
--------------------------------------------

SELECT 
cst_lastname
FROM 
silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)


SELECT 
cst_marital_status
FROM 
silver.crm_cust_info
WHERE cst_marital_status != TRIM(cst_marital_status)



-- Check the consisteny of values in low cardinality columns 
-- Data Standardization & Consistency 
SELECT 
DISTINCT cst_marital_status
FROM 
silver.crm_cust_info




===================================================================================
  Checking 'silver.crm_prd_info'
===================================================================================
-------QUALITY CHECKS---- 
-- Check for NULLS or Duplicates in Primary Key 
-- Expectation: No Result

SELECT 
prd_id, 
COUNT(*) 
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1
OR prd_id IS NULL;


-- Check for unwanted spaces
-- Expectation: No Result
SELECT
prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);


-- Check for NULLs or negative numbers 
-- Expectation: No Result
SELECT
*
FROM silver.crm_prd_info
WHERE prd_cost IS NULL 
OR prd_cost < 0 ;

-- Data Standardization & Consistency
-- Replace the abbreviations with user friendly names
SELECT
DISTINCT prd_line
FROM silver.crm_prd_info


-- Check Invalid Date Orders
-- Using end_date < start_date, end_date must not be earlier than the start date  c
SELECT
*
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt


-- Check the Table 
SELECT
*
FROM silver.crm_prd_info;


===================================================================================
  Checking 'silver.crm_sales_details'
===================================================================================
-------QUALITY CHECKS---- 
-- Check for unwanted spaces
-- Expectation: No Result
SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num);



-- sls Prd key to link Prd_info
-- check if there are keys in sales_details not in prd_info
SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_prd_key
NOT IN (
		SELECT prd_key
		FROM silver.crm_prd_info
		);




-- sls cust id to link cust_info
-- check if there are cust ids in sales_details not in cust_info
SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_cust_id
NOT IN (
		SELECT cst_id
		FROM silver.crm_cust_info
		);


-- The dates are integers not actual dates 
SELECT 
sls_order_dt
FROM silver.crm_sales_details
WHERE sls_order_dt <= 0 



-- Replace 0 order dates and dates with incomplete length (8 character length) with NULL 
-- Add all possible scenarios 

SELECT 
LEN(sls_order_dt) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE LEN(sls_order_dt) != 8


SELECT 
NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 
OR LEN(sls_order_dt) != 8;  

--To check the boundaries 
SELECT 
NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt > 20500101 OR sls_order_dt > 19000101




-- Replace 0 ship dates and dates with incomplete length (8 character length) with NULL 
-- Add all possible scenarios 

SELECT 
NULLIF(sls_ship_dt, 0) AS sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0 

SELECT 
LEN(sls_ship_dt) AS sls_ship_dt
FROM bronze.crm_sales_details
WHERE LEN(sls_ship_dt) != 8


SELECT 
NULLIF(sls_ship_dt, 0) AS sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0 
OR LEN(sls_ship_dt) != 8;  

--To check the boundaries 
SELECT 
NULLIF(sls_ship_dt, 0) AS sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt > 20500101 OR sls_ship_dt > 19000101


-- Replace 0 due dates and dates with incomplete length (8 character length) with NULL 
-- Add all possible scenarios 

SELECT 
NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 

SELECT 
LEN(sls_due_dt) AS sls_due_dt
FROM bronze.crm_sales_details
WHERE LEN(sls_due_dt) != 8;


SELECT 
NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
OR LEN(sls_due_dt) != 8;  

--To check the boundaries 
SELECT 
NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt > 20500101 OR sls_due_dt > 19000101




-- Check for Invalid Date Orders
-- Check order date is less than shipping or due date by doing this check
SELECT 
*
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt




-- Business Rule: Sales must be equal to Qty * Price 
-- All sales, quantity and price info must be postive 
-- Negatives, zeros, nulls are not allowed 
SELECT 
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0  OR sls_price <= 0 ;


SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0  OR sls_price <= 0 
ORDER BY sls_sales,
sls_quantity,
sls_price; 



---Quality checks on date in silver_sales_details
SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0  OR sls_price <= 0 
ORDER BY sls_sales,
sls_quantity,
sls_price; 



===================================================================================
  Checking 'silver.erp_cust_az12'
===================================================================================
--Not all having the preceeding 3 characters so use CASE WHEN 
SELECT 
SUBSTRING(cid, 4, LEN(cid)) AS cid
FROM bronze.erp_cust_az12;
SELECT * FROM silver.crm_cust_info;


SELECT 
*
FROM bronze.erp_cust_az12;
SELECT * FROM silver.crm_cust_info;

--Search using LIKE
SELECT 
cid,
bdate,
gen
FROM bronze.erp_cust_az12
WHERE cid LIKE '%AW00011000%';


-------QUALITY CHECKS---- 
-- Check for unwanted spaces
-- Expectation: No Result
SELECT 
cid,
bdate,
gen
FROM silver.erp_cust_az12
WHERE gen != TRIM(gen);



-- Check for relationship to crm_cust_info
-- Expectation: No Result
SELECT 
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) 
	 ELSE cid
END AS cid,
bdate,
gen
FROM bronze.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) 
	  ELSE cid
	  END NOT IN (
					SELECT DISTINCT
					cst_key
					FROM silver.crm_cust_info
					);


--- Identify out of range dates
--- Check birthday is not old like 1900 and in the future
SELECT 
bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE(); 



-- Gender has low cardinality
SELECT DISTINCT 
gen,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	 ELSE 'n/a'
END AS gen
FROM silver.erp_cust_az12;


-- Date Standardization & Consistency 
SELECT DISTINCT 
gen
FROM silver.erp_cust_az12;


===================================================================================
  Checking 'silver.erp_loc_a101'
===================================================================================
---Unwanted spaces
SELECT 
cid,
cntry
FROM bronze.erp_loc_a101
WHERE cntry != TRIM(cntry);



SELECT 
REPLACE(cid, '-', '') AS cid,
cntry
FROM bronze.erp_loc_a101
WHERE REPLACE(cid, '-', '') NOT IN (
				SELECT cst_key
				FROM silver.crm_cust_info
				); 
				 

SELECT cst_key
FROM silver.crm_cust_info; 


--- Data Standardization & Consistency 
---Country has low cardinality, let's check all possible entries 
SELECT DISTINCT
cntry
FROM silver.erp_loc_a101
ORDER BY cntry;


---Correct 
SELECT DISTINCT
cntry AS old_cntry,
CASE WHEN UPPER(TRIM(cntry)) IN ('DE', 'GERMANY') THEN 'Germany'
	 WHEN UPPER(TRIM(cntry)) IN ('US', 'USA', 'UNITED STATES') THEN 'United States'
	 WHEN TRIM(cntry) = '' OR TRIM(cntry) IS NULL THEN 'n/a'
	 ELSE TRIM(cntry)
END AS cntry
FROM silver.erp_loc_a101
ORDER BY cntry;



===================================================================================
  Checking 'silver.erp_px_cat_g1v2'
===================================================================================
--Check unwanted spaces 
SELECT 
id, 
cat,
subcat,
maintenance
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance);


--Data Standardization & Consistency
SELECT DISTINCT
maintenance
FROM silver.erp_px_cat_g1v2



---FINAL 
SELECT 
id, 
cat,
subcat,
maintenance
FROM silver.erp_px_cat_g1v2;
