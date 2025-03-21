
/*
==================================================================================
Quality Checks 
==================================================================================
Script Purpose:
	This script performs quality checks to validate the integrity, consistency, 
	and accuracy of the Gold Layer. These checks ensure:
	- Uniqueness of surrogate keys in dimension tables.
	- Referential Integrity between fact and dimension tables.
	- Validation of relationships in the data modelfor analytical purposes. 
	

Usage:
	- Run these checks after data loading Silver Layer.
	- Investigate and resolve any discrepancies found during the checks.
==================================================================================
*/



-- ==================================================================================
-- Checking 'gold.dim_customers'
-- ==================================================================================

SELECT 
	customer_key, 
	COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1; 

-- ==================================================================================


SELECT DISTINCT 
	gender
FROM gold.dim_customers



-- ==================================================================================
-- Checking 'gold.dim_products'
-- ==================================================================================

SELECT 
	product_key, 
	COUNT(*) AS duplicate_count
FROM gold.dim_product
GROUP BY product_key
HAVING COUNT(*) > 1; 

-- ==================================================================================


-- ==================================================================================
-- Checking 'gold.fact_sales'
-- ==================================================================================
---Quality checks 
---Foreign Key Integrity (Dimensions)

---To check if all is matching perfectly 
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON		  f.customer_key = c.customer_key
WHERE c.customer_key IS NULL;


---To check if all is matching perfectly 
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON		  f.product_key	 = p.product_key	
WHERE p.product_key	IS NULL
