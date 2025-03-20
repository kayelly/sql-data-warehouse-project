Data Dictionary for Gold Layer 

Overview 

The Gold Layer is the business-level data representation, structured to support analytical and reporting use cases. It consists of dimension tables and fact tables for specific business metrics.

1.	gold.dim_customers 
•	Purpose: Stores customer details enriched with demographic and geographic data.
•	Columns: 

|  Column Name	          |  Data Type	            |  Description                                                                      |
|-------------------------|-------------------------|-----------------------------------------------------------------------------------|
|customer_key	            |  INT	                  |  Surrogate key uniquely identifying each customer record in the dimension table.  |
|customer_id	INT	        |  INT                    |  Unique numerical identifier assigned to each customer.                           |
|customer_number		      |  NVARCHAR(50)           |  Alphanumeric identifier representing the customer, used for tracking and referencing.|          
|first_name		            |  NVARCHAR(50)           |  The customer's first name, as recorded in the system.                            |
|last_name		            |
country		
marital_status		
gender		
birthdate		
create_date		

![image](https://github.com/user-attachments/assets/485a1edf-ac72-4522-bc55-8464d59c96f4)
