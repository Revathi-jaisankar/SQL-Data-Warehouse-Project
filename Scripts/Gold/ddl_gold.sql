/*

=========================================================
DDL Script: Create Gold Views
========================================================
Script Purpose:
  This Scripts creates views for the gold layer in the data warehouse. The Gold layer represents the final dimentions and fact tables (star schema)
  Each view  Performs transformations and combines data from the silver layer to produce a clean, enriched and business ready dataset.
Usage:
  -These views can be queried directly for analytics and reporting.
=======================================
*/


--========================================================
--Create Customer Dimention table in gold layer
--=========================================================

-- Step 1: To join the CRM Customer table into ERP Customer table then check primary ID duplicates
Select
	cst_id,
	count(*)
From(
Select 
	ci.cst_id,	
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	ci.cst_gndr,
	ci.cst_create_date,
	cb.BDATE,
	cb.GEN,
	la.CNTRY
From  silver.crm_cust_info ci
Left Join silver.erp_cust_az12 cb
	on ci.cst_key = cb.CID
Left Join silver.erp_loc_a101 la
	on ci.cst_key = la.CID

)t
Group by cst_id
Having count(*) > 1


-- Step 2: Data Integration : Two same column

Select Distinct
	ci.cst_gndr,
	cb.GEN,
Case 
	When cst_gndr != 'n/a' then ci.cst_gndr --CRM is the master for genter info
	Else Coalesce(cb.GEN, 'n/a')
End as New_gen
From  silver.crm_cust_info ci
Left Join silver.erp_cust_az12 cb
	on ci.cst_key = cb.CID
Left Join silver.erp_loc_a101 la
	on ci.cst_key = la.CID
	order by 1, 2


-- Step 3: To remove 2 gender column to add new gender columns and add naming conversion & to arrange the order of the column

Select 
	ci.cst_id as customer_id,	
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name,
	la.CNTRY as country,
	ci.cst_marital_status as marital_status,
	Case 
	When cst_gndr != 'n/a' then ci.cst_gndr --CRM is the master for genter info
	Else Coalesce(cb.GEN, 'n/a')
End as gender,
	cb.BDATE as birthdate,
	ci.cst_create_date as create_date
From  silver.crm_cust_info ci
Left Join silver.erp_cust_az12 cb
	on ci.cst_key = cb.CID
Left Join silver.erp_loc_a101 la
	on ci.cst_key = la.CID


-- Step 4: To decide the dim or fact table -> This Object table is Dim table 
-- To ensure the primary key in the table, sometimes do not have the primary key so we can generate the key is called as surrogate key


Select 
	ROW_NUMBER() Over (Order by cst_id) as Customer_key,
	ci.cst_id as customer_id,	
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name,
	la.CNTRY as country,
	ci.cst_marital_status as marital_status,
	Case 
	When cst_gndr != 'n/a' then ci.cst_gndr --CRM is the master for genter info
	Else Coalesce(cb.GEN, 'n/a')
End as gender,
	cb.BDATE as birthdate,
	ci.cst_create_date as create_date
From  silver.crm_cust_info ci
Left Join silver.erp_cust_az12 cb
	on ci.cst_key = cb.CID
Left Join silver.erp_loc_a101 la
	on ci.cst_key = la.CID;

-- Step 5: In silver layer, we create a views not in stored procedured

Create View gold.dim_customers AS
Select 
	ROW_NUMBER() Over (Order by cst_id) as Customer_key,
	ci.cst_id as customer_id,	
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name,
	la.CNTRY as country,
	ci.cst_marital_status as marital_status,
	Case 
	When cst_gndr != 'n/a' then ci.cst_gndr --CRM is the master for genter info
	Else Coalesce(cb.GEN, 'n/a')
End as gender,
	cb.BDATE as birthdate,
	ci.cst_create_date as create_date
From  silver.crm_cust_info ci
Left Join silver.erp_cust_az12 cb
	on ci.cst_key = cb.CID
Left Join silver.erp_loc_a101 la
	on ci.cst_key = la.CID

--Step 6: Data Quality :

Select
	*
From gold.dim_customers;


========================================================
--Build Gold Layer - Dimentional Product table
=========================================================

-- Step 1: To join the CRM Product table into ERP Product table then check primary ID duplicates

Select
	prd_key,
	count(*)
From(
Select 
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt,
	pc.cat,
	pc.subcat,
	pc.maintenance
From silver.crm_prd_info pn
Left Join Silver.erp_px_cat_g1v2 pc
	on pn.cat_id = pc.cid
where prd_end_dt IS NULL --Filter out Historical data
)t Group by prd_key
Having 	count(*) > 1

-- Step 2: To check Data Integrations (Any same 2 columns in this table but ans is no)
-- Step 3: Sort the column into logical groups to improve readability
--Step4: Give Nicely Friendly name
--step 5: To add suggrocate key

Create View gold.dim_products AS
Select 
	Row_number() Over(Order by pn.prd_start_dt, pn.prd_key) as prd_key,
	pn.prd_id as product_id,
	pn.prd_key as product_number,
	pn.prd_nm as product_name,
	pn.cat_id as category_id,
	pc.cat as category,
	pc.subcat as subcategory,
	pc.maintenance as maintenance,
	pn.prd_cost as cost,
	pn.prd_line as product_line,
	pn.prd_start_dt	as start_date
From silver.crm_prd_info pn
Left Join Silver.erp_px_cat_g1v2 pc
	on pn.cat_id = pc.cid
where prd_end_dt IS NULL; --Filter out Historical data


select * from gold.dim_products;


========================================================
--Build Gold Layer - Create Fact Sales
==========================================================
-- Step1: No join & NO data integration and its fact table
/* step2: Use the dimensions surrogate keys instead of IDs to easily connect facts with dimentions
To replece original Id into surrogate key to join the dim table to fact table*/

/* step 3: To join the golder layer product & customer info into silver layer of fact table 
because silver layer does not contains the surrogate key.*/

-- Step 4: To give nice name & sort the column

Create view gold.fact_sales AS
Select 
sls_ord_num as order_number,
dp.prd_key as Product_key,
dc.Customer_key as Customer_key,
sls_order_dt as order_date,
sls_ship_dt as shipping_date,
sls_due_dt as due_date,
sls_quantity as quantity,
sls_price as price,
sls_sales as sales_amount
From silver.crm_sales_details sp
Left join gold.dim_products dp
	on sp.sls_prd_key = dp.product_number
Left join gold.dim_customers dc
	on sp.sls_cust_id = dc.customer_id


--step 5: To check the quality

select * from gold.fact_sales


======================================

--To check all dim tables & fact table are working fine or not

Select 
f.*,
c.*
From gold.fact_sales f
left join gold.dim_customers c
	on f.customer_key = c.customer_key
where c.customer_key is NULL

Select 
f.*,
p.*
From gold.fact_sales f
left join gold.dim_products p
	on f.product_key = p.prd_key
where p.prd_key IS NULL

select * from gold.fact_sales
select * from gold.dim_customers;
select * from gold.dim_products;

=========================================
