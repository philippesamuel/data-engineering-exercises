# Activity 1 : Building an End-to-End dbt Model (Olist Dataset)

## Objective
In this exercise, you will build an **end-to-end dbt pipeline** using the Olist dataset in Snowflake. 
By the end, you will have a business-facing model (`fact_orders_geography`) that combines orders, customers geography.


## Instructions

### Step 1: Define Sources
1. Declare Olist tables (`orders`, `customers`) as **sources**.  
3. Use the correct database and schema where the tables are stored.  

### Step 2: Build Staging Models
1. Create staging models for **orders**, **customers**.  
2. Clean and rename columns where necessary (e.g., casting timestamps, removing redundant fields).  
3. Ensure staging models provide a **1:1 cleaned version of source tables**.  


### Step 3: Create an Intermediate Model
1. Create an intermediate model that joins **orders** with **customers**.  
2. Keep only useful columns (e.g., order status, purchase date, customer city, customer state). Keep only 'shipped','approved','delivered' orders.
3. Ensure this model represents a **clean customer–order relationship**.  


### Step 4: Build the Fact Model
1. Create a mart model named **`fact_orders_geography`**.  
2. This fact model should aggregate order and customer id count based on order_purchase_year,order_status,customer_city and customer_state.


### Step 5: Run Models
1. Run the dbt command to **build all models** in dependency order.  
2. Validate that the `fact_orders_geography` table is created in Snowflake.  
3. Explore the data to confirm it includes:  
   - Order details  
   - Customer details  
   - Order item details  

