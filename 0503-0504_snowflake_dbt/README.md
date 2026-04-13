# Week 5 day 3

## Activity 1 : Building an End-to-End dbt Model (Olist Dataset)

### Objective
In this exercise, you will build an **end-to-end dbt pipeline** using the Olist dataset in Snowflake. 
By the end, you will have a business-facing model (`fact_orders_geography`) that combines orders, customers geography.


### Instructions

#### Step 1: Define Sources
1. Declare Olist tables (`orders`, `customers`) as **sources**.  
3. Use the correct database and schema where the tables are stored.  

#### Step 2: Build Staging Models
1. Create staging models for **orders**, **customers**.  
2. Clean and rename columns where necessary (e.g., casting timestamps, removing redundant fields).  
3. Ensure staging models provide a **1:1 cleaned version of source tables**.  


#### Step 3: Create an Intermediate Model
1. Create an intermediate model that joins **orders** with **customers**.  
2. Keep only useful columns (e.g., order status, purchase date, customer city, customer state). Keep only 'shipped','approved','delivered' orders.
3. Ensure this model represents a **clean customer–order relationship**.  


#### Step 4: Build the Fact Model
1. Create a mart model named **`fact_orders_geography`**.  
2. This fact model should aggregate order and customer id count based on order_purchase_year,order_status,customer_city and customer_state.


#### Step 5: Run Models
1. Run the dbt command to **build all models** in dependency order.  
2. Validate that the `fact_orders_geography` table is created in Snowflake.  
3. Explore the data to confirm it includes:  
   - Order details  
   - Customer details  
   - Order item details  

---
## Activity 2 : Seller Performance Analysis using olist dataset

Analyze top sellers by sales value using olist dataset.

### 1. Stage (Staging Layer)

- **Purpose:**
Store raw or lightly cleaned data directly loaded from source (CSV, database, etc.) into Snowflake for downstream processing.
- **Tables Involved:**
    - `stg_order_items` ← Loaded from `OLIST_ORDER_ITEMS_DATASET`


### 2. Intermediate (Transformation Layer)

Create `int_order_item_sales`. 
Keep only required columns seller_id,order_id and price.
Exclude records where price is null.

### 3. Fact Model

- Create a fact model `fact_seller_performance` which provides aggregated price, and distinct order count for each seller.

---
# Week 5 day 4

## Activity 1 : Add test cases and generate documentation for all the fact models

### Objective
In this exercise, you will add test cases for all the fact models and generate documentation for the same.

### Instructions

#### Step 1: Add test cases and documentation for `fact_orders`

1. Create or update a `schema.yml` file inside the `models/facts/` directory.  
2. Add test cases to ensure `ORDER_PURCHASE_YEAR` and `ORDER_STATUS` are not null.  
3. Add test cases to confirm that `CNT_DISTINCT_ORDER` and `CNT_DISTINCT_CUSTOMER` are greater than or equal to 0.  
4. Add descriptions for each column in the `schema.yml`.  


#### Step 2: Add test cases and documentation for `fact_orders_geography` (You can reuse alredy built test cases)

1. In the same `schema.yml`, define test cases for this model.  
2. Add tests to ensure `ORDER_PURCHASE_YEAR`, `ORDER_STATUS`, `CUSTOMER_CITY`, and `CUSTOMER_STATE` are not null.  
3. Add test cases to confirm that `CNT_DISTINCT_ORDER` and `CNT_DISTINCT_CUSTOMER` are greater than or equal to 0.  
4. Document each column with business-friendly descriptions.  


#### Step 3: Add test cases and documentation for `fact_seller_performance`

1. Extend the `schema.yml` to include this model.  
2. Add test cases to ensure `SELLER_ID` is not null and unique.  
3. Add tests to ensure `TOTAL_SALES_VALUE` and `ORDER_COUNT` are not null.  
4. Add test cases to confirm that `TOTAL_SALES_VALUE` and `ORDER_COUNT` are greater than or equal to 0.  
5. Document each column in plain language, for example: "Total sales value contributed by the seller."  


#### Step 4: Generate Documentation and Explore Lineage

1. Generate dbt documentation for the project.  
2. Open the documentation in the UI.  
3. Navigate to each fact model and verify that the column descriptions are visible.  
4. Explore the lineage graph to see upstream staging and source tables.
