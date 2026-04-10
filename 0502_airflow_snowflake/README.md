# Week 5 Day 2: Apache Airflow Exercises

## Activity 1: Extract Weather Data from an API and load to snowflake

### Step 1: Extract

- Use the OpenWeatherMap [API](https://openweathermap.org/) to fetch real-time weather data for a specified city.
  - Create an account and generate an API [key](https://home.openweathermap.org/api_keys)
  - example API call that you can use `http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"`
- Write a Python function that calls the API and retrieves the weather data in JSON format.  
- Push the raw JSON data into Airflow’s XCom for use in downstream tasks.  

### Step 2: Transform

- Retrieve raw JSON weather data from XCom.  
- Use pandas to parse and flatten the JSON.  
- Extract relevant fields: city, temperature, humidity, weather description, and timestamp.  
- Convert the cleaned data into JSON format.  
- Push the transformed JSON data back to XCom for the next step.  

### Step 3: Load

- Pull the cleaned JSON data from XCom.  
- Deserialize it back into a pandas DataFrame.  
- Save the DataFrame as a CSV file in a local directory.  
- Log the file path for verification.

### Step 4: Load CSV file to snowflake table

- Create a snowflake table. Based on file structure.
- Load content of file to this table. You can use code in day 2 exercise to accomplish this step.

## Activity 2: Load Olist Dataset into Snowflake with Airflow  

### Objective  

Build an **Apache Airflow DAG** that ingests multiple CSV files from the **Olist e-commerce dataset** into Snowflake. The DAG should ensure **idempotent loading** (avoid loading the same file multiple times).  This data is publicly available here <https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce>

### Dataset  

You are provided with the following CSV files (Olist dataset):  

- `olist_order_reviews_dataset.csv`  
- `olist_orders_dataset.csv`  
- `olist_products_dataset.csv`  
- `olist_sellers_dataset.csv`  
- `product_category_name_translation.csv`  
- `olist_geolocation_dataset.csv`  
- `olist_order_items_dataset.csv`  
- `olist_order_payments_dataset.csv`  
- `olist_customers_dataset.csv`  

---

### Tasks  

#### **Step 1 – DAG Setup**

1. Create a DAG in Airflow v3.  
2. Configure it with:
   - `start_date` in the past  
   - `catchup=False`  
   - `schedule=None` (manual runs)  
   - Tags: `["snowflake", "olist", "etl"]`  

---

#### **Step 2 – Whitelist File Loading**

1. Ensure your DAG **only processes the above 9 files**.  
2. Ignore other CSVs if present in the same directory.  

---

#### **Step 3 – Extract**

1. Read each CSV file into a DataFrame.  
2. Standardize column names (convert to uppercase).  
3. Skip the file if it is missing or empty.
4. Load data into chunks of 50000 to avoid memory issue due to local execution.

---

#### **Step 4 – Transform**

1. Use the file name to determine the **target Snowflake table name**.  
   - Example: `olist_orders_dataset.csv → OLIST_ORDERS_DATASET`  

---

#### **Step 5 – Load**

1. Connect to Snowflake using SQLAlchemy.  
2. Before loading, make sure the following objects exist (or create them if missing):  
   - Warehouse: `COMPUTE_WH`  
   - Database: `SNOWFLAKE_LEARNING_DB`  
   - Schema: `PUBLIC`  
3. Load each DataFrame into Snowflake using `to_sql()` or equivalent.  

---

#### **Step 6 – Prevent Duplicate Loads**

1. Create a **control table** in Snowflake:  

   ```sql
   CREATE TABLE IF NOT EXISTS LOADED_FILES (
       FILE_NAME STRING PRIMARY KEY,
       LOAD_TIMESTAMP TIMESTAMP
   );
   ```

2. Before loading a file, check if it already exists in LOADED_FILES.
   If it exists → skip loading.
   If not → load the file and insert a record into LOADED_FILES.

---

#### **Step 7 – Verification**

1. **Run the DAG once** → All 9 Olist files should be loaded into Snowflake.  
2. **Run the DAG again** → No duplicate loads should happen (all files should be skipped).  
