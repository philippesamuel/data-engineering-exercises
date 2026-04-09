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
