## Activity 1: Titanic Data ETL with TaskFlow API

### Objective
Build an ETL pipeline using Airflow 3.x **TaskFlow API**.

### Steps
* **Extract**
  * Load Titanic CSV file
* **Transform**
  * Keep columns: `Name`, `Sex`, `Age`, `Survived`
  * Fill missing ages with the mean
  * Convert `Survived` (0 → "No", 1 → "Yes")
* **Load**
  * Save cleaned data into a new CSV file

### Requirements
- Use **Airflow DAG + TaskFlow API**
- Define `extract`, `transform`, `load` as tasks
- Ensure proper dependencies using function calls

