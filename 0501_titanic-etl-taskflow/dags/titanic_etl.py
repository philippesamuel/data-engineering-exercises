from pathlib import Path

import pendulum
import pandas as pd
from airflow.sdk import dag, task
from loguru import logger

BASE_DIR = Path(__file__).resolve().parent
CSV_PATH = BASE_DIR / "titanic.csv"
SCHEMA = {
    "Name": pd.StringDtype,
    "Sex": pd.CategoricalDtype(categories=["male", "female"]),
    "Age": pd.Float32Dtype,
    "Survived": pd.CategoricalDtype(categories=[0, 1]),
}
COLS_TO_KEEP = list(SCHEMA)


# Define DAG
@dag(
    dag_id="titanic_etl",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["etl", "titanic"],
)
def titanic_etl_dag():
    # Task 1: Extract
    @task
    def extract(csv_path: Path = CSV_PATH) -> pd.DataFrame:
        # Prefer to raise early if file not present on worker:
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV not found at {csv_path} on worker")
        df = pd.read_csv(csv_path, usecols=COLS_TO_KEEP, dtype=SCHEMA)
        return df

    # Task 2: Transform
    @task
    def transform(df: pd.DataFrame) -> pd.DataFrame:
        transformed = (
            df.pipe(fill_missing_with_mean, "Age")
            .pipe(convert_survived_column)
            )
        return transformed

    # Task 3: Load
    @task
    def load(final_df: pd.DataFrame) -> None:
        final_path = BASE_DIR / "titanic_transformed.csv"
        final_df.to_csv(final_path, index=False)
        logger.info(f"Transformed data saved to {final_path}")

    # DAG Task Pipeline
    raw_data = extract()
    transformed = transform(raw_data)
    load(transformed)


def fill_missing_with_mean(df: pd.DataFrame, col: str) -> pd.DataFrame:
    return df.fillna({col: df[col].mean()})


def convert_survived_column(df: pd.DataFrame) -> pd.DataFrame:
    col = "Survived"
    rename_map = {0: "No", 1: "Yes"}
    survived = df[col]
    mapped_survived = survived.cat.rename_categories(rename_map)
    return df.assign(**{col: mapped_survived})


# Instantiate DAG
titanic_etl_dag()
