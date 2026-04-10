import csv
from os import lseek
from pathlib import Path

from airflow.sdk import dag, task
from pendulum import datetime
import polars as pl

# from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from kaggle.api.kaggle_api_extended import KaggleApi
from loguru import logger

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

from config import settings
sf_settings = settings.snowflake

DATASET = "olistbr/brazilian-ecommerce"
TMP_DIR_PATH = Path("/tmp/olist")

DATA_FILENAMES = [
    "olist_order_reviews_dataset",
    "olist_orders_dataset",
    "olist_products_dataset",
    "olist_sellers_dataset",
    "product_category_name_translation",
    "olist_geolocation_dataset",
    "olist_order_items_dataset",
    "olist_order_payments_dataset",
    "olist_customers_dataset",
]

API = KaggleApi()
API.authenticate()


@dag(
    dag_id="olist_to_snowflake_etl",
    start_date=datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["etl", "ecommerce", "brazil", "snowflakes"],
)
def olist_to_snowflake_etl_dag():
    @task
    def extract_kaggle(csv_name) -> str:
        return download_kaggle_file(csv_name, API)

    @task
    def load_to_snowflake(csv_path_str: str) -> None:
        csv_path = Path(csv_path_str)
        table_name = csv_path.stem.upper()
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV not found at {csv_path}")
        df = pl.scan_csv(csv_path)
        
        if df.head(1).collect().is_empty():
            raise ValueError(f"CSV at {csv_path} is empty.")
        
        df = df.rename(str.upper)
        
        sf = sf_settings
        conn = snowflake.connector.connect(
            user=sf.user,
            password=sf.password.get_secret_value(),
            account=sf.account,
            warehouse=sf.warehouse,
            database=sf.database,
            schema=sf.schema_name,
            role=sf.role
        )
        try:
            logger.info("Loading to Snowflake using write_pandas...")

            for df_chunk in df.collect_batches(chunk_size=5_000):
                success, nchunks, nrows, _ = write_pandas(
                    conn=conn,
                    df=df_chunk.to_pandas(),
                    table_name=table_name,
                    auto_create_table=True, # Handles the 'replace' logic for you
                    overwrite=True          # Overwrites the table if it exists
                )
        
                if success:
                    logger.success(f"Successfully loaded {nrows} rows to Snowflake!")
            
        finally:
            conn.close()
    csv_names = [name + ".csv" for name in DATA_FILENAMES]
    _csv_path_strings = extract_kaggle.expand(csv_name=csv_names)
    load_to_snowflake.expand(csv_path_str=_csv_path_strings)
        

def download_kaggle_file(name, api: KaggleApi) -> str:
    target_path = (TMP_DIR_PATH / name).with_suffix(".csv")
    logger.info(f"Downloading {name}...")
    success = api.dataset_download_file(
        file_name=name,
        dataset=DATASET,
        path=target_path,
    )
    if success:
        logger.success(f"File downloaded to {target_path}")
    return str(target_path.absolute())
