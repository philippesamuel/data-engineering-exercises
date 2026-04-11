import sys
from pathlib import Path
from zipfile import ZipFile

from airflow.sdk import dag, task
from pendulum import datetime
import polars as pl

from kaggle.api.kaggle_api_extended import KaggleApi
from loguru import logger

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

from config import settings

# Remove the default stderr handler
logger.remove()

# Add a new handler pointing to stdout
logger.add(
    sys.stdout, colorize=True, format="<green>{time}</green> <level>{message}</level>"
)

sf_settings = settings.snowflake

CHUNK_SIZE = 5_000
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


def download_kaggle_file(name, api: KaggleApi) -> str:
    target_path = (TMP_DIR_PATH / name).with_suffix(".csv")
    logger.info(f"Downloading {name}...")
    success = api.dataset_download_file(
        file_name=name,
        dataset=DATASET,
        path=TMP_DIR_PATH,
    )
    if success:
        logger.success(f"File downloaded to {target_path}")
    return str(target_path.absolute())


def download_kaggle_files() -> Path:
    target_path = Path(TMP_DIR_PATH)
    api = API
    logger.info(f"Downloading {DATASET}...")
    success = api.dataset_download_files(dataset=DATASET, path=target_path)
    if success:
        logger.success(f"File downloaded to {target_path}")

    with ZipFile(target_path / "brazilian-ecommerce.zip", "r") as f:
        f.extractall(target_path)

    return target_path.absolute()


@dag(
    dag_id="olist_to_snowflake_etl",
    start_date=datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["etl", "ecommerce", "brazil", "snowflakes"],
)
def olist_to_snowflake_etl_dag() -> None:
    @task
    def extract_kaggle() -> None:
        _ = download_kaggle_files()

    @task
    def sanitize_csv(input_path_str: str) -> None:
        input_path = Path(input_path_str)
        output_path = (
            input_path.with_stem(input_path.stem + "_utf8")
            .with_suffix(".csv")
            )

        # Open input with source encoding and output with target encoding
        with input_path.open("r", encoding="iso-8859-1", newline="") as source:
            with output_path.open("w", encoding="utf-8", newline="") as target:
                # Python's file iterator reads line by line (buffered)
                for line in source:
                    target.write(line)

    @task
    def load_to_snowflake(csv_path_str: str, table_name: str) -> None:
        csv_path = Path(csv_path_str)
        table_name = csv_path.stem.upper()
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV not found at {csv_path}")
        df = pl.scan_csv(csv_path_str)

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
            role=sf.role,
        )
        try:
            logger.info("Loading to Snowflake using write_pandas...")
            # Load the first chunk with overwrite, then append others
            first_chunck = True
            for df_chunk in df.collect_batches(chunk_size=CHUNK_SIZE):
                success, nchunks, nrows, _ = write_pandas(
                    conn=conn,
                    df=df_chunk.to_pandas(),
                    table_name=table_name,
                    auto_create_table=True,  # Handles the 'replace' logic for you
                    overwrite=first_chunck,  # Overwrites the table if it exists
                )
                first_chunck = False

                if success:
                    logger.success(f"Successfully loaded {nrows} rows to Snowflake!")

        finally:
            conn.close()

    import shutil

    @task(trigger_rule="all_done")  # Ensures cleanup runs even if a load fails
    def cleanup_local_files(directory_path: str) -> None:
        path = Path(directory_path)
        if path.exists() and path.is_dir():
            shutil.rmtree(path)
            logger.success(f"Cleaned up temporary directory: {path}")
        else:
            logger.warning(f"Cleanup skipped: {path} not found or not a directory.")

    _csv_paths = [str(TMP_DIR_PATH / f"{name}.csv") for name in DATA_FILENAMES]
    _load_kwargs = [
        {
            "csv_path_str": str(TMP_DIR_PATH / f"{name}_utf8.csv"),
            "table_name": name.upper(),
        }
        for name in DATA_FILENAMES
    ]
    
    extract_task = extract_kaggle()
    sanitize_tasks = (
        sanitize_csv
        .partial(
            map_index_template="""{{ task.input_path_str.split('/')[-1] }}""",
            )
        .expand(input_path_str=_csv_paths))
    load_tasks = (
        load_to_snowflake
        .partial(
            map_index_template="""{{ task.table_name }}""",
            )
        .expand_kwargs(_load_kwargs)
        )
    cleanup_task = cleanup_local_files(str(TMP_DIR_PATH))

    extract_task >> sanitize_tasks >> load_tasks >> cleanup_task 


olist_to_snowflake_etl_dag()
