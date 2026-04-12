from pathlib import Path
from zipfile import ZipFile

from airflow.sdk import dag, task
from pendulum import datetime
import polars as pl

from kaggle.api.kaggle_api_extended import KaggleApi
import logging as logger

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

from config import settings
from mappings import FILE_STEM_TO_TABLE_NAME
sf_settings = settings.snowflake

CHUNK_SIZE = 50_000
DATASET = "olistbr/brazilian-ecommerce"
TMP_DIR_PATH = Path("/tmp/olist")

DATA_FILENAMES = list(FILE_STEM_TO_TABLE_NAME)

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
        logger.info(f"File downloaded to {target_path}")
    return str(target_path.absolute())


def download_kaggle_files() -> Path:
    target_path = Path(TMP_DIR_PATH)
    zip_file_path = target_path / "brazilian-ecommerce.zip"

    if not zip_file_path.exists(): 
        api = API
        logger.info(f"Downloading {DATASET}...")
        success = api.dataset_download_files(dataset=DATASET, path=target_path)
        if success:
            logger.info(f"File downloaded to {target_path}")

    else:
        logger.info(f"File {zip_file_path} already downloaded" )

    logger.info(f"Extracting {zip_file_path}...")

    with ZipFile(zip_file_path, "r") as f:
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

    @task(map_index_template="""{{ input_path_str.split('/')[-1] }}""")
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

    @task(map_index_template="""{{ table_name }}""")
    def load_to_snowflake(csv_path_str: str, table_name: str) -> None:
        csv_path = Path(csv_path_str)
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
            logger.info(f"Loading {table_name} to Snowflake using write_pandas...")
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
                    logger.info(f"Successfully loaded {nrows} rows to Snowflake!")

        finally:
            conn.close()

    @task(trigger_rule="all_success")  # Ensures cleanup runs even if a load fails
    def cleanup_local_files(directory_path: str) -> None:
        path = Path(directory_path)
        if path.exists() and path.is_dir():
            csv_files = path.glob("*.csv")
            for f in csv_files:
                logger.info(f"Removing file {f}")
                f.unlink()
            logger.info(f"Cleaned up temporary directory: {path}")
        else:
            logger.warning(f"Cleanup skipped: {path} not found or not a directory.")

    _csv_paths = [str(TMP_DIR_PATH / f"{stem}.csv") for stem in DATA_FILENAMES]
    _load_kwargs = [
        {
            "csv_path_str": str(TMP_DIR_PATH / f"{stem}_utf8.csv"),
            "table_name": tname,
        }
        for stem, tname in FILE_STEM_TO_TABLE_NAME.items()
    ]
    
    extract_task = extract_kaggle()
    sanitize_tasks = sanitize_csv.expand(input_path_str=_csv_paths)
    load_tasks = (
        load_to_snowflake
        .override(max_active_tis_per_dag=2)
        .expand_kwargs(_load_kwargs)
        )
    cleanup_task = cleanup_local_files(str(TMP_DIR_PATH))

    extract_task >> sanitize_tasks >> load_tasks >> cleanup_task 


olist_to_snowflake_etl_dag()
