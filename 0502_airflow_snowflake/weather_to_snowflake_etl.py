import pendulum
import pandas as pd
from pathlib import Path
from airflow.sdk import dag, task
from pydantic import BaseModel, Field
from loguru import logger
import requests
from sqlalchemy import create_engine

from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector

from config import settings

API_KEY = settings.openweather_api_key
WEATHER_ENDPOINT = settings.weather_endpoint
CITY = settings.city

DATA_PATH = Path(__file__).parent.parent / "data"
CSV_PATH = DATA_PATH / "weather.csv"
DB_URL = settings.get_snowflake_db_url()
ENGINE = create_engine(DB_URL, echo=True)

sf_settings = settings.snowflake

WAREHOUSE = sf_settings.warehouse
DATABASE = sf_settings.database
SCHEMA = sf_settings.schema_name
TABLE_NAME = "TBL_WEATHER"


@dag(
    dag_id="weather_to_snowflakes_etl",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["etl", "weather", "snowflakes"],
)
def weather_snowflakes_etl_dag():
    @task
    def extract():
        endpoint = WEATHER_ENDPOINT
        params = {
            "q": CITY,
            "appid": API_KEY,
            "units": "metric",
        }
        logger.info(f"Fetching weather data from {WEATHER_ENDPOINT} for {CITY}...")
        r = requests.get(endpoint, params)
        logger.success("Data fetched, saving to XCom")
        return r.json()

    @task
    def transform(weather_response_json: dict) -> dict:
        logger.info("Parsing response data...")
        wr = WeatherResponse(**weather_response_json)

        timestamp = pendulum.from_timestamp(wr.dt, tz="utc")
        descriptions = (w.description for w in wr.weather)

        transformed_dict = {
            "city": wr.name,
            "temperature": wr.main_.temp,
            "humidity": wr.main_.humidity,
            "weather_description": r"\n".join(descriptions),
            "timestamp": timestamp.to_iso8601_string(),
        }
        logger.success("Data parsed, saving to XCom")
        return {"data": transformed_dict}

    @task
    def json_to_csv(payload: dict) -> str:
        logger.info("Saving parsed data as csv...")
        data = payload["data"]
        df = pd.DataFrame(data, index=[0])
        DATA_PATH.mkdir(parents=True, exist_ok=True)
        df.to_csv(CSV_PATH, index=False)
        logger.success(f"csv file saved: {CSV_PATH}")
        return str(CSV_PATH.absolute())

    @task
    def load_csv(csv_path_str: str) -> None:
        logger.info("Reading local csv file...")
        csv_path = Path(csv_path_str)
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV not found at {csv_path}")

        df = pd.read_csv(
            csv_path,
            dtype={
                "city": pd.StringDtype(),
                "temperature": pd.Float32Dtype(),
                "humidity": pd.Float32Dtype(),
                "weather_description": pd.StringDtype(),
            },
            parse_dates=["timestamp"],
        )

        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

        if df.empty:
            raise ValueError(f"CSV at {csv_path} is empty.")

        # Standardize column names for Snowflake
        df.columns = [c.upper() for c in df.columns]
        
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
        
        # Use the specialized Snowflake-Pandas tool
        # This is MUCH faster than to_sql as it uses the PUT + COPY command internally
            success, nchunks, nrows, _ = write_pandas(
                conn=conn,
                df=df,
                table_name=TABLE_NAME.upper(),
                auto_create_table=True, # Handles the 'replace' logic for you
                overwrite=True          # Overwrites the table if it exists
            )
        
            if success:
                logger.success(f"Successfully loaded {nrows} rows to Snowflake!")
            
        finally:
            conn.close()

    # DAG Task Pipeline
    raw_data = extract()
    transformed = transform(raw_data)  # type: ignore
    csv_path_str = json_to_csv(transformed)  # type: ignore
    load_csv(csv_path_str)  # type: ignore


class Sys(BaseModel):
    sunset: int
    country: str
    sunrise: int


class Main(BaseModel):
    temp: float
    humidity: int
    pressure: int
    temp_max: float
    temp_min: float
    sea_level: int
    feels_like: float
    grnd_level: int


class Wind(BaseModel):
    deg: int
    speed: float


class Coord(BaseModel):
    lat: float
    lon: float


class Clouds(BaseModel):
    all: int


class WeatherItem(BaseModel):
    id_: int = Field(alias="id")
    icon: str
    main_: str = Field(alias="main")
    description: str


class WeatherResponse(BaseModel):
    dt: int
    id_: int = Field(alias="id")
    cod: int
    sys_: Sys = Field(alias="sys")
    base: str
    main_: Main = Field(alias="main")
    name: str
    wind: Wind
    coord: Coord
    clouds: Clouds
    weather: list[WeatherItem]
    timezone: int
    visibility: int


# Instantiate DAG
weather_snowflakes_etl_dag()
