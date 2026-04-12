from pydantic import AliasChoices, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import URL

common_config = SettingsConfigDict(
    env_file=".env",
    env_file_encoding="utf8",
    secrets_dir="/run/secrets",
    extra="ignore",
)


class SnowflakeSettings(BaseSettings):
    user: str = Field("")
    password: SecretStr = Field(SecretStr(""))
    account: str = Field("")
    database: str = Field("")
    schema_name: str = Field(
        "",
        validation_alias=AliasChoices("schema", "snowflake_schema"),
    )
    warehouse: str = Field("")
    role: str = Field("")

    model_config = SettingsConfigDict(
        common_config,
        env_prefix="snowflake_",
        case_sensitive=False,
    )

    def get_db_url(self) -> URL:
        return URL.create(
            drivername="snowflake",
            username=self.user,
            password=self.password.get_secret_value(),
            host=self.account,
            port=None,
            database=f"{self.database}/{self.schema_name}",
            query=dict(  # type: ignore
                warehouse=self.warehouse,
                role=self.role,
            ),
        )


class Settings(BaseSettings):
    openweather_api_key: str
    weather_endpoint: str = Field("http://api.openweathermap.org/data/2.5/weather")
    city: str = Field("belo horizonte")

    snowflake: SnowflakeSettings = Field(default_factory=lambda: SnowflakeSettings())
    model_config = common_config

    def get_snowflake_db_url(self) -> URL:
        return self.snowflake.get_db_url()


settings = Settings()
