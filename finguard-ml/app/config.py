from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    KAFKA_BOOTSTRAP_SERVERS: str
    REDIS_URL: str
    MODEL_PATH: str
    APP_ENV: str

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
    )


settings = Settings()
