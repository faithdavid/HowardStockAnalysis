import logging
import logging.config
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    airtable_token: str | None = None
    airtable_base_id: str | None = None

    airtable_table_runreports: str = "RunReports"  # can override via env: AIRTABLE_TABLE_RUNREPORTS
    zapier_webhook_url: str | None = None
    
    # Logging configuration
    log_level: str = "INFO"
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


settings = Settings()


def configure_logging():
    """Configure structured logging for the application."""
    logging.basicConfig(
        level=settings.log_level,
        format=settings.log_format,
    )
    return logging.getLogger(__name__)
