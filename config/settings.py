"""
TaxiPulse — Centralized Configuration
Loads environment variables from .env file and provides
typed access to all configuration values.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env file from project root
ROOT_DIR = Path(__file__).resolve().parent.parent
load_dotenv(ROOT_DIR / ".env")


class MinIOConfig:
    """MinIO (S3-compatible) object storage configuration."""
    ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "taxipulse")
    SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "taxipulse123")
    BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "taxipulse-data-lake")
    BRONZE_PREFIX = os.getenv("MINIO_BRONZE_PREFIX", "bronze/")
    SILVER_PREFIX = os.getenv("MINIO_SILVER_PREFIX", "silver/")
    GOLD_PREFIX = os.getenv("MINIO_GOLD_PREFIX", "gold/")
    USE_SSL = os.getenv("MINIO_USE_SSL", "false").lower() == "true"


class PostgresConfig:
    """PostgreSQL data warehouse configuration."""
    HOST = os.getenv("POSTGRES_HOST", "localhost")
    PORT = int(os.getenv("POSTGRES_PORT", "5432"))
    DATABASE = os.getenv("POSTGRES_DB", "taxipulse")
    USER = os.getenv("POSTGRES_USER", "taxipulse")
    PASSWORD = os.getenv("POSTGRES_PASSWORD", "taxipulse123")

    # Schema names (Medallion layers)
    SCHEMA_BRONZE = os.getenv("PG_SCHEMA_BRONZE", "bronze")
    SCHEMA_SILVER = os.getenv("PG_SCHEMA_SILVER", "silver")
    SCHEMA_GOLD = os.getenv("PG_SCHEMA_GOLD", "gold")

    @classmethod
    def get_connection_string(cls) -> str:
        """SQLAlchemy connection string."""
        return (
            f"postgresql://{cls.USER}:{cls.PASSWORD}"
            f"@{cls.HOST}:{cls.PORT}/{cls.DATABASE}"
        )


class TLCConfig:
    """NYC TLC Data Source configuration."""
    BASE_URL = os.getenv(
        "TLC_BASE_URL",
        "https://d37ci6vzurychx.cloudfront.net/trip-data"
    )
    DATA_YEAR = int(os.getenv("TLC_DATA_YEAR", "2024"))
    DATA_MONTHS = os.getenv("TLC_DATA_MONTHS", "01,02,03").split(",")

    @classmethod
    def get_parquet_url(cls, year: int, month: str) -> str:
        """Generate download URL for a specific month's data."""
        return f"{cls.BASE_URL}/yellow_tripdata_{year}-{month}.parquet"

    @classmethod
    def get_filename(cls, year: int, month: str) -> str:
        """Generate filename for a specific month's data."""
        return f"yellow_tripdata_{year}-{month}.parquet"


class KafkaConfig:
    """Apache Kafka configuration."""
    BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    TOPIC = os.getenv("KAFKA_TOPIC", "taxipulse-trips")


class SlackConfig:
    """Slack alerting configuration."""
    WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
    CHANNEL = os.getenv("SLACK_CHANNEL", "#taxipulse-alerts")


class AnomalyConfig:
    """Anomaly detection configuration."""
    ZSCORE_THRESHOLD = float(os.getenv("ANOMALY_ZSCORE_THRESHOLD", "3.0"))
    CHECK_INTERVAL_HOURS = int(os.getenv("ANOMALY_CHECK_INTERVAL_HOURS", "1"))


class StreamlitConfig:
    """Streamlit app configuration."""
    PORT = int(os.getenv("STREAMLIT_PORT", "8501"))


class AirflowConfig:
    """Airflow configuration."""
    HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
    EXECUTOR = os.getenv("AIRFLOW__CORE__EXECUTOR", "LocalExecutor")


# Convenience: validate critical config on import
def validate_config():
    """Check that essential configuration values are set."""
    missing = []
    if not MinIOConfig.ENDPOINT:
        missing.append("MINIO_ENDPOINT")
    if not PostgresConfig.HOST:
        missing.append("POSTGRES_HOST")
    if missing:
        print(f"⚠️  Missing required config: {', '.join(missing)}")
        print("   Copy .env.example to .env and fill in your values.")
    return len(missing) == 0