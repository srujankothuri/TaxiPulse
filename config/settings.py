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


class GCPConfig:
    """Google Cloud Platform configuration."""
    PROJECT_ID = os.getenv("GCP_PROJECT_ID")
    REGION = os.getenv("GCP_REGION", "us-central1")
    CREDENTIALS_PATH = os.getenv("GCP_CREDENTIALS_PATH")


class GCSConfig:
    """Google Cloud Storage configuration."""
    BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "taxipulse-data-lake")
    BRONZE_PREFIX = os.getenv("GCS_BRONZE_PREFIX", "bronze/")
    SILVER_PREFIX = os.getenv("GCS_SILVER_PREFIX", "silver/")
    GOLD_PREFIX = os.getenv("GCS_GOLD_PREFIX", "gold/")


class BigQueryConfig:
    """BigQuery configuration."""
    DATASET_BRONZE = os.getenv("BQ_DATASET_BRONZE", "taxipulse_bronze")
    DATASET_SILVER = os.getenv("BQ_DATASET_SILVER", "taxipulse_silver")
    DATASET_GOLD = os.getenv("BQ_DATASET_GOLD", "taxipulse_gold")


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


# Convenience: validate critical config on import
def validate_config():
    """Check that essential configuration values are set."""
    missing = []
    if not GCPConfig.PROJECT_ID:
        missing.append("GCP_PROJECT_ID")
    if not GCPConfig.CREDENTIALS_PATH:
        missing.append("GCP_CREDENTIALS_PATH")
    if missing:
        print(f"⚠️  Missing required config: {', '.join(missing)}")
        print("   Copy .env.example to .env and fill in your values.")
    return len(missing) == 0