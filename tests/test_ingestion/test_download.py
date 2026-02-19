"""
TaxiPulse — Tests for Data Ingestion Module
"""

import sys
from pathlib import Path
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.settings import TLCConfig


class TestTLCConfig:
    """Test TLC data source configuration."""

    def test_parquet_url_format(self):
        """URL should follow expected pattern."""
        url = TLCConfig.get_parquet_url(2024, "01")
        assert "yellow_tripdata_2024-01.parquet" in url
        assert url.startswith("https://")

    def test_filename_format(self):
        """Filename should follow expected pattern."""
        name = TLCConfig.get_filename(2024, "03")
        assert name == "yellow_tripdata_2024-03.parquet"

    def test_default_year(self):
        """Default year should be set."""
        assert TLCConfig.DATA_YEAR is not None
        assert isinstance(TLCConfig.DATA_YEAR, int)

    def test_default_months(self):
        """Default months should be a list."""
        assert isinstance(TLCConfig.DATA_MONTHS, list)
        assert len(TLCConfig.DATA_MONTHS) > 0

    def test_url_different_months(self):
        """Different months should produce different URLs."""
        url1 = TLCConfig.get_parquet_url(2024, "01")
        url2 = TLCConfig.get_parquet_url(2024, "02")
        assert url1 != url2