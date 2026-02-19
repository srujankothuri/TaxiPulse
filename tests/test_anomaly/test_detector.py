"""
TaxiPulse — Tests for Anomaly Detection Module
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from anomaly_detection.detector import detect_zscore_anomalies


class TestZScoreDetection:
    """Test Z-score anomaly detection."""

    def test_no_anomalies_normal_data(self):
        """Normal distribution data should have few/no anomalies."""
        np.random.seed(42)
        df = pd.DataFrame({
            "value": np.random.normal(20, 2, 1000),
        })
        anomalies = detect_zscore_anomalies(df, "value", threshold=3.0)
        # With 1000 samples from normal dist, expect < 1% anomalies
        assert len(anomalies) < 50

    def test_detects_obvious_outlier(self):
        """An extreme outlier should be detected."""
        df = pd.DataFrame({
            "value": [10, 11, 12, 10, 11, 12, 10, 11, 100],
        })
        anomalies = detect_zscore_anomalies(df, "value", threshold=2.0)
        assert len(anomalies) >= 1
        assert 100 in anomalies["value"].values

    def test_grouped_detection(self):
        """Should detect anomalies within groups."""
        df = pd.DataFrame({
            "zone": ["A"] * 10 + ["B"] * 10,
            "value": [10, 11, 10, 11, 10, 11, 10, 11, 10, 11,
                      50, 51, 50, 51, 50, 51, 50, 51, 50, 200],
        })
        anomalies = detect_zscore_anomalies(
            df, "value", group_col="zone", threshold=2.0
        )
        # 200 should be anomalous within group B
        assert len(anomalies) >= 1

    def test_empty_dataframe(self):
        """Empty DataFrame should return empty anomalies."""
        df = pd.DataFrame(columns=["value"])
        anomalies = detect_zscore_anomalies(df, "value", threshold=3.0)
        assert len(anomalies) == 0

    def test_single_value(self):
        """Single value should not produce anomalies."""
        df = pd.DataFrame({"value": [10]})
        anomalies = detect_zscore_anomalies(df, "value", threshold=3.0)
        assert len(anomalies) == 0

    def test_threshold_sensitivity(self):
        """Lower threshold should detect more anomalies."""
        np.random.seed(42)
        data = list(np.random.normal(20, 2, 100)) + [50, 55]
        df = pd.DataFrame({"value": data})

        strict = detect_zscore_anomalies(df, "value", threshold=4.0)
        relaxed = detect_zscore_anomalies(df, "value", threshold=2.0)

        assert len(relaxed) >= len(strict)