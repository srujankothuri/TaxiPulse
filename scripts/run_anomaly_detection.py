"""
TaxiPulse — Run Anomaly Detection + Alerting
"""

import sys
from pathlib import Path
from loguru import logger

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from anomaly_detection.detector import run_anomaly_detection
from anomaly_detection.alerting import send_anomaly_alerts_from_db


def main():
    logger.info("🚕 TaxiPulse — Anomaly Detection + Alerting")
    logger.info("=" * 60)

    # Step 1: Detect anomalies
    results = run_anomaly_detection()

    # Step 2: Send alerts
    if results["total_anomalies"] > 0:
        logger.info("")
        logger.info("📬 Sending alerts...")
        logger.info("-" * 40)
        send_anomaly_alerts_from_db()

    logger.info("")
    logger.info("=" * 60)
    logger.success(
        f"✅ Complete! {results['total_anomalies']} anomalies detected "
        f"({results['fare_anomalies']} fare, "
        f"{results['volume_anomalies']} volume, "
        f"{results['daily_anomalies']} daily)"
    )
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)