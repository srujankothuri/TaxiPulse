"""
TaxiPulse — Run Silver Layer Transformation
"""

import sys
from pathlib import Path
from loguru import logger

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from transformations.silver.clean_and_validate import run_silver_pipeline


def main():
    logger.info("🚕 TaxiPulse — Silver Layer Transformation")
    logger.info("=" * 60)

    result = run_silver_pipeline()

    logger.info("")
    logger.info("=" * 60)
    if result["status"] == "success":
        logger.success(
            f"✅ Silver pipeline complete! "
            f"{result['silver_rows']:,} clean rows, "
            f"{result['quarantine_rows']:,} quarantined"
        )
    elif result["status"] == "skipped":
        logger.info(
            f"⏭️  Skipped — Silver already has "
            f"{result['existing_rows']:,} rows"
        )
    else:
        logger.warning("⚠️  No data processed")

    return result["status"] == "success" or result["status"] == "skipped"


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)