"""
TaxiPulse — Run Gold Layer Build
"""

import sys
from pathlib import Path
from loguru import logger

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from transformations.gold.build_star_schema import run_gold_pipeline


def main():
    logger.info("🚕 TaxiPulse — Gold Layer Builder")
    logger.info("=" * 60)

    results = run_gold_pipeline()

    logger.info("")
    logger.info("=" * 60)
    total = sum(results.values())
    logger.success(f"✅ Gold layer complete! {total:,} total rows across all tables")
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)