"""
TaxiPulse — Run Data Quality Validation
Validates Bronze data and shows results.
"""

import sys
from pathlib import Path
from loguru import logger

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quality.validate_data import validate_and_split_bronze


def main():
    logger.info("🚕 TaxiPulse — Data Quality Check")
    logger.info("=" * 60)

    result = validate_and_split_bronze()

    if result is None:
        logger.error("❌ No data to validate")
        return False

    logger.info("")
    logger.info("=" * 60)
    logger.info("📋 Final Results:")
    logger.info(f"   ✅ Clean rows ready for Silver: "
                f"{len(result['clean_df']):,}")
    logger.info(f"   ❌ Quarantined rows: "
                f"{len(result['quarantine_df']):,}")
    logger.info(f"   📈 Overall pass rate: "
                f"{result['summary']['overall_pass_rate']}%")

    # Show quarantine reasons breakdown
    if len(result["quarantine_df"]) > 0:
        logger.info("")
        logger.info("🔍 Top quarantine reasons:")
        reasons = (
            result["quarantine_df"]["quarantine_reason"]
            .value_counts()
            .head(10)
        )
        for reason, count in reasons.items():
            logger.info(f"   {count:,} — {reason}")

    # Show per-check breakdown
    logger.info("")
    logger.info("📊 Per-Check Results:")
    for r in result["results"]:
        if r["failed"] > 0:
            icon = "⚠️" if r["severity"] == "warning" else "❌"
            logger.info(
                f"   {icon} {r['name']}: "
                f"{r['failed']:,} failed ({r['pass_rate']}% pass)"
            )

    logger.success("✅ Quality check complete!")
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)