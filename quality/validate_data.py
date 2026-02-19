"""
TaxiPulse — Data Quality Validation Engine
Validates Bronze layer data against defined expectations.
Clean records are promoted to Silver, failed records are quarantined.
Results are logged to gold.quality_log for monitoring.
"""

import sys
import json
from datetime import datetime
from pathlib import Path
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from loguru import logger

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import PostgresConfig
from quality.expectations.taxi_expectations import (
    ALL_EXPECTATIONS,
    TOTAL_EXPECTATIONS,
    CRITICAL_COUNT,
    WARNING_COUNT,
)


def get_pg_engine():
    """Create and return a SQLAlchemy engine."""
    return create_engine(PostgresConfig.get_connection_string())


# ============================================================
# Individual Validation Functions
# ============================================================

def check_not_null(df: pd.DataFrame, column: str) -> pd.Series:
    """Returns True for rows where column is NOT null."""
    return df[column].notna()


def check_between(
    df: pd.DataFrame, column: str, min_val: float, max_val: float
) -> pd.Series:
    """Returns True for rows where column is within [min, max]."""
    col = df[column]
    return (col.isna()) | ((col >= min_val) & (col <= max_val))


def check_in_set(
    df: pd.DataFrame, column: str, valid_values: list
) -> pd.Series:
    """Returns True for rows where column value is in the valid set."""
    col = df[column]
    return (col.isna()) | (col.isin(valid_values))


def check_year_between(
    df: pd.DataFrame, column: str, min_year: int, max_year: int
) -> pd.Series:
    """Returns True for rows where datetime column's year is in range."""
    col = pd.to_datetime(df[column], errors="coerce")
    year = col.dt.year
    return (col.isna()) | ((year >= min_year) & (year <= max_year))


def check_column_comparison(
    df: pd.DataFrame, col_a: str, col_b: str, operator: str
) -> pd.Series:
    """Returns True for rows where col_a operator col_b is satisfied."""
    a = pd.to_datetime(df[col_a], errors="coerce")
    b = pd.to_datetime(df[col_b], errors="coerce")

    # Treat nulls as passing (null checks are separate)
    null_mask = a.isna() | b.isna()

    if operator == ">=":
        return null_mask | (a >= b)
    elif operator == ">":
        return null_mask | (a > b)
    elif operator == "<=":
        return null_mask | (a <= b)
    elif operator == "<":
        return null_mask | (a < b)
    else:
        return pd.Series([True] * len(df))


# ============================================================
# Main Validation Engine
# ============================================================

def run_expectation(df: pd.DataFrame, expectation: dict) -> dict:
    """
    Run a single expectation against the DataFrame.

    Returns:
        dict with name, passed_count, failed_count, pass_mask
    """
    check_type = expectation["type"]
    name = expectation["name"]
    params = expectation.get("params", {})

    try:
        if check_type == "not_null":
            mask = check_not_null(df, expectation["column"])

        elif check_type == "between":
            mask = check_between(
                df, expectation["column"],
                params["min"], params["max"]
            )

        elif check_type == "in_set":
            mask = check_in_set(
                df, expectation["column"], params["values"]
            )

        elif check_type == "year_between":
            mask = check_year_between(
                df, expectation["column"],
                params["min_year"], params["max_year"]
            )

        elif check_type == "column_comparison":
            mask = check_column_comparison(
                df,
                params["column_a"],
                params["column_b"],
                params["operator"],
            )

        else:
            logger.warning(f"Unknown check type: {check_type}")
            mask = pd.Series([True] * len(df))

        passed = mask.sum()
        failed = len(df) - passed
        pass_rate = (passed / len(df)) * 100 if len(df) > 0 else 100

        return {
            "name": name,
            "description": expectation.get("description", ""),
            "severity": expectation["severity"],
            "passed": int(passed),
            "failed": int(failed),
            "pass_rate": round(pass_rate, 2),
            "mask": mask,
        }

    except Exception as e:
        logger.error(f"❌ Error running expectation '{name}': {e}")
        return {
            "name": name,
            "description": expectation.get("description", ""),
            "severity": expectation["severity"],
            "passed": 0,
            "failed": len(df),
            "pass_rate": 0.0,
            "mask": pd.Series([False] * len(df)),
        }


def validate_bronze_data(
    df: pd.DataFrame,
    expectations: list = None,
) -> dict:
    """
    Validate a DataFrame against all expectations.

    Args:
        df: Bronze layer DataFrame to validate
        expectations: List of expectation dicts (default: ALL_EXPECTATIONS)

    Returns:
        dict with:
          - clean_df: DataFrame of rows passing all critical checks
          - quarantine_df: DataFrame of rows failing any critical check
          - results: List of per-expectation results
          - summary: Overall summary statistics
    """
    expectations = expectations or ALL_EXPECTATIONS
    total_rows = len(df)

    logger.info(f"🔍 Running {len(expectations)} expectations on "
                f"{total_rows:,} rows...")
    logger.info(f"   Critical checks: {CRITICAL_COUNT}")
    logger.info(f"   Warning checks:  {WARNING_COUNT}")

    results = []
    # Track which rows fail critical checks
    critical_fail_mask = pd.Series([False] * total_rows, dtype=bool)
    failure_reasons = pd.Series([""] * total_rows, dtype=str)

    for exp in expectations:
        result = run_expectation(df, exp)
        results.append(result)

        status = "✅" if result["failed"] == 0 else "⚠️" if result["severity"] == "warning" else "❌"
        logger.info(
            f"   {status} {result['name']}: "
            f"{result['pass_rate']}% pass "
            f"({result['failed']:,} failed)"
        )

        # Mark rows failing critical checks for quarantine
        if exp["severity"] == "critical" and result["failed"] > 0:
            failed_rows = ~result["mask"]
            critical_fail_mask = critical_fail_mask | failed_rows
            # Append reason for failing rows
            reason_text = exp.get("description", exp["name"])
            failure_reasons = failure_reasons.where(
                ~failed_rows,
                failure_reasons + reason_text + "; "
            )

    # Split into clean and quarantined DataFrames
    clean_df = df[~critical_fail_mask].copy()
    quarantine_df = df[critical_fail_mask].copy()
    quarantine_df["quarantine_reason"] = failure_reasons[critical_fail_mask]

    # Summary
    clean_count = len(clean_df)
    quarantine_count = len(quarantine_df)
    overall_pass_rate = (clean_count / total_rows) * 100 if total_rows > 0 else 100

    summary = {
        "total_rows": total_rows,
        "clean_rows": clean_count,
        "quarantined_rows": quarantine_count,
        "overall_pass_rate": round(overall_pass_rate, 2),
        "total_expectations": len(expectations),
        "expectations_all_passed": sum(
            1 for r in results if r["failed"] == 0
        ),
        "timestamp": datetime.now().isoformat(),
    }

    logger.info("")
    logger.info("📊 Validation Summary:")
    logger.info(f"   Total rows:      {total_rows:,}")
    logger.info(f"   ✅ Clean rows:    {clean_count:,} ({overall_pass_rate:.1f}%)")
    logger.info(f"   ❌ Quarantined:   {quarantine_count:,}")

    return {
        "clean_df": clean_df,
        "quarantine_df": quarantine_df,
        "results": results,
        "summary": summary,
    }


# ============================================================
# Log Results to PostgreSQL
# ============================================================

def log_quality_results(
    engine,
    source_file: str,
    summary: dict,
    results: list,
) -> None:
    """Write validation results to gold.quality_log table."""
    # Build check details JSON (exclude mask)
    check_details = [
        {
            "name": r["name"],
            "severity": r["severity"],
            "passed": r["passed"],
            "failed": r["failed"],
            "pass_rate": r["pass_rate"],
        }
        for r in results
    ]

    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO gold.quality_log
                    (source_file, total_records, passed_records,
                     failed_records, pass_rate, check_details)
                VALUES
                    (:source_file, :total, :passed,
                     :failed, :pass_rate, :details)
            """),
            {
                "source_file": source_file,
                "total": summary["total_rows"],
                "passed": summary["clean_rows"],
                "failed": summary["quarantined_rows"],
                "pass_rate": summary["overall_pass_rate"],
                "details": json.dumps(check_details),
            },
        )

    logger.info(f"📝 Quality results logged for: {source_file}")


# ============================================================
# Full Validation Pipeline
# ============================================================

def validate_and_split_bronze() -> dict:
    """
    Read Bronze data from PostgreSQL, validate it,
    and return clean + quarantined DataFrames.

    Returns:
        dict with clean_df, quarantine_df, results, summary
    """
    engine = get_pg_engine()

    logger.info("📖 Reading Bronze data from PostgreSQL...")
    df = pd.read_sql(
        "SELECT * FROM bronze.raw_yellow_trips",
        con=engine,
    )
    logger.info(f"   Read {len(df):,} rows from bronze.raw_yellow_trips")

    if df.empty:
        logger.warning("⚠️  No data in Bronze table!")
        return None

    # Get unique source files for logging
    source_files = df["source_file"].unique().tolist()

    # Run validation
    validation = validate_bronze_data(df)

    # Log results per source file
    for src in source_files:
        file_df = df[df["source_file"] == src]
        file_validation = validate_bronze_data(file_df)
        log_quality_results(
            engine, src,
            file_validation["summary"],
            file_validation["results"],
        )

    return validation


if __name__ == "__main__":
    logger.info("🚕 TaxiPulse — Data Quality Validation Engine")
    logger.info("=" * 60)

    result = validate_and_split_bronze()

    if result:
        logger.info("")
        logger.info("📋 Final Results:")
        logger.info(f"   Clean rows ready for Silver: "
                     f"{len(result['clean_df']):,}")
        logger.info(f"   Quarantined rows: "
                     f"{len(result['quarantine_df']):,}")

        # Show quarantine reasons breakdown
        if len(result["quarantine_df"]) > 0:
            logger.info("")
            logger.info("🔍 Top quarantine reasons:")
            reasons = result["quarantine_df"]["quarantine_reason"].value_counts().head(10)
            for reason, count in reasons.items():
                logger.info(f"   {count:,} — {reason}")