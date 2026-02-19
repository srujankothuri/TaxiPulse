#!/bin/bash
# ============================================================
# TaxiPulse — Run Full Pipeline (End-to-End)
# ============================================================
# Usage: bash scripts/run_pipeline.sh
# ============================================================

set -e  # Exit on any error

echo "🚕 TaxiPulse — Full Pipeline"
echo "============================================================"

echo ""
echo "📥 Step 1: Ingestion (Download + Upload to MinIO + Load Bronze)"
python scripts/run_ingestion.py

echo ""
echo "🔍 Step 2: Data Quality Validation"
python scripts/run_quality_check.py

echo ""
echo "🥈 Step 3: Silver Layer Transformation"
python scripts/run_silver.py

echo ""
echo "🥇 Step 4: Gold Layer (Star Schema + Aggregations)"
python scripts/run_gold.py

echo ""
echo "🚨 Step 5: Anomaly Detection + Alerting"
python scripts/run_anomaly_detection.py

echo ""
echo "============================================================"
echo "✅ Full pipeline complete!"
echo "============================================================"