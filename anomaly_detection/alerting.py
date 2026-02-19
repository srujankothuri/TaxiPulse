"""
TaxiPulse — Alerting Module
Sends anomaly alerts via Slack webhook.
Falls back to console logging if Slack is not configured.
"""

import sys
import json
from pathlib import Path
import requests
from loguru import logger

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import SlackConfig


def format_alert_message(anomalies: list) -> dict:
    """
    Format anomalies into a Slack message payload.

    Args:
        anomalies: List of anomaly dicts

    Returns:
        Slack message payload dict
    """
    severity_emoji = {
        "critical": "🔴",
        "high": "🟠",
        "medium": "🟡",
    }

    # Header
    total = len(anomalies)
    critical = sum(1 for a in anomalies if a.get("severity") == "critical")
    high = sum(1 for a in anomalies if a.get("severity") == "high")

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"🚨 TaxiPulse Alert: {total} Anomalies Detected",
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*Summary:* {total} anomalies found\n"
                    f"🔴 Critical: {critical} | 🟠 High: {high} | "
                    f"🟡 Medium: {total - critical - high}"
                ),
            }
        },
        {"type": "divider"},
    ]

    # Top anomalies (max 10)
    for a in anomalies[:10]:
        emoji = severity_emoji.get(a.get("severity", "medium"), "🟡")
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"{emoji} *{a.get('severity', 'medium').upper()}*: {a.get('description', 'Unknown')}",
            }
        })

    if total > 10:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"_...and {total - 10} more anomalies_",
            }
        })

    return {"blocks": blocks}


def send_slack_alert(anomalies: list) -> bool:
    """
    Send anomaly alert to Slack.
    Falls back to console if Slack webhook is not configured.

    Args:
        anomalies: List of anomaly dicts with keys:
                   severity, description, anomaly_type, z_score

    Returns:
        True if alert was sent successfully
    """
    if not anomalies:
        logger.info("📭 No anomalies to alert on")
        return True

    webhook_url = SlackConfig.WEBHOOK_URL

    # Format message
    payload = format_alert_message(anomalies)

    # Check if Slack is configured
    if not webhook_url or webhook_url.startswith("https://hooks.slack.com/services/YOUR"):
        logger.info("📢 Slack not configured — logging alerts to console:")
        logger.info("-" * 40)
        for a in anomalies[:10]:
            severity = a.get("severity", "medium").upper()
            desc = a.get("description", "Unknown anomaly")
            logger.info(f"   🚨 [{severity}] {desc}")
        if len(anomalies) > 10:
            logger.info(f"   ...and {len(anomalies) - 10} more")
        logger.info("-" * 40)
        logger.info("   To enable Slack alerts, set SLACK_WEBHOOK_URL in .env")
        return True

    # Send to Slack
    try:
        response = requests.post(
            webhook_url,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        response.raise_for_status()
        logger.success(f"✅ Slack alert sent! ({len(anomalies)} anomalies)")
        return True

    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Failed to send Slack alert: {e}")
        # Fall back to console
        logger.info("📢 Falling back to console alerts:")
        for a in anomalies[:5]:
            logger.info(f"   🚨 [{a.get('severity', '?')}] {a.get('description', '?')}")
        return False


def send_anomaly_alerts_from_db() -> bool:
    """
    Read unsent anomalies from gold.anomaly_log and send alerts.
    Marks them as sent after alerting.
    """
    from sqlalchemy import create_engine, text
    from config.settings import PostgresConfig

    engine = create_engine(PostgresConfig.get_connection_string())

    # Fetch unsent anomalies
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT anomaly_id, anomaly_type, severity,
                   zone_id, z_score, description
            FROM gold.anomaly_log
            WHERE alert_sent = FALSE
            ORDER BY z_score DESC
            LIMIT 50
        """))
        rows = result.fetchall()
        columns = result.keys()

    if not rows:
        logger.info("📭 No unsent anomalies to alert on")
        return True

    anomalies = [dict(zip(columns, row)) for row in rows]
    logger.info(f"📬 Found {len(anomalies)} unsent anomalies")

    # Send alert
    success = send_slack_alert(anomalies)

    # Mark as sent
    if success:
        anomaly_ids = [a["anomaly_id"] for a in anomalies]
        with engine.begin() as conn:
            conn.execute(
                text("""
                    UPDATE gold.anomaly_log
                    SET alert_sent = TRUE
                    WHERE anomaly_id = ANY(:ids)
                """),
                {"ids": anomaly_ids},
            )
        logger.info(f"   Marked {len(anomaly_ids)} anomalies as sent")

    return success


if __name__ == "__main__":
    logger.info("🚕 TaxiPulse — Alerting Module")
    logger.info("=" * 60)
    send_anomaly_alerts_from_db()