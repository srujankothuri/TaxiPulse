"""
TaxiPulse — Streaming Demo DAG
Manually triggered DAG that demonstrates the real-time streaming path:
  1. Produce simulated taxi events to Kafka
  2. Consume events from Kafka and insert to Silver

NOTE: In production, the Kafka consumer would run as a
long-running service (e.g., Kubernetes pod), NOT as an
Airflow task. This DAG exists for demonstration and testing.
"""

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")


def task_produce_events(**context):
    """Produce simulated taxi events to Kafka."""
    from ingestion.streaming.kafka_producer import produce_trip_events
    from loguru import logger

    num_events = 500
    logger.info(f"📤 Producing {num_events} events to Kafka...")

    results = produce_trip_events(
        num_events=num_events,
        delay_ms=0,
        burst_mode=True,
    )

    context["ti"].xcom_push(key="producer_results", value=results)
    logger.info(f"✅ Produced {results['sent']} events")
    return results["sent"]


def task_consume_events(**context):
    """Consume events from Kafka and insert to Silver."""
    from ingestion.streaming.kafka_consumer import consume_trip_events
    from loguru import logger

    logger.info("📥 Consuming events from Kafka...")

    results = consume_trip_events(max_events=500)

    context["ti"].xcom_push(key="consumer_results", value=results)
    logger.info(
        f"✅ Consumed {results['consumed']} events, "
        f"inserted {results['inserted']} to Silver"
    )
    return results["consumed"]


default_args = {
    "owner": "taxipulse",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="taxipulse_streaming_demo",
    default_args=default_args,
    description="Demo: Kafka streaming path (produce → consume → Silver)",
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["taxipulse", "streaming", "demo"],
) as dag:

    produce = PythonOperator(
        task_id="produce_kafka_events",
        python_callable=task_produce_events,
        provide_context=True,
    )

    consume = PythonOperator(
        task_id="consume_kafka_events",
        python_callable=task_consume_events,
        provide_context=True,
    )

    produce >> consume