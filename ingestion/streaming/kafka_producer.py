"""
TaxiPulse — Kafka Producer
Simulates real-time taxi trip events by reading from Silver layer
and publishing them to a Kafka topic one by one, as if rides
are completing in real-time.

In production, this would be replaced by an actual event stream
from the taxi dispatch system.
"""

import sys
import json
import time
import random
from pathlib import Path
from datetime import datetime
from sqlalchemy import create_engine, text
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from loguru import logger

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from config.settings import KafkaConfig, PostgresConfig


def get_pg_engine():
    """Create and return a SQLAlchemy engine."""
    return create_engine(PostgresConfig.get_connection_string())


def create_producer(retries: int = 5, delay: int = 3) -> KafkaProducer:
    """Create Kafka producer with retry logic."""
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KafkaConfig.BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
            )
            logger.info(f"🔗 Connected to Kafka at {KafkaConfig.BOOTSTRAP_SERVERS}")
            return producer
        except NoBrokersAvailable:
            if attempt < retries - 1:
                logger.warning(
                    f"   Kafka not ready, retrying in {delay}s "
                    f"({attempt + 1}/{retries})..."
                )
                time.sleep(delay)
            else:
                raise Exception("❌ Could not connect to Kafka after retries")


def fetch_sample_trips(engine, limit: int = 1000) -> list:
    """
    Fetch a sample of trips from Silver layer to simulate streaming.
    Returns list of dicts (one per trip).
    """
    logger.info(f"📖 Fetching {limit:,} sample trips from Silver layer...")

    query = text(f"""
        SELECT
            vendor_id, pickup_datetime, dropoff_datetime,
            passenger_count, trip_distance, rate_code_id,
            pickup_location_id, dropoff_location_id,
            payment_type_id, fare_amount, extra, mta_tax,
            tip_amount, tolls_amount, improvement_surcharge,
            total_amount, congestion_surcharge, airport_fee,
            trip_duration_minutes
        FROM silver.clean_yellow_trips
        ORDER BY RANDOM()
        LIMIT :limit
    """)

    with engine.connect() as conn:
        result = conn.execute(query, {"limit": limit})
        columns = result.keys()
        trips = [dict(zip(columns, row)) for row in result.fetchall()]

    logger.info(f"   Fetched {len(trips):,} trips")
    return trips


def produce_trip_events(
    num_events: int = 500,
    delay_ms: int = 100,
    burst_mode: bool = False,
) -> dict:
    """
    Produce simulated taxi trip events to Kafka.

    Args:
        num_events: Number of events to produce
        delay_ms: Delay between events in milliseconds
        burst_mode: If True, send all events as fast as possible

    Returns:
        dict with production stats
    """
    engine = get_pg_engine()
    producer = create_producer()
    topic = KafkaConfig.TOPIC

    # Fetch sample trips
    trips = fetch_sample_trips(engine, limit=num_events)

    if not trips:
        logger.warning("⚠️ No trips found in Silver layer")
        return {"sent": 0, "failed": 0}

    logger.info(f"📤 Producing {len(trips):,} events to topic '{topic}'...")
    if not burst_mode:
        logger.info(f"   Delay between events: {delay_ms}ms")

    sent = 0
    failed = 0
    start_time = time.time()

    for i, trip in enumerate(trips):
        try:
            # Add streaming metadata
            event = {
                **trip,
                "event_id": f"evt-{int(time.time() * 1000)}-{i}",
                "event_timestamp": datetime.now().isoformat(),
                "event_type": "trip_completed",
            }

            # Use pickup_location_id as partition key
            # (ensures events from same zone go to same partition)
            key = str(trip.get("pickup_location_id", "unknown"))

            producer.send(topic, value=event, key=key)
            sent += 1

            # Progress logging every 100 events
            if (i + 1) % 100 == 0:
                elapsed = time.time() - start_time
                rate = sent / elapsed if elapsed > 0 else 0
                logger.info(
                    f"   Sent: {sent:,}/{len(trips):,} "
                    f"({rate:.0f} events/sec)"
                )

            # Simulate real-time delay
            if not burst_mode and delay_ms > 0:
                # Add some jitter for realism
                jitter = random.uniform(0.5, 1.5)
                time.sleep((delay_ms / 1000) * jitter)

        except Exception as e:
            logger.error(f"   ❌ Failed to send event {i}: {e}")
            failed += 1

    # Flush remaining messages
    producer.flush()
    producer.close()

    elapsed = time.time() - start_time
    rate = sent / elapsed if elapsed > 0 else 0

    logger.info("")
    logger.info("📊 Producer Summary:")
    logger.info(f"   ✅ Sent:     {sent:,}")
    logger.info(f"   ❌ Failed:   {failed:,}")
    logger.info(f"   ⏱️  Duration: {elapsed:.1f}s")
    logger.info(f"   📈 Rate:     {rate:.0f} events/sec")

    return {
        "sent": sent,
        "failed": failed,
        "duration_sec": round(elapsed, 1),
        "events_per_sec": round(rate, 1),
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="TaxiPulse Kafka Producer")
    parser.add_argument("-n", "--num-events", type=int, default=500,
                        help="Number of events to produce")
    parser.add_argument("-d", "--delay", type=int, default=100,
                        help="Delay between events in ms")
    parser.add_argument("--burst", action="store_true",
                        help="Send all events as fast as possible")
    args = parser.parse_args()

    logger.info("🚕 TaxiPulse — Kafka Producer")
    logger.info("=" * 60)

    results = produce_trip_events(
        num_events=args.num_events,
        delay_ms=args.delay,
        burst_mode=args.burst,
    )

    if results["sent"] > 0:
        logger.success("✅ Producer finished successfully!")
    else:
        logger.error("❌ No events were produced")