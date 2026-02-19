"""
TaxiPulse — Kafka Consumer
Consumes real-time taxi trip events from Kafka,
validates them, and writes to PostgreSQL Silver layer.

Demonstrates the streaming ingestion path that runs
parallel to the batch path.
"""

import sys
import json
import time
from pathlib import Path
from datetime import datetime
from sqlalchemy import create_engine, text
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from loguru import logger

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from config.settings import KafkaConfig, PostgresConfig


def get_pg_engine():
    """Create and return a SQLAlchemy engine."""
    return create_engine(PostgresConfig.get_connection_string())


def create_consumer(retries: int = 5, delay: int = 3) -> KafkaConsumer:
    """Create Kafka consumer with retry logic."""
    for attempt in range(retries):
        try:
            consumer = KafkaConsumer(
                KafkaConfig.TOPIC,
                bootstrap_servers=KafkaConfig.BOOTSTRAP_SERVERS,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="taxipulse-consumer-group",
                consumer_timeout_ms=10000,  # Stop after 10s of no messages
            )
            logger.info(
                f"🔗 Connected to Kafka at {KafkaConfig.BOOTSTRAP_SERVERS}"
            )
            logger.info(f"   Subscribed to topic: {KafkaConfig.TOPIC}")
            return consumer
        except NoBrokersAvailable:
            if attempt < retries - 1:
                logger.warning(
                    f"   Kafka not ready, retrying in {delay}s "
                    f"({attempt + 1}/{retries})..."
                )
                time.sleep(delay)
            else:
                raise Exception("❌ Could not connect to Kafka after retries")


def validate_event(event: dict) -> tuple:
    """
    Quick validation of a streaming event.
    Returns (is_valid, reason).
    """
    # Check required fields
    required = [
        "pickup_location_id", "dropoff_location_id",
        "fare_amount", "total_amount",
    ]
    for field in required:
        if field not in event or event[field] is None:
            return False, f"Missing required field: {field}"

    # Range checks
    fare = event.get("fare_amount", 0)
    if not isinstance(fare, (int, float)):
        try:
            fare = float(fare)
        except (ValueError, TypeError):
            return False, f"Invalid fare_amount: {fare}"

    if fare < 0 or fare > 500:
        return False, f"Fare out of range: {fare}"

    total = event.get("total_amount", 0)
    if not isinstance(total, (int, float)):
        try:
            total = float(total)
        except (ValueError, TypeError):
            return False, f"Invalid total_amount: {total}"

    if total < 0 or total > 1000:
        return False, f"Total out of range: {total}"

    return True, "OK"


def insert_event_to_silver(engine, event: dict) -> bool:
    """Insert a validated streaming event into Silver layer."""
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO silver.clean_yellow_trips (
                    vendor_id, pickup_datetime, dropoff_datetime,
                    passenger_count, trip_distance, rate_code_id,
                    pickup_location_id, dropoff_location_id,
                    payment_type_id, fare_amount, extra, mta_tax,
                    tip_amount, tolls_amount, improvement_surcharge,
                    total_amount, congestion_surcharge, airport_fee,
                    trip_duration_minutes, source_file
                ) VALUES (
                    :vendor_id, :pickup_datetime, :dropoff_datetime,
                    :passenger_count, :trip_distance, :rate_code_id,
                    :pickup_location_id, :dropoff_location_id,
                    :payment_type_id, :fare_amount, :extra, :mta_tax,
                    :tip_amount, :tolls_amount, :improvement_surcharge,
                    :total_amount, :congestion_surcharge, :airport_fee,
                    :trip_duration_minutes, :source_file
                )
            """), {
                "vendor_id": event.get("vendor_id", 0),
                "pickup_datetime": event.get("pickup_datetime"),
                "dropoff_datetime": event.get("dropoff_datetime"),
                "passenger_count": event.get("passenger_count", 0),
                "trip_distance": event.get("trip_distance", 0),
                "rate_code_id": event.get("rate_code_id", 0),
                "pickup_location_id": event.get("pickup_location_id"),
                "dropoff_location_id": event.get("dropoff_location_id"),
                "payment_type_id": event.get("payment_type_id", 0),
                "fare_amount": event.get("fare_amount", 0),
                "extra": event.get("extra", 0),
                "mta_tax": event.get("mta_tax", 0),
                "tip_amount": event.get("tip_amount", 0),
                "tolls_amount": event.get("tolls_amount", 0),
                "improvement_surcharge": event.get("improvement_surcharge", 0),
                "total_amount": event.get("total_amount", 0),
                "congestion_surcharge": event.get("congestion_surcharge", 0),
                "airport_fee": event.get("airport_fee", 0),
                "trip_duration_minutes": event.get("trip_duration_minutes", 0),
                "source_file": "kafka-stream",
            })
        return True
    except Exception as e:
        logger.error(f"   ❌ DB insert failed: {e}")
        return False


def consume_trip_events(max_events: int = None) -> dict:
    """
    Consume taxi trip events from Kafka topic.

    Args:
        max_events: Maximum events to consume (None = consume until timeout)

    Returns:
        dict with consumption stats
    """
    engine = get_pg_engine()
    consumer = create_consumer()

    logger.info(f"👂 Consuming events from '{KafkaConfig.TOPIC}'...")
    if max_events:
        logger.info(f"   Max events: {max_events:,}")

    consumed = 0
    valid = 0
    invalid = 0
    inserted = 0
    start_time = time.time()

    for message in consumer:
        event = message.value
        consumed += 1

        # Validate
        is_valid, reason = validate_event(event)

        if is_valid:
            valid += 1
            # Insert to Silver
            if insert_event_to_silver(engine, event):
                inserted += 1
        else:
            invalid += 1
            if invalid <= 5:  # Only log first 5 invalid events
                logger.warning(f"   ⚠️ Invalid event: {reason}")

        # Progress logging
        if consumed % 100 == 0:
            elapsed = time.time() - start_time
            rate = consumed / elapsed if elapsed > 0 else 0
            logger.info(
                f"   Consumed: {consumed:,} | "
                f"Valid: {valid:,} | "
                f"Inserted: {inserted:,} | "
                f"Rate: {rate:.0f}/sec"
            )

        # Check max events limit
        if max_events and consumed >= max_events:
            logger.info(f"   Reached max events limit ({max_events})")
            break

    consumer.close()

    elapsed = time.time() - start_time
    rate = consumed / elapsed if elapsed > 0 else 0

    logger.info("")
    logger.info("📊 Consumer Summary:")
    logger.info(f"   📥 Consumed: {consumed:,}")
    logger.info(f"   ✅ Valid:    {valid:,}")
    logger.info(f"   ❌ Invalid:  {invalid:,}")
    logger.info(f"   💾 Inserted: {inserted:,}")
    logger.info(f"   ⏱️  Duration: {elapsed:.1f}s")
    logger.info(f"   📈 Rate:     {rate:.0f} events/sec")

    return {
        "consumed": consumed,
        "valid": valid,
        "invalid": invalid,
        "inserted": inserted,
        "duration_sec": round(elapsed, 1),
        "events_per_sec": round(rate, 1),
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="TaxiPulse Kafka Consumer")
    parser.add_argument("-n", "--max-events", type=int, default=None,
                        help="Max events to consume (default: all)")
    args = parser.parse_args()

    logger.info("🚕 TaxiPulse — Kafka Consumer")
    logger.info("=" * 60)

    results = consume_trip_events(max_events=args.max_events)

    if results["consumed"] > 0:
        logger.success("✅ Consumer finished successfully!")
    else:
        logger.info("📭 No messages to consume")