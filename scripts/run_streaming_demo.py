"""
TaxiPulse — Streaming Demo
Demonstrates the real-time path:
  1. Producer sends simulated taxi events to Kafka
  2. Consumer reads from Kafka, validates, and inserts to Silver

Run this to showcase the streaming capability of TaxiPulse.
"""

import sys
import time
import threading
from pathlib import Path
from loguru import logger

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ingestion.streaming.kafka_producer import produce_trip_events
from ingestion.streaming.kafka_consumer import consume_trip_events


def run_producer(num_events: int, delay_ms: int):
    """Run producer in a thread."""
    logger.info("🚀 Starting Producer...")
    produce_trip_events(
        num_events=num_events,
        delay_ms=delay_ms,
        burst_mode=True,
    )


def run_consumer(max_events: int):
    """Run consumer in a thread."""
    # Small delay to let producer start first
    time.sleep(3)
    logger.info("👂 Starting Consumer...")
    consume_trip_events(max_events=max_events)


def main():
    num_events = 500

    logger.info("🚕 TaxiPulse — Streaming Demo")
    logger.info("=" * 60)
    logger.info(f"   Events to produce: {num_events}")
    logger.info(f"   Mode: Burst (fast as possible)")
    logger.info("")

    # Step 1: Produce events
    logger.info("📤 STEP 1: Producing events to Kafka...")
    logger.info("-" * 40)
    producer_results = produce_trip_events(
        num_events=num_events,
        delay_ms=0,
        burst_mode=True,
    )

    if producer_results["sent"] == 0:
        logger.error("❌ No events produced. Aborting.")
        return False

    # Step 2: Consume events
    logger.info("")
    logger.info("📥 STEP 2: Consuming events from Kafka...")
    logger.info("-" * 40)
    consumer_results = consume_trip_events(max_events=num_events)

    # Summary
    logger.info("")
    logger.info("=" * 60)
    logger.info("🎬 Streaming Demo Summary:")
    logger.info(f"   📤 Produced:  {producer_results['sent']:,} events")
    logger.info(f"   📥 Consumed:  {consumer_results['consumed']:,} events")
    logger.info(f"   ✅ Validated: {consumer_results['valid']:,} events")
    logger.info(f"   💾 Inserted:  {consumer_results['inserted']:,} to Silver")
    logger.success("✅ Streaming demo complete!")

    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)