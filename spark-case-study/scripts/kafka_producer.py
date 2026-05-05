"""
kafka_producer.py
Streams test_ratings.parquet to Kafka topic 'user_events' as JSON.
Partitioning: 2 partitions keyed by user_id hash.
"""

import json
import time
import argparse
import pandas as pd
from kafka import KafkaProducer
from datetime import datetime, timezone

KAFKA_BROKER   = "kafka:9092"
TOPIC          = "user_events"
NUM_PARTITIONS = 2
DATA_PATH      = "/data/test_ratings.parquet"
DEFAULT_DELAY  = 0.05

def get_partition(user_id: int) -> int:
    """
    Deterministic partition assignment: hash(user_id) % NUM_PARTITIONS.

    Why this strategy (document in report):
    - All events for the same user always land on the same partition,
      preserving per-user ordering — critical for computing per-user
      interaction counts in the streaming window.
    - Avoids session fragmentation: the downstream Spark consumer can
      compute user-level aggregates without shuffling across partitions.
    - With 2 partitions and integer user IDs, odd/even split gives a
      roughly uniform distribution for large user populations.
    """
    return hash(user_id) % NUM_PARTITIONS


def build_event(row: pd.Series) -> dict:
    """Convert a parquet row into the required JSON event schema."""
    return {
        "user_id":   int(row["user_id_int"]),
        "item_id":   int(row["item_id_int"]),
        "rating":    float(row["rating"]),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
        acks="all",           # wait for broker ack → no silent drops
        retries=3,
    )


def stream(delay: float, limit: int | None) -> None:
    print(f"Loading data from {DATA_PATH} ...")
    df = pd.read_parquet(DATA_PATH)

    if limit:
        df = df.head(limit)
        print(f"  Limited to {limit:,} rows for quick testing.")

    # Shuffle so we don't stream in rating-file order
    df = df.sample(frac=1, random_state=42).reset_index(drop=True)
    total = len(df)
    print(f"  {total:,} events to stream → topic '{TOPIC}' on {KAFKA_BROKER}")
    print(f"  Delay between messages: {delay}s  (~{1/delay:.0f} events/sec)\n")

    producer = make_producer()
    sent = 0
    errors = 0

    for _, row in df.iterrows():
        try:
            event     = build_event(row)
            partition = get_partition(event["user_id"])

            producer.send(
                topic=TOPIC,
                key=event["user_id"],        # key drives partition routing
                value=event,
                partition=partition,
            )

            sent += 1

            # Progress log every 500 messages
            if sent % 500 == 0:
                pct = sent / total * 100
                print(f"  [{pct:5.1f}%] Sent {sent:,}/{total:,} | "
                      f"errors: {errors} | last user_id: {event['user_id']}")

            time.sleep(delay)

        except Exception as e:
            errors += 1
            print(f"  [ERROR] {e}")

    producer.flush()
    producer.close()
    print(f"\nDone. Sent: {sent:,}  Errors: {errors}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stream test ratings to Kafka")
    parser.add_argument(
        "--delay", type=float, default=DEFAULT_DELAY,
        help=f"Seconds between messages (default: {DEFAULT_DELAY})"
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Cap number of events (omit = stream full test set)"
    )
    args = parser.parse_args()

    stream(delay=args.delay, limit=args.limit)