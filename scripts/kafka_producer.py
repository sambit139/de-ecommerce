
import json
import time
import pandas as pd

from pathlib import Path
from kafka import KafkaProducer

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_FILE = PROJECT_ROOT / "data" / "ingested" / "prepared_clickstream.parquet"

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "ecommerce_clickstream"
BATCH_SIZE = 100


def clean_value(value):
    """Convert pandas and Python values into JSON-safe values."""
    if isinstance(value, dict):
        return {k: clean_value(v) for k, v in value.items()}

    if isinstance(value, (list, tuple)):
        return [clean_value(v) for v in value]

    if value is None:
        return None

    if pd.isna(value):
        return None

    if hasattr(value, "isoformat"):
        return value.isoformat()

    if hasattr(value, "item"):
        return value.item()

    return value


def main():
    print("=" * 55)
    print("ECOMMERCE KAFKA PRODUCER")
    print("=" * 55)

    if not DATA_FILE.exists():
        print(f"Dataset not found: {DATA_FILE}")
        return

    df = pd.read_parquet(DATA_FILE).head(BATCH_SIZE)

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
        acks="all"
    )

    print(f"Sending {len(df)} events to topic: {TOPIC}")

    sent = 0

    try:
        for _, row in df.iterrows():
            event = clean_value(row.to_dict())

            producer.send(
                TOPIC,
                key=str(event["session_id"]),
                value=event
            )

            sent += 1

            if sent % 10 == 0:
                print(f"Events sent: {sent}/{len(df)}")

            time.sleep(0.1)

        producer.flush()
        print("\nAll events delivered successfully!")

    except Exception as error:
        print(f"Producer error: {error}")

    finally:
        producer.close()

    print(f"Total events submitted: {sent}")


if __name__ == "__main__":
    main()
