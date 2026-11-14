
import json
from kafka import KafkaConsumer

TOPIC = "ecommerce_clickstream"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id=None,
    consumer_timeout_ms=10000,
    value_deserializer=lambda data: json.loads(data.decode("utf-8"))
)

print("=" * 55)
print("ECOMMERCE KAFKA CONSUMER")
print("=" * 55)

count = 0

try:
    for message in consumer:
        count += 1

        if count <= 5:
            event = message.value
            print(f"\nEvent {count}")
            print(f"Partition: {message.partition}")
            print(f"Offset: {message.offset}")
            print(f"Event name: {event.get('event_name')}")
            print(f"Session ID: {event.get('session_id')}")

    print("\n" + "=" * 55)
    print(f"TOTAL EVENTS CONSUMED: {count}")
    print("=" * 55)

finally:
    consumer.close()
