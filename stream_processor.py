import json
from confluent_kafka import Consumer, Producer

BOOTSTRAP_SERVERS = "localhost:29092"
INPUT_TOPIC = "raw_events"
OUTPUT_TOPIC = "clean_events"
GROUP_ID = "silver-stream-processor"
VALID_EVENT_TYPES = ["PAGE_VIEW", "ADD_TO_CART", "PURCHASE"]

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
})

consumer.subscribe([INPUT_TOPIC])
producer = Producer({
    "bootstrap.servers": BOOTSTRAP_SERVERS
})

def is_valid_event(event):
    if not event.get("customer_id"):
        return False
    if event.get("event_type") not in VALID_EVENT_TYPES:
        return False
    if event.get("amount") is None or event.get("amount") <= 0:
        return False
    if not event.get("currency"):
        return False
    if event.get("is_valid") is not True:
        return False
    if not event.get("product_id"):
        return False
    if event.get("quantity", 0) <= 0:
        return False

    if event.get("unit_price", 0) <= 0:
        return False
    return True

print("Starting silver stream processor.....")
try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(msg.error())
            continue

        key = msg.key().decode() if msg.key() else None
        event = json.loads(msg.value().decode())
        if is_valid_event(event):
            producer.produce(
                topic=OUTPUT_TOPIC,
                key=key,
                value=json.dumps(event)
            )
            producer.poll(0)

            print(f"FORWARDED | key = {key} | event_type = {event['event_type']}")
        else:
            print(f"DROPPED | Customer={key} | Reason=Invalid Data")
        consumer.commit(msg)
except KeyboardInterrupt:
    print("\nStopping consumer...")            
finally:
    producer.flush()
    consumer.close()
    
        