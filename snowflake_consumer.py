import json
from confluent_kafka import Consumer
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd

BOOTSTRAP_SERVERS = "localhost:29092"
TOPIC_NAME = "clean_events"
GROUP_ID = "snowflake_loader_new"

SNOWFLAKE_CONFIG = {
    "user" : "Write your username",
    "password" : "write your pass",
    "account" : "write your url",
    "warehouse" : "COMPUTE_WH",
    "database" : "KAFKA_DB",
    "schema" : "STREAMING"
}
BATCH_SIZE = 10

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
})

consumer.subscribe([TOPIC_NAME])
sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
print("Connected to Snowflake")
print("Starting Kafka -> Snowflake Loader...")

buffer = []

def flush_to_snowflake(records):
    df = pd.DataFrame(records)
    df.columns = [c.upper() for c in df.columns]
    success, nchunksn, nrows, _ = write_pandas(
        conn = sf_conn,
        df = df,
        table_name = "KAFKA_EVENTS_SILVER" 
    )
    if not success:
        raise Exception("snowflake insert failed")
    print(f"Inserted {nrows} rows into snowflake")
try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(msg.error())
            continue

        event = json.loads(msg.value().decode())
        buffer.append({
            "event_id": event["event_id"],
            "session_id": event["session_id"],
            "customer_id": event["customer_id"],
            "customer_name": event["customer_name"],
            "city": event["city"],
            "country": event["country"],
            "age": event["age"],
            "gender": event["gender"],
            "segment": event["segment"],
            "channel": event["channel"],
            "product_id": event["product_id"],
            "product_name": event["product_name"],
            "category": event["category"],
            "brand": event["brand"],
            "unit_price": event["unit_price"],
            "quantity": event["quantity"],
            "discount": event["discount"],
            "subtotal": event["subtotal"],
            "amount": event["amount"],
            "order_status": event["order_status"],
            "device": event["device"],
            "payment_method": event["payment_method"],
            "event_type": event["event_type"],
            "currency": event["currency"],
            "event_timestamp": event["event_timestamp"]
        })
        if len(buffer) >= BATCH_SIZE:
            try:
                flush_to_snowflake(buffer)
                consumer.commit(msg)
                buffer.clear()
            except Exception as e:
                print(f"ERROR inserting batch: {e}")
except KeyboardInterrupt:
    print("\nStopping loader...")
finally: 
    try:
        if buffer: 
            flush_to_snowflake(buffer) 
            consumer.commit(asynchronous=False)
    finally:        
        sf_conn.close() 
        consumer.close()