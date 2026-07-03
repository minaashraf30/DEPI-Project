import json
import random
import time
import uuid
from datetime import datetime, timedelta, UTC
from confluent_kafka import Producer
BOOTSTRAP_SERVERS = "localhost:29092"
Topic_Name = "raw_events"
producer = Producer({
    "bootstrap.servers" : BOOTSTRAP_SERVERS
})

EVENT_TYPES = ["PAGE_VIEW", "ADD_TO_CART", "PURCHASE"]
INVALID_EVENT_TYPES = ["CLICK", "VIEW", "PAY"]
ORDER_STATUS = ["Completed", "Pending", "Cancelled"]
CUSTOMERS = [
    {"customer_id": "CUST_1", "customer_name": "Ahmed Ali", "city": "Cairo", "age": 24, "gender": "Male", "segment": "Gold"},
    {"customer_id": "CUST_2", "customer_name": "Sara Mohamed", "city": "Alexandria", "age": 29, "gender": "Female", "segment": "Silver"},
    {"customer_id": "CUST_3", "customer_name": "Omar Hassan", "city": "Giza", "age": 34, "gender": "Male", "segment": "Gold"},
    {"customer_id": "CUST_4", "customer_name": "Mona Adel", "city": "Mansoura", "age": 27, "gender": "Female", "segment": "Bronze"},
    {"customer_id": "CUST_5", "customer_name": "Youssef Samy", "city": "Tanta", "age": 31, "gender": "Male", "segment": "Silver"}
]

PRODUCTS = [
    {"product_id": "P001", "product_name": "Laptop", "category": "Electronics", "brand": "Dell", "price": 1200},
    {"product_id": "P002", "product_name": "Headphones", "category": "Electronics", "brand": "Sony", "price": 150},
    {"product_id": "P003", "product_name": "T-Shirt", "category": "Fashion", "brand": "Nike", "price": 40},
    {"product_id": "P004", "product_name": "Sneakers", "category": "Fashion", "brand": "Adidas", "price": 180},
    {"product_id": "P005", "product_name": "Coffee Machine", "category": "Home", "brand": "Philips", "price": 350}
]

DEVICES = ["Mobile", "Desktop", "Tablet"]

PAYMENT_METHODS = ["Visa", "MasterCard", "Cash", "PayPal"]

CHANNELS = ["Website", "Mobile App"]

def random_timestamp_last_6_days():
    now = datetime.now(UTC)
    past = now - timedelta(days=6)
    random_seconds = random.uniform(0, (now - past).total_seconds())
    return past + timedelta(seconds=random_seconds)
def generate_event():
    is_invalid = random.random() < 0.25
    customer = random.choice(CUSTOMERS)
    product = random.choice(PRODUCTS)
    channel = random.choice(CHANNELS)
    device = random.choice(DEVICES)
    payment_method = random.choice(PAYMENT_METHODS)
    order_status = random.choice(ORDER_STATUS)
    quantity = random.randint(1, 3)
    discount = random.choice([0, 10, 20, 30, 50])
    
    event_type = random.choice(EVENT_TYPES)
    currency = "USD"

    subtotal = product["price"] * quantity
    amount = round(subtotal - discount, 2)
    invalid_field = None
    if is_invalid:
        invalid_field = random.choice([
            "customer_id",
            "event_type",
            "amount",
            "currency"
        ])
    event = {
        "event_id": str(uuid.uuid4()),
        "session_id": str(uuid.uuid4()),
        "customer_id": None if invalid_field == "customer_id" else customer["customer_id"],
        "customer_name": customer["customer_name"],
        "city": customer["city"],
        "country": "Egypt",
        "age": customer["age"],
        "gender": customer["gender"],
        "segment": customer["segment"],
        "channel": channel,
        "product_id": product["product_id"],
        "product_name": product["product_name"],
        "category": product["category"],
        "brand": product["brand"],
        "unit_price": product["price"],
        "quantity": quantity,
        "discount": discount,
        "subtotal": subtotal,
        "order_status": order_status,
        "device": device,
        "payment_method": payment_method,
        "event_type": (
            random.choice(INVALID_EVENT_TYPES)
            if invalid_field == "event_type"
            else event_type
        ),
        "amount": (
            random.uniform(-500,-10)
            if invalid_field == "amount"
            else amount
        ),
        "currency": None if invalid_field == "currency" else currency,
        "event_timestamp": random_timestamp_last_6_days().isoformat(),
        "is_valid": not is_invalid,
        "invalid_field": invalid_field
    }
    return event["customer_id"], event
print("starting kafka producer....")

while(True):
    key, event = generate_event()
    producer.produce(
    topic=Topic_Name,
    key=key,
    value=json.dumps(event)
    )
    producer.poll(0)
    print(f"Produced event | key = {key} | value = {event['is_valid']}")
    time.sleep(1)

