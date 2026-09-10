import json
import random
import time
import uuid
from io import BytesIO

from confluent_kafka import Producer
from fastavro import parse_schema, schemaless_writer


# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
ORDERS_TOPIC = "orders"

# How many orders to send, and how long to wait between each
NUM_ORDERS = 20
SEND_INTERVAL_SECONDS = 1.0

PRODUCTS = ["Item1", "Item2", "Item3", "Item4", "Item5"]
PRICE_MIN = 5.00
PRICE_MAX = 500.00


# ============================================================
# LOAD AVRO SCHEMA
# ============================================================

with open("schemas/order.avsc", "r") as file:
    schema = json.load(file)

parsed_schema = parse_schema(schema)
print("Avro schema loaded successfully")


# ============================================================
# KAFKA PRODUCER
# ============================================================

producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS
})
print("Kafka producer created successfully")


# ============================================================
# DELIVERY CALLBACK
# ============================================================

def delivery_report(err, msg):
    if err is not None:
        print("Message delivery failed:", err)
    else:
        print(
            "Delivered:", msg.topic(),
            "partition:", msg.partition(),
            "offset:", msg.offset()
        )


# ============================================================
# GENERATE + SEND A RANDOM ORDER
# ============================================================

def generate_order(index):
    return {
        "orderId": f"{1000 + index}",
        "product": random.choice(PRODUCTS),
        "price": round(random.uniform(PRICE_MIN, PRICE_MAX), 2)
    }


def send_order(order):
    bytes_writer = BytesIO()
    schemaless_writer(bytes_writer, parsed_schema, order)
    avro_data = bytes_writer.getvalue()

    producer.produce(
        topic=ORDERS_TOPIC,
        key=order["orderId"].encode("utf-8"),
        value=avro_data,
        callback=delivery_report
    )

    # Serve delivery callbacks without blocking on a full flush
    producer.poll(0)


# ============================================================
# MAIN LOOP
# ============================================================

print(f"\nSending {NUM_ORDERS} randomized orders to '{ORDERS_TOPIC}'...\n")

for i in range(NUM_ORDERS):
    order = generate_order(i)
    print(f"Producing order: {order}")
    send_order(order)
    time.sleep(SEND_INTERVAL_SECONDS)

# Make sure every message is actually delivered before exiting
producer.flush()
print("\nAll orders sent.")