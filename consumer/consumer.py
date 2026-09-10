from io import BytesIO
import json
import time
import random

from confluent_kafka import Consumer, Producer
from fastavro import schemaless_reader, schemaless_writer


# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

ORDERS_TOPIC = "orders"
DLQ_TOPIC = "orders-dlq"

CONSUMER_GROUP = "order-consumer-group"

MAX_RETRIES = 3
RETRY_DELAY = 2

# Probability of a temporary processing failure
FAILURE_PROBABILITY = 0.30


# ============================================================
# KAFKA CONSUMER
# ============================================================
# enable.auto.commit is OFF on purpose: we only want an offset
# committed once a message has been fully handled (either
# processed successfully or safely written to the DLQ). If we
# let Kafka auto-commit on a timer, a crash mid-retry could
# advance past a message before it's actually been dealt with,
# and it would be lost forever instead of ending up in the DLQ.

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": CONSUMER_GROUP,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
})


# ============================================================
# KAFKA PRODUCER FOR DLQ
# ============================================================

dlq_producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS
})


# ============================================================
# RUNNING AVERAGE VARIABLES
# ============================================================

total_price = 0.0
order_count = 0


# ============================================================
# LOAD AVRO SCHEMA
# ============================================================

try:
    with open("schemas/order.avsc", "r") as f:
        schema = json.load(f)

    print("Avro schema loaded successfully.")

except Exception as e:
    print(f"Error loading Avro schema: {e}")
    consumer.close()
    exit(1)


# ============================================================
# PROCESS ORDER
# ============================================================

def process_order(order):
    """
    Simulates processing an order.

    There is a 30% probability of a temporary failure.
    """

    # Random failure
    if random.random() < FAILURE_PROBABILITY:
        raise Exception("Temporary processing failure")

    print("Order processed successfully!")


# ============================================================
# SEND ORDER TO DEAD LETTER QUEUE
# ============================================================

def send_to_dlq(order):
    """
    Sends permanently failed orders to the orders-dlq topic.
    """

    print(
        f"\nSending order {order['orderId']} "
        f"to Dead Letter Queue..."
    )

    try:

        # Convert order dictionary into Avro bytes
        bytes_writer = BytesIO()

        schemaless_writer(
            bytes_writer,
            schema,
            order
        )

        avro_data = bytes_writer.getvalue()

        # Produce message to DLQ
        dlq_producer.produce(
            topic=DLQ_TOPIC,
            key=order["orderId"].encode("utf-8"),
            value=avro_data
        )

        # Make sure the message is delivered
        dlq_producer.flush()

        print(
            f"Order {order['orderId']} "
            f"successfully sent to {DLQ_TOPIC}"
        )

    except Exception as e:

        print(
            f"Failed to send order {order['orderId']} "
            f"to DLQ: {e}"
        )


# ============================================================
# RETRY LOGIC
# ============================================================

def process_with_retry(order):
    """
    Attempts to process an order up to MAX_RETRIES times.

    If all attempts fail, the order is sent to the DLQ.
    """

    for attempt in range(1, MAX_RETRIES + 1):

        try:

            print(
                f"\nProcessing order "
                f"{order['orderId']}..."
            )

            print(
                f"Attempt {attempt}/{MAX_RETRIES}"
            )

            # Try to process the order
            process_order(order)

            # Processing succeeded
            return True

        except Exception as e:

            print(
                f"Processing failed: {e}"
            )

            # Retry if attempts remain
            if attempt < MAX_RETRIES:

                print(
                    f"Retrying in {RETRY_DELAY} seconds..."
                )

                time.sleep(RETRY_DELAY)

    # All retries failed
    print(
        f"\nOrder {order['orderId']} "
        f"failed after {MAX_RETRIES} attempts."
    )

    # Send permanently failed order to DLQ
    send_to_dlq(order)

    return False


# ============================================================
# MAIN CONSUMER LOOP
# ============================================================

print("\n========================================")
print("      KAFKA ORDER CONSUMER")
print("========================================")

print(f"Kafka broker : {KAFKA_BOOTSTRAP_SERVERS}")
print(f"Topic        : {ORDERS_TOPIC}")
print(f"Consumer     : {CONSUMER_GROUP}")
print(f"DLQ topic    : {DLQ_TOPIC}")
print(
    f"Failure rate : "
    f"{FAILURE_PROBABILITY * 100:.0f}%"
)
print(
    f"Max retries  : {MAX_RETRIES}"
)

print("\nWaiting for orders...\n")


# Subscribe to orders topic
consumer.subscribe([ORDERS_TOPIC])


try:

    while True:

        # Poll Kafka
        msg = consumer.poll(1.0)

        # No message available
        if msg is None:
            continue

        # Kafka error
        if msg.error():

            print(
                f"Kafka error: {msg.error()}"
            )

            continue

        try:

            # ====================================================
            # AVRO DESERIALIZATION
            # ====================================================

            bytes_reader = BytesIO(
                msg.value()
            )

            order = schemaless_reader(
                bytes_reader,
                schema
            )

            # ====================================================
            # DISPLAY RECEIVED MESSAGE
            # ====================================================

            print("\n----------------------------------------")
            print("New order received")
            print("----------------------------------------")

            print(
                f"Order ID : {order['orderId']}"
            )

            print(
                f"Product  : {order['product']}"
            )

            print(
                f"Price    : {order['price']}"
            )

            print(
                f"Partition: {msg.partition()}"
            )

            print(
                f"Offset   : {msg.offset()}"
            )

            # ====================================================
            # PROCESS WITH RETRY
            # ====================================================

            success = process_with_retry(order)

            # ====================================================
            # UPDATE RUNNING AVERAGE
            # ====================================================

            if success:

                total_price += order["price"]

                order_count += 1

                running_average = (
                    total_price / order_count
                )

                print("\n========================================")
                print("ORDER PROCESSING SUMMARY")
                print("========================================")

                print(
                    f"Total orders processed: "
                    f"{order_count}"
                )

                print(
                    f"Total price: "
                    f"{total_price}"
                )

                print(
                    f"Running average price: "
                    f"{running_average}"
                )

                print("========================================")

            else:

                print(
                    f"\nOrder {order['orderId']} "
                    f"was sent to the DLQ."
                )

            # ====================================================
            # COMMIT OFFSET
            # ====================================================
            # Only commit now that the message has been fully
            # handled — either processed successfully or safely
            # written to the DLQ. This is why auto-commit is off.

            consumer.commit(msg)

        except Exception as e:

            print(
                f"\nError processing Kafka message: {e}"
            )


except KeyboardInterrupt:

    print("\nConsumer stopped by user.")


finally:

    # Close consumer
    consumer.close()

    # Flush any remaining DLQ messages
    dlq_producer.flush()

    print("\nKafka consumer closed.")