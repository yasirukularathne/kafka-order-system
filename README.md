# Kafka Order Processing System

A Kafka-based producer/consumer system that generates and processes order
messages using Avro serialization, with real-time price aggregation, retry
logic for transient failures, and a Dead Letter Queue (DLQ) for permanently
failed messages.

## Architecture

```
producer/producer.py  --Avro-->  [orders topic]  --Avro-->  consumer/consumer.py
                                                                    |
                                                        (3 failed attempts)
                                                                    v
                                                          [orders-dlq topic]
```

- **Broker**: single-node Kafka (KRaft mode, no Zookeeper), defined in
  `docker-compose.yml`, 3 partitions on the `orders` topic.
- **Producer**: generates randomized orders (random product, random price)
  and sends them Avro-serialized to the `orders` topic.
- **Consumer**: subscribes to `orders`, deserializes each message, attempts
  to process it (with a simulated 30% chance of transient failure), retries
  up to 3 times, and if all retries fail, forwards the order Avro-serialized
  to the `orders-dlq` topic. It also maintains a running average of prices
  for all successfully processed orders.

## Order Schema (`schemas/order.avsc`)

| Field     | Type   | Description                       |
| --------- | ------ | --------------------------------- |
| `orderId` | string | Unique identifier for the order   |
| `product` | string | Name of the purchased item        |
| `price`   | float  | Price of the product (randomized) |

Both the producer and consumer load this schema from disk at startup, so
there is a single source of truth for the message format.

## Prerequisites

- Docker (for running the Kafka broker)
- Python 3.11+
- A virtual environment with dependencies installed (see below)

## Setup

```bash
# 1. Start the Kafka broker
docker-compose up -d

# 2. Create and activate a virtual environment
python -m venv .venv
.venv\Scripts\activate        # Windows
# source .venv/bin/activate   # macOS/Linux

# 3. Install dependencies
pip install -r requirements.txt
```

Wait ~20-30 seconds after `docker-compose up -d` for the broker to finish
starting before running the producer or consumer.

## Running the System

**Important:** both scripts load the schema via the relative path
`schemas/order.avsc`, so they must be run from the project root — not from
inside `producer/` or `consumer/`.

Open two terminals, both from the project root, both with the virtual
environment activated:

```bash
# Terminal 1 — start the consumer first
python consumer/consumer.py

# Terminal 2 — then start the producer
python producer/producer.py
```

The producer sends 20 randomly generated orders (one per second) and exits.
The consumer runs continuously until stopped with `Ctrl+C`.

## Configuration

Key parameters, adjustable at the top of each script:

| Parameter               | File        | Default | Purpose                                  |
| ----------------------- | ----------- | ------- | ---------------------------------------- |
| `NUM_ORDERS`            | producer.py | 20      | How many orders to generate per run      |
| `SEND_INTERVAL_SECONDS` | producer.py | 1.0     | Delay between sends                      |
| `FAILURE_PROBABILITY`   | consumer.py | 0.30    | Simulated chance of a transient failure  |
| `MAX_RETRIES`           | consumer.py | 3       | Retry attempts before sending to the DLQ |
| `RETRY_DELAY`           | consumer.py | 2       | Seconds between retry attempts           |

## How Each Requirement Is Met

- **Avro serialization** — both producer and consumer use `fastavro`'s
  `schemaless_writer`/`schemaless_reader` against the shared
  `schemas/order.avsc` schema.
- **Real-time aggregation** — the consumer tracks `total_price` and
  `order_count` across all successfully processed orders and prints the
  running average after each one.
- **Retry logic** — `process_with_retry()` in `consumer.py` attempts each
  order up to `MAX_RETRIES` times with a delay between attempts, simulating
  transient failures via `FAILURE_PROBABILITY`.
- **Dead Letter Queue** — orders that exhaust all retry attempts are
  Avro-serialized and published to the `orders-dlq` topic via `send_to_dlq()`.
- **Reliable offset handling** — the consumer disables auto-commit
  (`enable.auto.commit: False`) and only commits an offset after a message
  has been fully resolved (processed successfully or sent to the DLQ), so a
  crash mid-retry can't silently lose a message.
- **Randomized data** — the producer generates a random product and price
  for every order rather than requiring manual input.

## Verifying It Works

Check what actually landed in the DLQ:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders-dlq --from-beginning --timeout-ms 5000
```

Check consumer group offsets (confirms committed progress, no reprocessing
after a restart):

```bash
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group order-consumer-group
```

To deliberately force a DLQ event for a demo, temporarily set
`FAILURE_PROBABILITY = 1.0` in `consumer.py`, restart the consumer, and send
a couple of orders — every one will exhaust its retries and land in the DLQ
within a few seconds. Remember to set it back to `0.30` afterward.

## Project Structure

```
kafka-order-system/
├── docker-compose.yml     # Single-node Kafka broker (KRaft mode)
├── requirements.txt       # confluent-kafka, fastavro
├── schemas/
│   └── order.avsc         # Shared Avro schema
├── producer/
│   └── producer.py        # Generates and sends randomized orders
└── consumer/
    └── consumer.py        # Consumes, retries, aggregates, and DLQs orders
```

## Resetting Between Runs

To wipe all topics and consumer group offsets for a clean slate (useful
before a live demo):

```bash
docker-compose down -v
docker-compose up -d
```
