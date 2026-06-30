"""
publish_order.py
Publishes ONE fake ORDER_RECEIVED event to RabbitMQ, then exits.
This simulates what the Input Processing Service will do later,
except the payload here is hand-written instead of OCR/NLP output.
"""

import pika
import json
import sys
import os
from datetime import datetime, timezone
from dotenv import load_dotenv

# ── 1. Connection parameters ──────────────────────────────────────
# Loads values from a local .env file (never committed to git) instead
# of hardcoding credentials in the script itself.
load_dotenv()

credentials = pika.PlainCredentials(
    os.getenv('RABBITMQ_USER', 'flowengine'),
    os.getenv('RABBITMQ_PASS', 'flowengine_dev')
)
connection_params = pika.ConnectionParameters(
    host=os.getenv('RABBITMQ_HOST', 'localhost'),
    port=int(os.getenv('RABBITMQ_PORT', 5672)),
    credentials=credentials
)

# ── 2. Open the connection and a "channel" ────────────────────────
# A connection is the TCP link to RabbitMQ.
# A channel is a lightweight virtual connection inside it — almost
# all actual work (declaring exchanges, publishing) happens on a
# channel, not the raw connection. You open one channel per script.
connection = pika.BlockingConnection(connection_params)
channel = connection.channel()

# ── 3. Declare the exchange ────────────────────────────────────────
# "Declare" means: create this exchange if it doesn't exist yet,
# or confirm it matches if it does. It's idempotent — safe to run
# every time the script starts.
# exchange_type='topic' lets us route using flexible keys like
# "order.received" instead of needing an exact string match.
EXCHANGE_NAME = 'flowengine.events'
channel.exchange_declare(
    exchange=EXCHANGE_NAME,
    exchange_type='topic',
    durable=True   # survives a RabbitMQ restart — events aren't lost
)

# ── 4. Build the fake event payload ───────────────────────────────
# This mimics the real ORDER_RECEIVED event schema your report
# defines: customer_id, items[], source, confidence_score.
event = {
    "event_type": "ORDER_RECEIVED",
    "timestamp": datetime.now(timezone.utc).isoformat(),
    "source": "test_script",
    "customer_id": "CUST_00123",
    "items": [
        {"product_name": "rice", "quantity": 50, "unit": "kg", "price": 42.0}
    ],
    "confidence_score": 0.97
}

# Convert the Python dictionary to a JSON string — messages on the
# wire are just bytes, so we serialize the structured data first.
message_body = json.dumps(event)

# ── 5. Publish the message ────────────────────────────────────────
# routing_key tells the exchange "label this message ORDER_RECEIVED."
# Any queue bound with a matching pattern will receive a copy.
ROUTING_KEY = 'ORDER_RECEIVED'

channel.basic_publish(
    exchange=EXCHANGE_NAME,
    routing_key=ROUTING_KEY,
    body=message_body,
    properties=pika.BasicProperties(
        content_type='application/json',
        delivery_mode=2  # makes the message persistent (saved to disk)
    )
)

print(f"[x] Published ORDER_RECEIVED event:")
print(json.dumps(event, indent=2))

# ── 6. Clean up ────────────────────────────────────────────────────
connection.close()