"""
subscribe_orders.py
Listens continuously for ORDER_RECEIVED events and prints each one
as it arrives. This simulates what the Inventory Service will do
later when it reacts to real order events.

Run this FIRST, then run publish_order.py in another terminal.
Stop this script with Ctrl+C.
"""

import pika
import json
import os
from dotenv import load_dotenv

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

connection = pika.BlockingConnection(connection_params)
channel = connection.channel()

EXCHANGE_NAME = 'flowengine.events'

# Must declare the exchange here too. Pub/sub has no guaranteed
# startup order — if the subscriber starts before the publisher
# has ever run, the exchange wouldn't exist yet without this line.
channel.exchange_declare(
    exchange=EXCHANGE_NAME,
    exchange_type='topic',
    durable=True
)

# ── Declare a queue for THIS subscriber ───────────────────────────
# queue='' with exclusive=True means: "give me a uniquely-named,
# auto-generated queue that only I can see, and delete it when I
# disconnect." This is correct for a TEST subscriber — you don't
# want old test queues piling up in RabbitMQ.
# A REAL microservice would instead use a fixed, named queue (e.g.
# "inventory_service_queue") so missed events wait for it even if
# it's temporarily offline — that's the durability guarantee from
# the Event Sourcing pattern in your literature review.
result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue

# ── Bind the queue to the exchange ────────────────────────────────
# This is the actual subscription step: "send anything published
# with routing_key 'ORDER_RECEIVED' into MY queue."
ROUTING_KEY = 'ORDER_RECEIVED'
channel.queue_bind(
    exchange=EXCHANGE_NAME,
    queue=queue_name,
    routing_key=ROUTING_KEY
)

print(f"[*] Waiting for ORDER_RECEIVED events. Press Ctrl+C to exit.")

# ── Define what happens when a message arrives ───────────────────
# RabbitMQ calls this function automatically for every matching
# message. ch/method/properties are metadata pika needs; body is
# the actual message bytes we sent from the publisher.
def callback(ch, method, properties, body):
    event = json.loads(body)  # turn JSON bytes back into a dict
    print("\n[x] Received event:")
    print(json.dumps(event, indent=2))

    # Acknowledge the message — tells RabbitMQ "I successfully
    # processed this, you can delete it from the queue now."
    # Without this, a crashed consumer would cause RabbitMQ to
    # think the message is still unprocessed and redeliver it.
    ch.basic_ack(delivery_tag=method.delivery_tag)

# ── Register the callback and start listening ─────────────────────
# auto_ack=False because we're manually ack-ing inside callback —
# this is the safer pattern: only acknowledge after processing
# actually succeeds.
channel.basic_consume(
    queue=queue_name,
    on_message_callback=callback,
    auto_ack=False
)

# This blocks forever, waiting for messages, until you Ctrl+C.
try:
    channel.start_consuming()
except KeyboardInterrupt:
    print("\n[*] Shutting down subscriber.")
    connection.close()