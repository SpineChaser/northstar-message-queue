"""
producer.py
Reads inventory update events from inventory_events.csv and sends each
one as a message to the 'inventory_queue' on RabbitMQ.
"""

import csv
import json
import pika

QUEUE_NAME = "inventory_queue"
EVENTS_FILE = "inventory_events.csv"


def get_connection():
    """Connect to RabbitMQ running on localhost (default port 5672)."""
    return pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))


def send_inventory_update(channel, sku: str, quantity_change: int, reason: str):
    """Publish a single inventory update event to the queue."""
    message = {
        "sku": sku,
        "quantity_change": quantity_change,
        "reason": reason,
    }

    channel.basic_publish(
        exchange="",
        routing_key=QUEUE_NAME,
        body=json.dumps(message),
        properties=pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Persistent,  # survive a broker restart
        ),
    )
    print(f"[sent] {message}")


def load_events(filepath: str):
    """Read inventory events from a CSV file and return them as a list of dicts."""
    events = []
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            events.append({
                "sku": row["sku"],
                "quantity_change": int(row["quantity_change"]),
                "reason": row["reason"],
            })
    return events


def main():
    events = load_events(EVENTS_FILE)
    print(f"Loaded {len(events)} events from {EVENTS_FILE}")

    connection = get_connection()
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    for event in events:
        send_inventory_update(
            channel,
            sku=event["sku"],
            quantity_change=event["quantity_change"],
            reason=event["reason"],
        )

    connection.close()
    print("Done sending messages.")


if __name__ == "__main__":
    main()