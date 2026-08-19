"""
producer.py
Sends inventory update messages to the 'inventory_queue' on RabbitMQ.
"""

import json
import pika

QUEUE_NAME = "inventory_queue"


def get_connection():
    """Connect to RabbitMQ running on localhost (default port 5672)."""
    return pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))


def send_inventory_update(channel, sku: str, quantity_change: int, reason: str = "manual_update"):
    """
    Publish a single inventory update event to the queue.

    sku: product identifier, e.g. "SKU-1001"
    quantity_change: positive to add stock, negative to remove stock
    reason: short tag describing why the change happened
    """
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


def main():
    connection = get_connection()
    channel = connection.channel()

    # Make sure the queue exists (safe to call even if it already does)
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    # Send a few sample inventory events
    send_inventory_update(channel, sku="SKU-1001", quantity_change=-5, reason="order_fulfilled")
    send_inventory_update(channel, sku="SKU-1002", quantity_change=100, reason="restock")
    send_inventory_update(channel, sku="SKU-1001", quantity_change=-2, reason="order_fulfilled")

    connection.close()
    print("Done sending messages.")


if __name__ == "__main__":
    main()