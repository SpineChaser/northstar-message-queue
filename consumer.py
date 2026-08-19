"""
consumer.py
Listens on 'inventory_queue' and processes incoming inventory update messages.
Run this in its own terminal window - it will keep running until you press Ctrl+C.
"""

import json
import pika

QUEUE_NAME = "inventory_queue"

# In-memory stand-in for a real inventory database/table
inventory = {}


def get_connection():
    return pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))


def process_message(sku: str, quantity_change: int, reason: str):
    """Apply the update to our in-memory inventory and print the result."""
    current = inventory.get(sku, 0)
    new_total = current + quantity_change
    inventory[sku] = new_total

    print(f"[processed] {sku}: {current} -> {new_total}  (change: {quantity_change:+d}, reason: {reason})")


def callback(ch, method, properties, body):
    try:
        data = json.loads(body)
        process_message(
            sku=data["sku"],
            quantity_change=data["quantity_change"],
            reason=data.get("reason", "unspecified"),
        )
        # Tell RabbitMQ we successfully handled this message
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"[error] Failed to process message: {e}")
        # Reject and don't requeue a malformed message, so it doesn't loop forever
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def main():
    connection = get_connection()
    channel = connection.channel()

    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    # Process one message at a time, don't let RabbitMQ flood the consumer
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)

    print("Waiting for inventory messages. Press Ctrl+C to stop.")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\nStopping consumer...")
        channel.stop_consuming()
        connection.close()


if __name__ == "__main__":
    main()