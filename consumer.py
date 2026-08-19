"""
consumer.py
Listens on 'inventory_queue', processes incoming inventory update messages,
and persists the running inventory totals to inventory_state.json after
every update.
Run this in its own terminal window - it will keep running until you press Ctrl+C.
"""

import json
import os
import pika

QUEUE_NAME = "inventory_queue"
STATE_FILE = "inventory_state.json"

# In-memory inventory, loaded from disk on startup if a state file already exists
inventory = {}


def load_state():
    """Load existing inventory totals from disk, if the file exists."""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_state():
    """Write the current inventory totals to disk."""
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(inventory, f, indent=2)


def get_connection():
    return pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))


def process_message(sku: str, quantity_change: int, reason: str):
    """Apply the update to inventory, print the result, and persist to disk."""
    current = inventory.get(sku, 0)
    new_total = current + quantity_change
    inventory[sku] = new_total

    print(f"[processed] {sku}: {current} -> {new_total}  (change: {quantity_change:+d}, reason: {reason})")

    save_state()


def callback(ch, method, properties, body):
    try:
        data = json.loads(body)
        process_message(
            sku=data["sku"],
            quantity_change=data["quantity_change"],
            reason=data.get("reason", "unspecified"),
        )
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"[error] Failed to process message: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def main():
    global inventory
    inventory = load_state()
    if inventory:
        print(f"Loaded existing state from {STATE_FILE}: {inventory}")

    connection = get_connection()
    channel = connection.channel()

    channel.queue_declare(queue=QUEUE_NAME, durable=True)
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