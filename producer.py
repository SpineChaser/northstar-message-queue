"""
producer.py
Reads inventory update events from inventory_events.csv and sends each
one as a message to the 'inventory_queue' on RabbitMQ.
"""

import csv
import json
import sys
import pika

QUEUE_NAME = "inventory_queue"
EVENTS_FILE = "inventory_events.csv"
REQUIRED_COLUMNS = {"sku", "quantity_change", "reason"}


def get_connection():
    """Connect to RabbitMQ running on localhost (default port 5672)."""
    try:
        return pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    except pika.exceptions.AMQPConnectionError:
        print("[error] Could not connect to RabbitMQ on localhost.")
        print("        Make sure the RabbitMQ service is running, then try again.")
        sys.exit(1)


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
    """
    Read inventory events from a CSV file and return them as a list of dicts.
    Skips rows that are missing data or have an invalid quantity_change,
    printing a warning for each one instead of crashing.
    """
    try:
        f = open(filepath, newline="", encoding="utf-8")
    except FileNotFoundError:
        print(f"[error] Could not find '{filepath}'.")
        print(f"        Make sure the file exists in this folder before running the producer.")
        sys.exit(1)

    events = []
    with f:
        reader = csv.DictReader(f)

        if reader.fieldnames is None or not REQUIRED_COLUMNS.issubset(set(reader.fieldnames)):
            print(f"[error] '{filepath}' is missing required columns: {sorted(REQUIRED_COLUMNS)}")
            print(f"        Found columns: {reader.fieldnames}")
            sys.exit(1)

        for line_num, row in enumerate(reader, start=2):  # start=2: header is line 1
            sku = row.get("sku", "").strip()
            reason = row.get("reason", "").strip()
            raw_qty = row.get("quantity_change", "").strip()

            if not sku:
                print(f"[skipped] Line {line_num}: missing sku")
                continue

            try:
                quantity_change = int(raw_qty)
            except ValueError:
                print(f"[skipped] Line {line_num}: quantity_change '{raw_qty}' is not a valid whole number")
                continue

            events.append({
                "sku": sku,
                "quantity_change": quantity_change,
                "reason": reason or "unspecified",
            })

    return events


def main():
    events = load_events(EVENTS_FILE)

    if not events:
        print(f"[error] No valid events found in '{EVENTS_FILE}'. Nothing to send.")
        sys.exit(1)

    print(f"Loaded {len(events)} valid event(s) from {EVENTS_FILE}")

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