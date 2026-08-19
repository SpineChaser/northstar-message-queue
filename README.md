# Northstar Message Queue Prototype

## Project Overview

This project is a mini-prototype developed as part of the Meridian Pivot
industry working simulation for Northstar Retail Co.

The prototype demonstrates the basic use of a message queue in an
inventory-management scenario.

## Objective

The objective is to demonstrate how inventory updates can be sent from
a producer to a message queue and processed by a consumer.

## Architecture

Producer → Message Queue → Consumer → Inventory System

## Technology

- Python
- RabbitMQ
- Git
- GitHub

## Current Status

Project setup completed.

Message queue implementation in progress.

## Inventory Queue Prototype

A small producer/consumer prototype using RabbitMQ to process inventory update events.

### How it works
- `producer.py` reads inventory events from `inventory_events.csv` and publishes each one as a message to the `inventory_queue` on RabbitMQ.
- `consumer.py` listens on that queue, applies each update to a running inventory total, and persists the current state to `inventory_state.json`.

### Prerequisites
- Python 3
- RabbitMQ installed and running locally (default port 5672)

### Setup
```bash
pip install -r requirements.txt
```

### Running it
Open two terminal windows in the project folder.

**Terminal 1 - start the consumer:**
```bash
py consumer.py
```

**Terminal 2 - run the producer:**
```bash
py producer.py
```

The producer sends events from `inventory_events.csv`. The consumer processes them and writes the running totals to `inventory_state.json`.

### Editing events
To send different inventory events, edit `inventory_events.csv`. Each row needs `sku`, `quantity_change`, and `reason` columns.