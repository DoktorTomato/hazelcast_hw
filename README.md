# Hazelcast Python Lab

Python scripts for Hazelcast 5.6.0 distributed data structure experiments.

## Prerequisites

- Python 3.10+
- Docker + Docker Compose
- Hazelcast cluster from this repository

## Setup

1. Start the Hazelcast cluster:
   - `docker compose up -d`
2. Verify containers:
   - `docker compose ps`
3. Create and activate virtual environment (PowerShell):
   - `python -m venv .venv`
   - `.\.venv\Scripts\Activate.ps1`
4. Ensure pip is available:
   - `python -m pip --version`
   - if missing: `python -m ensurepip --upgrade`
5. Install dependencies:
   - `python -m pip install -r requirements.txt`

## Configuration

Default client settings are:
- cluster name: `dev`
- members: `127.0.0.1:5701,127.0.0.1:5702,127.0.0.1:5703`

Override with environment variables if needed:
- `HZ_CLUSTER_NAME`
- `HZ_CLUSTER_MEMBERS`

## Run Scripts

### 1) Distributed Map Basics

- `python .\scripts\map_basics.py`

- Check missing keys after node shutdown:
  - One-shot check: `python .\scripts\map_check_after_shutdown.py --map-name lab-map --start-key 0 --expected-count 1000`
  - Seed + watch mode: `python .\scripts\map_check_after_shutdown.py --map-name lab-map --start-key 0 --expected-count 1000 --seed --watch-interval 2`

### 2) Concurrency Control

- No lock (race condition):
  - `python .\scripts\concurrency_no_lock.py`
- Pessimistic lock (`lock` / `unlock`):
  - `python .\scripts\concurrency_pessimistic.py`
- Optimistic lock (CAS with `replace_if_same`):
  - `python .\scripts\concurrency_optimistic.py`

### 3) Bounded Queue

- Queue name: `bounded-q`
- Capacity: `10` (configured in `hazelcast.yaml`)

- Blocking behavior demo (no active consumers):
  - `python .\scripts\queue_blocking_demo.py`

- Producer + 2 consumers (run in 3 terminals):
  - Terminal A: `python .\scripts\queue_consumer.py --id 1`
  - Terminal B: `python .\scripts\queue_consumer.py --id 2`
  - Terminal C: `python .\scripts\queue_producer.py --clear`

Notes:
- Each queue item is consumed by exactly one consumer (work-queue pattern).
- With two consumers, values `1..100` are split between them in non-deterministic order.
- If there are no readers and queue is full, `put()` blocks until a consumer removes an item.

## Suggested Run Order

1. `python .\scripts\map_basics.py`
2. `python .\scripts\concurrency_no_lock.py`
3. `python .\scripts\concurrency_pessimistic.py`
4. `python .\scripts\concurrency_optimistic.py`
5. `python .\scripts\queue_blocking_demo.py`
6. Start 2 consumers, then run producer

## Troubleshooting

- `ModuleNotFoundError: hazelcast`
  - `python -m pip install -r requirements.txt`
- Client cannot connect
  - check `docker compose ps`
  - verify ports `5701`, `5702`, `5703`
  - verify `HZ_CLUSTER_NAME` / `HZ_CLUSTER_MEMBERS`
