import os
import random

import httpx
from fastapi import FastAPI, Body, HTTPException

from scripts.common import create_client

app = FastAPI()

config_server = os.getenv("CONFIG_SERVER", "config-server:8000")

hz_client = create_client("facade-service")
counter_queue = hz_client.get_queue("counter-queue").blocking()


async def _get_addresses(service_name: str) -> list[str]:
    async with httpx.AsyncClient() as client:
        r = await client.get(f"http://{config_server}/services/{service_name}", timeout=3.0)
        r.raise_for_status()
        return r.json()


@app.post("/messages")
async def post_message(msg: str = Body(..., embed=True)):
    # Push to Hazelcast queue asynchronously — counter-service consumes from it
    try:
        counter_queue.offer(msg)
        print(f"[facade-service] Pushed to counter-queue: {msg}")
    except Exception as e:
        print(f"[facade-service] Error pushing to counter-queue: {e}")

    # Log to one randomly chosen logging instance (discovered via config-server)
    try:
        logging_addresses = await _get_addresses("logging")
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Config-server unavailable: {e}")

    if not logging_addresses:
        raise HTTPException(status_code=503, detail="No logging services registered")

    targets = list(logging_addresses)
    random.shuffle(targets)

    log_response = None
    async with httpx.AsyncClient() as client:
        for target in targets:
            try:
                print(f"[facade-service] Attempting to log to {target}")
                r = await client.post(f"http://{target}/log", json={"msg": msg}, timeout=2.0)
                if r.status_code == 200:
                    log_response = r.json()
                    print(f"[facade-service] Successfully logged to {target}")
                    break
            except Exception as e:
                print(f"[facade-service] Error logging to {target}: {e}")

    if log_response is None:
        raise HTTPException(status_code=503, detail="All logging services are unavailable")

    return {"status": "ok", "logging": log_response}


@app.get("/messages")
async def get_messages():
    # Fetch logs from a random logging instance
    try:
        logging_addresses = await _get_addresses("logging")
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Config-server unavailable: {e}")

    if not logging_addresses:
        raise HTTPException(status_code=503, detail="No logging services registered")

    targets = list(logging_addresses)
    random.shuffle(targets)

    logs = None
    async with httpx.AsyncClient() as client:
        for target in targets:
            try:
                r = await client.get(f"http://{target}/logs", timeout=2.0)
                if r.status_code == 200:
                    logs = r.json()
                    print(f"[facade-service] Fetched logs from {target}")
                    break
            except Exception as e:
                print(f"[facade-service] Error fetching logs from {target}: {e}")

        if logs is None:
            raise HTTPException(status_code=503, detail="All logging services are unavailable")

        # Fetch counter data via HTTP GET (unchanged from lab3)
        counter_msgs = []
        try:
            counter_addresses = await _get_addresses("counter")
            if counter_addresses:
                addr = random.choice(counter_addresses)
                r = await client.get(f"http://{addr}/messages", timeout=2.0)
                if r.status_code == 200:
                    counter_msgs = r.json()
                    print(f"[facade-service] Fetched counter data from {addr}")
        except Exception as e:
            print(f"[facade-service] Error fetching counter data: {e}")

    return {"logs": logs, "counter_messages": counter_msgs}
