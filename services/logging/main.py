import asyncio
import os
import uuid

import httpx
from fastapi import FastAPI, Body

from scripts.common import create_client

app = FastAPI()

service_id = os.getenv("SERVICE_ID", "logging-service")
hz_client = create_client(service_id)
log_map = hz_client.get_map("logging-map").blocking()


@app.on_event("startup")
async def startup():
    config_server = os.getenv("CONFIG_SERVER", "config-server:8000")
    service_address = os.getenv("SERVICE_ADDRESS", f"{service_id}:8000")
    for attempt in range(10):
        try:
            async with httpx.AsyncClient() as client:
                await client.post(
                    f"http://{config_server}/register",
                    json={"name": "logging", "address": service_address},
                    timeout=3.0,
                )
            print(f"[{service_id}] Registered with config-server as logging@{service_address}")
            return
        except Exception as e:
            print(f"[{service_id}] Config-server not ready (attempt {attempt + 1}): {e}")
            await asyncio.sleep(2)


@app.post("/log")
async def log_message(msg: str = Body(..., embed=True)):
    msg_id = str(uuid.uuid4())
    log_map.put(msg_id, msg)
    print(f"[{service_id}] Logged message: {msg} (ID: {msg_id})")
    return {"status": "ok", "id": msg_id, "service": service_id}


@app.get("/logs")
async def get_logs():
    print(f"[{service_id}] Fetching all logs")
    logs = log_map.entry_set()
    return {str(k): v for k, v in logs}
