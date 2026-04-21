import os
import uuid
from fastapi import FastAPI, Body
from scripts.common import create_client

app = FastAPI()

# Initialize Hazelcast client
service_id = os.getenv("SERVICE_ID", "logging-service")
hz_client = create_client(service_id)
log_map = hz_client.get_map("logging-map").blocking()

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
    # Convert map entries to a list of dicts or just a list of values
    return {str(k): v for k, v in logs}
