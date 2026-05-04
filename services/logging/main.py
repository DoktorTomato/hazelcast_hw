import asyncio
import os
import uuid

from fastapi import FastAPI, Body

from scripts.common import create_client
from services.consul_utils import wait_for_consul, kv_get, register_service, deregister_service

app = FastAPI()

service_id = os.getenv("SERVICE_ID", "logging-service")
_addr = os.getenv("SERVICE_ADDRESS", f"{service_id}:8000")
_host, _port = _addr.rsplit(":", 1)

hz_client = None
log_map = None


@app.on_event("startup")
async def startup():
    global hz_client, log_map

    await wait_for_consul()

    hz_members = await kv_get("config/hazelcast/members")
    if hz_members:
        os.environ["HZ_CLUSTER_MEMBERS"] = hz_members.strip()
        print(f"[{service_id}] HZ members from Consul: {hz_members.strip()}")

    hz_client = await asyncio.to_thread(create_client, service_id)
    log_map = hz_client.get_map("logging-map").blocking()

    await register_service(service_id, "logging", _host, int(_port))


@app.on_event("shutdown")
async def shutdown():
    await deregister_service(service_id)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/log")
async def log_message(msg: str = Body(..., embed=True)):
    msg_id = str(uuid.uuid4())
    log_map.put(msg_id, msg)
    print(f"[{service_id}] Logged: {msg} (id={msg_id})")
    return {"status": "ok", "id": msg_id, "service": service_id}


@app.get("/logs")
async def get_logs():
    return {str(k): v for k, v in log_map.entry_set()}
