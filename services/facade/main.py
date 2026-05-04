import asyncio
import os
import random

import httpx
from fastapi import FastAPI, Body, HTTPException

from scripts.common import create_client
from services.consul_utils import wait_for_consul, kv_get, register_service, deregister_service, discover_service

app = FastAPI()

service_id = os.getenv("SERVICE_ID", "facade")
_addr = os.getenv("SERVICE_ADDRESS", "facade:8000")
_host, _port = _addr.rsplit(":", 1)

hz_client = None
counter_queue = None


@app.on_event("startup")
async def startup():
    global hz_client, counter_queue

    await wait_for_consul()

    hz_members = await kv_get("config/mq/members")
    if hz_members:
        os.environ["HZ_CLUSTER_MEMBERS"] = hz_members.strip()
        print(f"[facade-service] MQ HZ members from Consul: {hz_members.strip()}")

    queue_name = (await kv_get("config/mq/queue_name") or "counter-queue").strip()
    print(f"[facade-service] Queue name from Consul: {queue_name}")

    hz_client = await asyncio.to_thread(create_client, "facade-service")
    counter_queue = hz_client.get_queue(queue_name).blocking()

    await register_service(service_id, "facade", _host, int(_port))


@app.on_event("shutdown")
async def shutdown():
    await deregister_service(service_id)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/messages")
async def post_message(msg: str = Body(..., embed=True)):
    try:
        counter_queue.offer(msg)
        print(f"[facade-service] Pushed to {counter_queue}: {msg}")
    except Exception as e:
        print(f"[facade-service] Error pushing to queue: {e}")

    try:
        logging_addresses = await discover_service("logging")
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Consul unavailable: {e}")

    if not logging_addresses:
        raise HTTPException(status_code=503, detail="No healthy logging services")

    random.shuffle(logging_addresses)
    log_response = None
    async with httpx.AsyncClient() as client:
        for target in logging_addresses:
            try:
                print(f"[facade-service] Trying to log to {target}")
                r = await client.post(f"http://{target}/log", json={"msg": msg}, timeout=2.0)
                if r.status_code == 200:
                    log_response = r.json()
                    print(f"[facade-service] Logged to {target}")
                    break
            except Exception as e:
                print(f"[facade-service] {target} unavailable: {e}")

    if log_response is None:
        raise HTTPException(status_code=503, detail="All logging services unavailable")

    return {"status": "ok", "logging": log_response}


@app.get("/messages")
async def get_messages():
    try:
        logging_addresses = await discover_service("logging")
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Consul unavailable: {e}")

    if not logging_addresses:
        raise HTTPException(status_code=503, detail="No healthy logging services")

    random.shuffle(logging_addresses)
    logs = None
    async with httpx.AsyncClient() as client:
        for target in logging_addresses:
            try:
                r = await client.get(f"http://{target}/logs", timeout=2.0)
                if r.status_code == 200:
                    logs = r.json()
                    print(f"[facade-service] Got logs from {target}")
                    break
            except Exception as e:
                print(f"[facade-service] {target} unavailable: {e}")

        if logs is None:
            raise HTTPException(status_code=503, detail="All logging services unavailable")

        counter_msgs = []
        try:
            counter_addresses = await discover_service("counter")
            if counter_addresses:
                addr = random.choice(counter_addresses)
                r = await client.get(f"http://{addr}/messages", timeout=2.0)
                if r.status_code == 200:
                    counter_msgs = r.json()
                    print(f"[facade-service] Got counter data from {addr}")
        except Exception as e:
            print(f"[facade-service] Error fetching counter data: {e}")

    return {"logs": logs, "counter_messages": counter_msgs}
