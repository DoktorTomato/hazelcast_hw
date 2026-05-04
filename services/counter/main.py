import asyncio
import os
import threading
import time

from fastapi import FastAPI
from sqlalchemy import create_engine, Column, String, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from scripts.common import create_client
from services.consul_utils import wait_for_consul, kv_get, register_service, deregister_service

app = FastAPI()

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@db:5432/counter_db")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class Message(Base):
    __tablename__ = "messages"
    id = Column(Integer, primary_key=True, index=True)
    content = Column(String)


Base.metadata.create_all(bind=engine)

service_id = os.getenv("SERVICE_ID", "counter-service")
_addr = os.getenv("SERVICE_ADDRESS", "counter:8000")
_host, _port = _addr.rsplit(":", 1)

# Consumer thread waits on this event until hz_client is ready
_consumer_ready = threading.Event()
_consumer_state: dict = {}  # holds hz_client and queue_name once set


def _consume_loop():
    _consumer_ready.wait()
    hz_client = _consumer_state["hz_client"]
    queue_name = _consumer_state["queue_name"]
    counter_queue = hz_client.get_queue(queue_name).blocking()
    print(f"[{service_id}] Queue consumer started (queue={queue_name})")
    while True:
        try:
            msg = counter_queue.take()
            db = SessionLocal()
            db.add(Message(content=msg))
            db.commit()
            db.close()
            print(f"[{service_id}] Consumed and saved: {msg}")
        except Exception as e:
            print(f"[{service_id}] Consumer error: {e}")
            time.sleep(1)


threading.Thread(target=_consume_loop, daemon=True).start()


@app.on_event("startup")
async def startup():
    await wait_for_consul()

    hz_members = await kv_get("config/mq/members")
    if hz_members:
        os.environ["HZ_CLUSTER_MEMBERS"] = hz_members.strip()
        print(f"[{service_id}] MQ HZ members from Consul: {hz_members.strip()}")

    queue_name = (await kv_get("config/mq/queue_name") or "counter-queue").strip()
    print(f"[{service_id}] Queue name from Consul: {queue_name}")

    hz_client = await asyncio.to_thread(create_client, service_id)
    _consumer_state["hz_client"] = hz_client
    _consumer_state["queue_name"] = queue_name
    _consumer_ready.set()

    await register_service(service_id, "counter", _host, int(_port))


@app.on_event("shutdown")
async def shutdown():
    await deregister_service(service_id)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/messages")
async def get_messages():
    db = SessionLocal()
    messages = db.query(Message).all()
    db.close()
    return [m.content for m in messages]
