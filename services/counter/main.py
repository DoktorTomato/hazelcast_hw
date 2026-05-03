import asyncio
import os
import threading
import time

import httpx
from fastapi import FastAPI
from sqlalchemy import create_engine, Column, String, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from scripts.common import create_client

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

hz_client = create_client("counter-service")
counter_queue = hz_client.get_queue("counter-queue").blocking()


def _consume_loop():
    print("[counter-service] Queue consumer started")
    while True:
        try:
            msg = counter_queue.take()  # blocks until a message arrives
            db = SessionLocal()
            db.add(Message(content=msg))
            db.commit()
            db.close()
            print(f"[counter-service] Consumed from queue and saved: {msg}")
        except Exception as e:
            print(f"[counter-service] Consumer error: {e}")
            time.sleep(1)


threading.Thread(target=_consume_loop, daemon=True).start()


@app.on_event("startup")
async def startup():
    config_server = os.getenv("CONFIG_SERVER", "config-server:8000")
    service_address = os.getenv("SERVICE_ADDRESS", "counter:8000")
    for attempt in range(10):
        try:
            async with httpx.AsyncClient() as client:
                await client.post(
                    f"http://{config_server}/register",
                    json={"name": "counter", "address": service_address},
                    timeout=3.0,
                )
            print(f"[counter-service] Registered with config-server as counter@{service_address}")
            return
        except Exception as e:
            print(f"[counter-service] Config-server not ready (attempt {attempt + 1}): {e}")
            await asyncio.sleep(2)


@app.get("/messages")
async def get_messages():
    db = SessionLocal()
    messages = db.query(Message).all()
    db.close()
    return [m.content for m in messages]
