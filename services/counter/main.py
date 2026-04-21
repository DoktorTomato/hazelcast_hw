import os
from fastapi import FastAPI, Body
from sqlalchemy import create_engine, Column, String, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

app = FastAPI()

# Database setup
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@db:5432/counter_db")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class Message(Base):
    __tablename__ = "messages"
    id = Column(Integer, primary_key=True, index=True)
    content = Column(String)

# Create tables
Base.metadata.create_all(bind=engine)

@app.post("/count")
async def add_message(msg: str = Body(..., embed=True)):
    db = SessionLocal()
    new_msg = Message(content=msg)
    db.add(new_msg)
    db.commit()
    db.refresh(new_msg)
    db.close()
    print(f"[counter-service] Saved to DB: {msg}")
    return {"status": "ok", "id": new_msg.id}

@app.get("/messages")
async def get_messages():
    db = SessionLocal()
    messages = db.query(Message).all()
    db.close()
    return [m.content for m in messages]
