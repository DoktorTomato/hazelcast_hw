import os
import random
import httpx
from fastapi import FastAPI, Body, HTTPException

app = FastAPI()

# Configuration from environment
logging_services = os.getenv("LOGGING_SERVICES", "logging1:8000,logging2:8000,logging3:8000").split(",")
counter_service = os.getenv("COUNTER_SERVICE", "counter:8000")

@app.post("/messages")
async def post_message(msg: str = Body(..., embed=True)):
    # 1. Randomly pick a logging service and try to log
    # Failover logic: shuffle and try until success
    targets = list(logging_services)
    random.shuffle(targets)
    
    log_success = False
    log_response = None
    
    async with httpx.AsyncClient() as client:
        # Log to one of the logging instances (randomly selected with failover)
        for target in targets:
            try:
                print(f"[facade-service] Attempting to log to {target}")
                r = await client.post(f"http://{target}/log", json={"msg": msg}, timeout=2.0)
                if r.status_code == 200:
                    log_response = r.json()
                    log_success = True
                    print(f"[facade-service] Successfully logged to {target}")
                    break
            except Exception as e:
                print(f"[facade-service] Error logging to {target}: {e}")
        
        if not log_success:
            raise HTTPException(status_code=503, detail="All logging services are unavailable")
        
        # 2. Also send to counter-service
        try:
            await client.post(f"http://{counter_service}/count", json={"msg": msg}, timeout=2.0)
            print(f"[facade-service] Successfully sent to counter-service")
        except Exception as e:
            print(f"[facade-service] Error sending to counter-service: {e}")
            
    return {"status": "ok", "logging": log_response}

@app.get("/messages")
async def get_messages():
    # 1. Get logs from a random logging service (with failover)
    targets = list(logging_services)
    random.shuffle(targets)
    
    logs = None
    async with httpx.AsyncClient() as client:
        for target in targets:
            try:
                r = await client.get(f"http://{target}/logs", timeout=2.0)
                if r.status_code == 200:
                    logs = r.json()
                    print(f"[facade-service] Successfully fetched logs from {target}")
                    break
            except Exception as e:
                print(f"[facade-service] Error fetching logs from {target}: {e}")
        
        if logs is None:
            raise HTTPException(status_code=503, detail="All logging services are unavailable")
            
        # 2. Get counter values
        counter_msgs = []
        try:
            r = await client.get(f"http://{counter_service}/messages", timeout=2.0)
            if r.status_code == 200:
                counter_msgs = r.json()
                print(f"[facade-service] Successfully fetched from counter-service")
        except Exception as e:
            print(f"[facade-service] Error fetching from counter-service: {e}")
            
    return {
        "logs": logs,
        "counter_messages": counter_msgs
    }
