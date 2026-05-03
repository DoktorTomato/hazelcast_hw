import os
from collections import defaultdict
from fastapi import FastAPI, Body

app = FastAPI()

_registry: dict[str, list[str]] = defaultdict(list)


@app.post("/register")
async def register(name: str = Body(...), address: str = Body(...)):
    if address not in _registry[name]:
        _registry[name].append(address)
    print(f"[config-server] Registered {name} at {address} (total: {_registry[name]})")
    return {"status": "ok", "name": name, "address": address}


@app.get("/services/{name}")
async def get_services(name: str):
    addresses = _registry.get(name, [])
    print(f"[config-server] Lookup '{name}' -> {addresses}")
    return addresses


@app.get("/services")
async def get_all_services():
    return dict(_registry)
