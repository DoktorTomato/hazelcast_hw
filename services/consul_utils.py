import asyncio
import os

import httpx


def _base() -> str:
    host = os.getenv("CONSUL_HOST", "consul:8500")
    return f"http://{host}/v1"


async def wait_for_consul(max_attempts: int = 30):
    for i in range(max_attempts):
        try:
            async with httpx.AsyncClient() as client:
                r = await client.get(f"{_base()}/status/leader", timeout=2.0)
                if r.status_code == 200 and r.text.strip().strip('"'):
                    print(f"[consul] Ready (attempt {i + 1})")
                    return
        except Exception:
            pass
        print(f"[consul] Waiting... ({i + 1}/{max_attempts})")
        await asyncio.sleep(2)
    raise RuntimeError("Consul not available after waiting")


async def kv_get(key: str) -> str | None:
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{_base()}/kv/{key}?raw", timeout=3.0)
        if r.status_code == 200:
            return r.text
        return None


async def kv_put(key: str, value: str):
    async with httpx.AsyncClient() as client:
        r = await client.put(f"{_base()}/kv/{key}", content=value.encode(), timeout=3.0)
        r.raise_for_status()


async def register_service(service_id: str, service_name: str, address: str, port: int):
    payload = {
        "ID": service_id,
        "Name": service_name,
        "Address": address,
        "Port": port,
        "Check": {
            "HTTP": f"http://{address}:{port}/health",
            "Interval": "10s",
            "Timeout": "3s",
            "DeregisterCriticalServiceAfter": "30s",
        },
    }
    async with httpx.AsyncClient() as client:
        r = await client.put(f"{_base()}/agent/service/register", json=payload, timeout=3.0)
        r.raise_for_status()
    print(f"[consul] Registered {service_name}/{service_id} at {address}:{port}")


async def deregister_service(service_id: str):
    async with httpx.AsyncClient() as client:
        await client.put(f"{_base()}/agent/service/deregister/{service_id}", timeout=3.0)
    print(f"[consul] Deregistered {service_id}")


async def discover_service(service_name: str) -> list[str]:
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{_base()}/health/service/{service_name}?passing", timeout=3.0)
        r.raise_for_status()
        return [
            f"{e['Service']['Address']}:{e['Service']['Port']}"
            for e in r.json()
        ]
