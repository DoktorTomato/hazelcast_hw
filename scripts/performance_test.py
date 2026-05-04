"""
Performance test for Task 5 — mirrors Task 1 scenarios.

Scenario 1 ("10 accounts"): 10 concurrent clients, each sends N unique messages.
Scenario 2 ("1 account"):   1 client sends N messages sequentially.
"""

import asyncio
import time

import httpx

FACADE_URL = "http://127.0.0.1:8000"
REQUESTS_PER_CLIENT = 1000


async def _send(client_id: int, num: int, prefix: str, client: httpx.AsyncClient):
    for i in range(num):
        try:
            await client.post(
                f"{FACADE_URL}/messages",
                json={"msg": f"{prefix}-{i}"},
                timeout=60.0,
            )
        except Exception as e:
            print(f"[client {client_id}] request {i} failed: {e}")


async def run_scenario(name: str, num_clients: int, requests_per_client: int):
    print(f"\n--- {name} ---")
    total = num_clients * requests_per_client

    limits = httpx.Limits(max_connections=100, max_keepalive_connections=50)
    async with httpx.AsyncClient(limits=limits) as client:
        start = time.perf_counter()
        await asyncio.gather(*[
            _send(i, requests_per_client, f"client{i+1}", client)
            for i in range(num_clients)
        ])
        elapsed = time.perf_counter() - start

    rps = total / elapsed if elapsed > 0 else 0
    print(f"Total requests : {total}")
    print(f"Total time     : {elapsed:.2f} s")
    print(f"RPS            : {rps:.2f}")
    return elapsed, rps


async def main():
    print("=" * 50)
    print("Task 5 Performance Test")
    print("=" * 50)

    await run_scenario(
        "Scenario 1: 10 clients, unique messages (10 accounts)",
        num_clients=10,
        requests_per_client=REQUESTS_PER_CLIENT,
    )

    await run_scenario(
        "Scenario 2: 1 client, sequential messages (1 account)",
        num_clients=1,
        requests_per_client=REQUESTS_PER_CLIENT,
    )


if __name__ == "__main__":
    asyncio.run(main())
