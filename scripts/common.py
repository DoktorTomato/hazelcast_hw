import os
import time
from contextlib import contextmanager

import hazelcast


DEFAULT_MEMBERS = [
    "127.0.0.1:5701",
    "127.0.0.1:5702",
    "127.0.0.1:5703",
]


def get_cluster_members() -> list[str]:
    raw = os.getenv("HZ_CLUSTER_MEMBERS")
    if not raw:
        return DEFAULT_MEMBERS

    members = [entry.strip() for entry in raw.split(",") if entry.strip()]
    return members or DEFAULT_MEMBERS


def create_client(client_name: str) -> hazelcast.HazelcastClient:
    cluster_name = os.getenv("HZ_CLUSTER_NAME", "dev")

    print(
        f"[{client_name}] Connecting to cluster '{cluster_name}' via {get_cluster_members()}"
    )

    client = hazelcast.HazelcastClient(
        cluster_name=cluster_name,
        cluster_members=get_cluster_members(),
        client_name=client_name,
        smart_routing=False,
        async_start=False,
        reconnect_mode="ON",
        retry_initial_backoff=1.0,
        retry_max_backoff=8.0,
        retry_multiplier=2.0,
        retry_jitter=0.2,
        cluster_connect_timeout=30.0,
    )

    print(f"[{client_name}] Connected.")
    return client


@contextmanager
def timed(label: str):
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed = time.perf_counter() - start
        print(f"{label} took {elapsed:.4f}s")
