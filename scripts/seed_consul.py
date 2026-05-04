import os
import sys
import time

import httpx

CONSUL_HOST = os.getenv("CONSUL_HOST", "consul:8500")
BASE = f"http://{CONSUL_HOST}/v1"

KV_DATA = {
    "config/hazelcast/members": "hazelcast1:5701,hazelcast2:5701,hazelcast3:5701",
    "config/mq/members":        "hazelcast1:5701,hazelcast2:5701,hazelcast3:5701",
    "config/mq/queue_name":     "counter-queue",
}


def wait_for_consul():
    for i in range(30):
        try:
            r = httpx.get(f"{BASE}/status/leader", timeout=2.0)
            if r.status_code == 200 and r.text.strip().strip('"'):
                print(f"[seed] Consul ready (attempt {i + 1})")
                return
        except Exception:
            pass
        print(f"[seed] Waiting for Consul ({i + 1}/30)...")
        time.sleep(2)
    print("[seed] Consul not available — giving up")
    sys.exit(1)


def seed():
    wait_for_consul()
    for key, value in KV_DATA.items():
        r = httpx.put(f"{BASE}/kv/{key}", content=value.encode(), timeout=3.0)
        r.raise_for_status()
        print(f"[seed] {key} = {value}")
    print("[seed] Done.")


if __name__ == "__main__":
    seed()
