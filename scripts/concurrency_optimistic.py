"""Three concurrent clients increment one map key with optimistic locking (CAS).

Uses replace_if_same(key, expected_old_value, new_value) retry loop.
"""

import threading

from common import create_client, timed

MAP_NAME = "counter-map"
COUNTER_KEY = "counter"
CLIENTS = 3
INCREMENTS_PER_CLIENT = 10000


def worker(worker_id: int) -> None:
    client = create_client(f"optimistic-worker-{worker_id}")
    retries = 0
    try:
        distributed_map = client.get_map(MAP_NAME).blocking()

        for _ in range(INCREMENTS_PER_CLIENT):
            while True:
                current = distributed_map.get(COUNTER_KEY)
                updated = current + 1
                if distributed_map.replace_if_same(COUNTER_KEY, current, updated):
                    break
                retries += 1

        print(f"[optimistic-worker-{worker_id}] CAS retries: {retries}")
    finally:
        client.shutdown()


def main() -> None:
    initializer = create_client("optimistic-initializer")
    try:
        distributed_map = initializer.get_map(MAP_NAME).blocking()
        distributed_map.set(COUNTER_KEY, 0)
        print(
            f"Initialized {MAP_NAME}[{COUNTER_KEY}] = 0. Running {CLIENTS} clients x {INCREMENTS_PER_CLIENT} increments"
        )

        threads = [threading.Thread(target=worker, args=(idx,)) for idx in range(1, CLIENTS + 1)]

        with timed("Optimistic-lock execution"):
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

        expected = CLIENTS * INCREMENTS_PER_CLIENT
        final_value = distributed_map.get(COUNTER_KEY)
        print(f"Expected final value: {expected}")
        print(f"Actual final value:   {final_value}")
        print(f"Data consistency OK:  {final_value == expected}")
    finally:
        initializer.shutdown()


if __name__ == "__main__":
    main()
