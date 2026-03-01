"""Three concurrent clients increment one map key with pessimistic locking."""

import threading

from common import create_client, timed

MAP_NAME = "counter-map"
COUNTER_KEY = "counter"
CLIENTS = 3
INCREMENTS_PER_CLIENT = 10000


def worker(worker_id: int) -> None:
    client = create_client(f"pessimistic-worker-{worker_id}")
    try:
        distributed_map = client.get_map(MAP_NAME).blocking()
        for _ in range(INCREMENTS_PER_CLIENT):
            distributed_map.lock(COUNTER_KEY)
            try:
                current = distributed_map.get(COUNTER_KEY)
                distributed_map.set(COUNTER_KEY, current + 1)
            finally:
                distributed_map.unlock(COUNTER_KEY)
    finally:
        client.shutdown()


def main() -> None:
    initializer = create_client("pessimistic-initializer")
    try:
        distributed_map = initializer.get_map(MAP_NAME).blocking()
        distributed_map.set(COUNTER_KEY, 0)
        print(
            f"Initialized {MAP_NAME}[{COUNTER_KEY}] = 0. Running {CLIENTS} clients x {INCREMENTS_PER_CLIENT} increments"
        )

        threads = [threading.Thread(target=worker, args=(idx,)) for idx in range(1, CLIENTS + 1)]

        with timed("Pessimistic-lock execution"):
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
