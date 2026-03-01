"""Demonstrate queue blocking behavior when queue is full and no active readers."""

import threading
import time

from common import create_client

QUEUE_NAME = "bounded-q"
CAPACITY = 10


def blocking_put(queue, value: int) -> None:
    start = time.perf_counter()
    queue.put(value)
    elapsed = time.perf_counter() - start
    print(f"Blocking put for value={value} finished after {elapsed:.3f}s")


def main() -> None:
    client = create_client("queue-blocking-demo")

    try:
        queue = client.get_queue(QUEUE_NAME).blocking()
        queue.clear()
        print(f"Queue '{QUEUE_NAME}' cleared.")

        print(f"Filling queue to capacity={CAPACITY} with no readers...")
        for value in range(1, CAPACITY + 1):
            queue.put(value)

        print("Queue is full. Starting one extra put() in a thread (it should block).")
        thread = threading.Thread(target=blocking_put, args=(queue, 999), daemon=True)
        thread.start()

        time.sleep(3)
        is_blocking = thread.is_alive()
        print(f"After 3s, blocked thread still waiting: {is_blocking}")

        if not is_blocking:
            print(
                "WARNING: put() did not block. Queue may not be using bounded config. "
                "Restart Hazelcast containers to reload hazelcast.yaml."
            )

        removed = queue.take()
        print(f"Removed one element ({removed}) to free one slot.")

        thread.join(timeout=5)
        print(f"After freeing space, blocked thread finished: {not thread.is_alive()}")
    finally:
        client.shutdown()
        print("Blocking demo client shutdown complete.")


if __name__ == "__main__":
    main()
