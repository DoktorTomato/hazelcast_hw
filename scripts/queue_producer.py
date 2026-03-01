"""Producer client: writes 1..100 into a bounded distributed queue."""

import time

from common import create_client

QUEUE_NAME = "bounded-queue"
START = 1
END = 100


def main() -> None:
    client = create_client("queue-producer")
    try:
        queue = client.get_queue(QUEUE_NAME).blocking()
        print(f"Producing values {START}..{END} into queue '{QUEUE_NAME}'")

        for value in range(START, END + 1):
            before = time.perf_counter()
            queue.put(value)
            waited = time.perf_counter() - before
            print(f"Produced {value:3d} (waited {waited:.3f}s if queue was full)")

        queue.put(-1)
        queue.put(-1)
        print("Producer done. Sent two stop markers (-1) for two consumers.")
    finally:
        client.shutdown()
        print("Producer client shutdown complete.")


if __name__ == "__main__":
    main()
