"""Producer client: writes 1..100 into a bounded distributed queue."""

import argparse
import time

from common import create_client

QUEUE_NAME = "bounded-q"
START = 1
END = 100
STOP_MARKER = -1


def main() -> None:
    parser = argparse.ArgumentParser(description="Hazelcast queue producer")
    parser.add_argument(
        "--clear",
        action="store_true",
        help="Clear queue before producing",
    )
    args = parser.parse_args()

    client = create_client("queue-producer")
    try:
        queue = client.get_queue(QUEUE_NAME).blocking()

        if args.clear:
            queue.clear()
            print(f"Queue '{QUEUE_NAME}' cleared before producing.")

        print(f"Producing values {START}..{END} into queue '{QUEUE_NAME}'")

        for value in range(START, END + 1):
            before = time.perf_counter()
            queue.put(value)
            waited = time.perf_counter() - before
            print(f"Produced {value:3d} (put() wait {waited:.3f}s)")

        queue.put(STOP_MARKER)
        queue.put(STOP_MARKER)
        print("Producer done. Sent two stop markers (-1) for two consumers.")
    finally:
        client.shutdown()
        print("Producer client shutdown complete.")


if __name__ == "__main__":
    main()
