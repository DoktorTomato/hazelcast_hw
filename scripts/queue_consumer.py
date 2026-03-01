"""Consumer client: reads from bounded queue until it receives stop marker -1."""

import argparse
import random
import time

from scripts.common import create_client

QUEUE_NAME = "bounded-queue"
STOP_MARKER = -1


def main() -> None:
    parser = argparse.ArgumentParser(description="Hazelcast queue consumer")
    parser.add_argument("--id", default="1", help="Consumer id for logging")
    args = parser.parse_args()

    client_name = f"queue-consumer-{args.id}"
    client = create_client(client_name)
    consumed = 0

    try:
        queue = client.get_queue(QUEUE_NAME).blocking()
        print(f"[{client_name}] Started consuming from '{QUEUE_NAME}'")

        while True:
            value = queue.take()
            if value == STOP_MARKER:
                print(f"[{client_name}] Received stop marker, exiting.")
                break

            consumed += 1
            print(f"[{client_name}] Consumed value={value}")
            time.sleep(random.uniform(0.01, 0.05))

        print(f"[{client_name}] Total consumed={consumed}")
    finally:
        client.shutdown()
        print(f"[{client_name}] Client shutdown complete.")


if __name__ == "__main__":
    main()
