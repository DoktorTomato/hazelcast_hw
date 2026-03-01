"""Populate a distributed map with 1,000 values (keys 0..999)."""

from common import create_client


def main() -> None:
    client = create_client("map-basics-client")
    distributed_map = client.get_map("lab-map").blocking()

    print("Populating map 'lab-map' with keys 0..999")
    for key in range(1000):
        distributed_map.set(key, key)
        if key % 200 == 0:
            print(f"Progress: inserted up to key={key}")

    size = distributed_map.size()
    print(f"Done. map size={size}")
    print(f"Sample values: key 0 -> {distributed_map.get(0)}, key 999 -> {distributed_map.get(999)}")
    client.shutdown()
    print("Client shutdown complete.")


if __name__ == "__main__":
    main()
