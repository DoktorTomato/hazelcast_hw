"""Check missing/corrupted map keys while shutting down Hazelcast nodes.

Typical usage:
1) Seed map with known keys.
2) Start watch mode.
3) Stop one or more Hazelcast nodes.
4) Observe missing keys in output.
"""

import argparse
import time
from typing import List, Tuple

from common import create_client, timed


def seed_map(distributed_map, start_key: int, expected_count: int) -> None:
    print(
        f"Seeding map '{distributed_map.name}' with {expected_count} entries "
        f"({start_key}..{start_key + expected_count - 1})"
    )
    for index, key in enumerate(range(start_key, start_key + expected_count), start=1):
        distributed_map.set(key, key)
        if index % 1000 == 0:
            print(f"  seeded {index}/{expected_count}")


def check_map(
    distributed_map,
    start_key: int,
    expected_count: int,
    batch_size: int,
    sample_size: int,
) -> Tuple[int, List[int], int, List[Tuple[int, object]]]:
    missing_count = 0
    missing_sample: List[int] = []
    wrong_value_count = 0
    wrong_value_sample: List[Tuple[int, object]] = []

    end_key = start_key + expected_count
    for batch_start in range(start_key, end_key, batch_size):
        batch_end = min(batch_start + batch_size, end_key)
        batch_keys = list(range(batch_start, batch_end))

        entries = distributed_map.get_all(set(batch_keys))

        for key in batch_keys:
            if key not in entries:
                missing_count += 1
                if len(missing_sample) < sample_size:
                    missing_sample.append(key)
                continue

            value = entries[key]
            if value != key:
                wrong_value_count += 1
                if len(wrong_value_sample) < sample_size:
                    wrong_value_sample.append((key, value))

    return missing_count, missing_sample, wrong_value_count, wrong_value_sample


def run_check_loop(
    distributed_map,
    start_key: int,
    expected_count: int,
    batch_size: int,
    sample_size: int,
    watch_interval: float,
    rounds: int,
) -> None:
    iteration = 1
    while True:
        with timed(f"Check round {iteration}"):
            missing_count, missing_sample, wrong_count, wrong_sample = check_map(
                distributed_map=distributed_map,
                start_key=start_key,
                expected_count=expected_count,
                batch_size=batch_size,
                sample_size=sample_size,
            )

        checked = expected_count
        ok = missing_count == 0 and wrong_count == 0
        status = "OK" if ok else "ISSUES DETECTED"
        print(f"[{time.strftime('%H:%M:%S')}] {status} | checked={checked} missing={missing_count} wrong_values={wrong_count}")

        if missing_sample:
            print(f"  missing sample: {missing_sample}")
        if wrong_sample:
            print(f"  wrong value sample: {wrong_sample}")

        if watch_interval <= 0:
            break
        if rounds > 0 and iteration >= rounds:
            break

        iteration += 1
        time.sleep(watch_interval)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Check map keys before/after node shutdown and detect lost keys"
    )
    parser.add_argument("--map-name", default="lab-map", help="Map name to check")
    parser.add_argument("--start-key", type=int, default=0, help="First expected key")
    parser.add_argument(
        "--expected-count", type=int, default=1000, help="How many consecutive keys are expected"
    )
    parser.add_argument(
        "--batch-size", type=int, default=500, help="How many keys to fetch per get_all call"
    )
    parser.add_argument(
        "--sample-size", type=int, default=20, help="How many missing/wrong keys to print"
    )
    parser.add_argument(
        "--seed",
        action="store_true",
        help="Populate map with key->value pairs before checking",
    )
    parser.add_argument(
        "--watch-interval",
        type=float,
        default=0.0,
        help="Seconds between checks; 0 means one-shot",
    )
    parser.add_argument(
        "--rounds",
        type=int,
        default=0,
        help="Number of check rounds in watch mode; 0 means infinite",
    )
    args = parser.parse_args()

    if args.expected_count <= 0:
        raise ValueError("--expected-count must be positive")
    if args.batch_size <= 0:
        raise ValueError("--batch-size must be positive")
    if args.sample_size <= 0:
        raise ValueError("--sample-size must be positive")
    if args.watch_interval < 0:
        raise ValueError("--watch-interval must be non-negative")
    if args.rounds < 0:
        raise ValueError("--rounds must be non-negative")

    client = create_client("map-shutdown-checker")
    try:
        distributed_map = client.get_map(args.map_name).blocking()

        if args.seed:
            with timed("Seeding"):
                seed_map(distributed_map, args.start_key, args.expected_count)

        run_check_loop(
            distributed_map=distributed_map,
            start_key=args.start_key,
            expected_count=args.expected_count,
            batch_size=args.batch_size,
            sample_size=args.sample_size,
            watch_interval=args.watch_interval,
            rounds=args.rounds,
        )
    except KeyboardInterrupt:
        print("Interrupted by user.")
    finally:
        client.shutdown()
        print("Checker client shutdown complete.")


if __name__ == "__main__":
    main()
