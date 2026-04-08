#!/usr/bin/env python3
"""
End-to-end test for haqlite shared mode.

Starts multiple haqlite-shared-experiment instances, sends concurrent writes
from an external client, and verifies data integrity.

Usage:
    # Start 2 nodes first (in separate terminals):
    # Node 1: cargo run --features turbolite-cloud,s3-manifest --bin haqlite-shared-experiment -- --port 9001 --instance node-1
    # Node 2: cargo run --features turbolite-cloud,s3-manifest --bin haqlite-shared-experiment -- --port 9002 --instance node-2

    # Then run this test:
    python tests/e2e_shared.py --nodes http://localhost:9001,http://localhost:9002 --writes 20 --duration 10
"""

import argparse
import json
import random
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.request import urlopen, Request
from urllib.error import URLError


def post_json(url, data):
    body = json.dumps(data).encode()
    req = Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def get_json(url):
    try:
        with urlopen(url, timeout=30) as resp:
            return json.loads(resp.read())
    except URLError as e:
        return {"error": str(e), "ok": False}


def write_row(node_url, row_id, value):
    """Write a row to a random node. Returns (id, value, ok, error)."""
    try:
        result = post_json(f"{node_url}/write", {"id": row_id, "value": value})
        return (row_id, value, result.get("ok", False), result.get("error"))
    except Exception as e:
        return (row_id, value, False, str(e))


def main():
    parser = argparse.ArgumentParser(description="e2e test for haqlite shared mode")
    parser.add_argument("--nodes", required=True, help="Comma-separated node URLs")
    parser.add_argument("--writes", type=int, default=50, help="Total writes to send")
    parser.add_argument("--workers", type=int, default=4, help="Concurrent writer threads")
    parser.add_argument("--duration", type=int, default=10, help="Test duration in seconds")
    args = parser.parse_args()

    nodes = [n.strip() for n in args.nodes.split(",")]
    print(f"Nodes: {nodes}")
    print(f"Writes: {args.writes}, Workers: {args.workers}, Duration: {args.duration}s")

    # Health check
    for node in nodes:
        try:
            urlopen(f"{node}/health", timeout=5)
            print(f"  {node}: healthy")
        except Exception as e:
            print(f"  {node}: UNREACHABLE - {e}")
            sys.exit(1)

    # Phase 1: Concurrent writes
    print(f"\n=== Phase 1: {args.writes} concurrent writes ===")
    records = []  # (id, value, ok, error, node)
    start = time.time()

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = []
        for i in range(args.writes):
            row_id = f"row-{i}-{uuid.uuid4().hex[:8]}"
            value = f"value-{i}"
            node = random.choice(nodes)
            futures.append(executor.submit(write_row, node, row_id, value))

        for f in as_completed(futures):
            row_id, value, ok, error = f.result()
            records.append((row_id, value, ok, error))
            if not ok:
                print(f"  FAIL: {row_id} -> {error}")

    elapsed = time.time() - start
    ok_count = sum(1 for r in records if r[2])
    err_count = sum(1 for r in records if not r[2])
    print(f"Done in {elapsed:.1f}s: {ok_count} ok, {err_count} err")

    # Phase 2: Verify from each node
    print(f"\n=== Phase 2: Verify from each node ===")
    ok_records = {r[0]: r[1] for r in records if r[2]}
    violations = 0

    for node in nodes:
        print(f"\n  Checking {node}...")
        verify = get_json(f"{node}/verify")
        if "error" in verify:
            print(f"    ERROR: {verify['error']}")
            violations += 1
            continue
        print(f"    count={verify.get('count', '?')}, duplicates={verify.get('duplicates', '?')}")

        # Check each Ok write is visible
        missing = 0
        for row_id, expected_value in ok_records.items():
            result = get_json(f"{node}/read?id={row_id}")
            if "error" in result:
                print(f"    ERROR reading '{row_id}': {result['error']}")
                missing += 1
                violations += 1
            elif not result.get("found"):
                print(f"    VIOLATION: Ok write '{row_id}' not visible on {node}")
                missing += 1
                violations += 1

        if missing:
            print(f"    {missing} missing Ok writes!")
        else:
            print(f"    All {len(ok_records)} Ok writes visible")

    # Phase 3: Cross-node consistency
    print(f"\n=== Phase 3: Cross-node consistency ===")
    counts = []
    for node in nodes:
        count = get_json(f"{node}/count")["count"]
        counts.append(count)
        print(f"  {node}: {count} rows")

    if len(set(counts)) > 1:
        print(f"  WARNING: nodes disagree on row count: {counts}")
        # This is expected if nodes haven't caught up yet.
        # Fresh reads should agree.

    # Phase 4: Duration test (sustained writes)
    if args.duration > 0:
        print(f"\n=== Phase 4: Sustained writes for {args.duration}s ===")
        sustained_ok = 0
        sustained_err = 0
        end_time = time.time() + args.duration

        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            batch = 0
            while time.time() < end_time:
                futures = []
                for i in range(min(10, args.workers)):
                    row_id = f"sustained-{batch}-{i}-{uuid.uuid4().hex[:8]}"
                    node = random.choice(nodes)
                    futures.append(executor.submit(write_row, node, row_id, f"sustained-{batch}-{i}"))

                for f in as_completed(futures):
                    _, _, ok, error = f.result()
                    if ok:
                        sustained_ok += 1
                    else:
                        sustained_err += 1
                batch += 1

        total = sustained_ok + sustained_err
        rate = total / args.duration if args.duration > 0 else 0
        print(f"  {sustained_ok}/{total} ok ({rate:.1f} ops/s)")

    # Summary
    print(f"\n=== SUMMARY ===")
    print(f"Phase 1: {ok_count}/{len(records)} ok")
    print(f"Violations: {violations}")

    if violations > 0:
        print("FAILED: data integrity violations detected")
        sys.exit(1)
    else:
        print("PASSED: all Ok writes visible, no violations")
        sys.exit(0)


if __name__ == "__main__":
    main()
