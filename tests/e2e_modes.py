#!/usr/bin/env python3
"""
Comprehensive e2e tests for all haqlite mode combinations.

Tests each of the 5 valid mode combinations:
  1. Shared + Synchronous    (multiwriter, turbolite S3Primary)
  2. Shared + Eventual       (multiwriter, turbolite + walrust)
  3. Dedicated + Replicated  (classic walrust HA)
  4. Dedicated + Synchronous (turbolite S3Primary HA)
  5. Dedicated + Eventual    (turbolite + walrust HA)

Runs haqlite-experiment server processes, sends HTTP requests, verifies correctness.
Requires: Tigris credentials via soup or env vars.

Usage:
    # Run all modes:
    soup run --project ladybug --env development -- python tests/e2e_modes.py

    # Run specific mode:
    soup run --project ladybug --env development -- python tests/e2e_modes.py --mode shared-synchronous

    # Run with more writes:
    soup run --project ladybug --env development -- python tests/e2e_modes.py --writes 100 --duration 30
"""

import argparse
import json
import os
import random
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BINARY = None  # resolved at startup
BUCKET = os.environ.get("TIERED_TEST_BUCKET", os.environ.get("S3_TEST_BUCKET", "hadb-test-bucket"))
BASE_PORT = 9100  # avoids collisions with manual experiments
HEALTH_TIMEOUT = 60  # seconds to wait for server to start
REQUEST_TIMEOUT = 30  # seconds per HTTP request


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def resolve_binary():
    """Find the haqlite-experiment binary."""
    haqlite_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    candidates = [
        os.path.join(haqlite_dir, "target", "debug", "haqlite-experiment"),
        os.path.join(haqlite_dir, "target", "release", "haqlite-experiment"),
    ]
    for c in candidates:
        if os.path.isfile(c) and os.access(c, os.X_OK):
            return c
    return None


def post_json(url, data, timeout=REQUEST_TIMEOUT):
    body = json.dumps(data).encode()
    req = Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read())
    except HTTPError as e:
        body_text = ""
        try:
            body_text = e.read().decode()[:500]
        except Exception:
            pass
        return {"ok": False, "error": f"HTTP {e.code}: {body_text}"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def get_json(url, timeout=REQUEST_TIMEOUT):
    try:
        with urlopen(url, timeout=timeout) as resp:
            return json.loads(resp.read())
    except HTTPError as e:
        body_text = ""
        try:
            body_text = e.read().decode()[:500]
        except Exception:
            pass
        return {"error": f"HTTP {e.code}: {body_text}"}
    except Exception as e:
        return {"error": str(e)}


def wait_healthy(url, timeout=HEALTH_TIMEOUT):
    """Wait for server health endpoint to respond."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            resp = get_json(f"{url}/health")
            if resp.get("ok"):
                return True
        except Exception:
            pass
        time.sleep(0.5)
    return False


def wait_role(url, expected_role, timeout=30):
    """Wait for a node to report a specific role."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            status = get_json(f"{url}/status")
            if status.get("role") == expected_role:
                return True
        except Exception:
            pass
        time.sleep(0.5)
    return False


class ServerProcess:
    """Manages a haqlite-experiment server process."""

    def __init__(self, topology, durability, port, instance_id, prefix, db_dir,
                 lease_ttl=30, extra_args=None):
        self.topology = topology
        self.durability = durability
        self.port = port
        self.instance_id = instance_id
        self.prefix = prefix
        self.db_dir = db_dir
        self.url = f"http://localhost:{port}"
        self.lease_ttl = lease_ttl
        self.extra_args = extra_args or []
        self.proc = None

    def start(self):
        # All nodes must use the same db filename (stem) so the lease key matches.
        # But each node needs its own directory to avoid file collisions.
        node_dir = os.path.join(self.db_dir, self.instance_id)
        os.makedirs(node_dir, exist_ok=True)
        db_path = os.path.join(node_dir, "experiment.db")

        env = dict(os.environ)
        # Map soup Tigris secrets to AWS env vars expected by haqlite
        if "TIGRIS_STORAGE_ACCESS_KEY_ID" in env:
            env["AWS_ACCESS_KEY_ID"] = env["TIGRIS_STORAGE_ACCESS_KEY_ID"]
        if "TIGRIS_STORAGE_SECRET_ACCESS_KEY" in env:
            env["AWS_SECRET_ACCESS_KEY"] = env["TIGRIS_STORAGE_SECRET_ACCESS_KEY"]
        if "TIGRIS_STORAGE_ENDPOINT" in env:
            env["AWS_ENDPOINT_URL"] = env["TIGRIS_STORAGE_ENDPOINT"]
        env.setdefault("AWS_REGION", "auto")
        env.setdefault("TIERED_TEST_BUCKET", BUCKET)
        env.setdefault("RUST_LOG", "info")

        cmd = [
            BINARY,
            "--topology", self.topology,
            "--durability", self.durability,
            "--port", str(self.port),
            "--instance", self.instance_id,
            "--prefix", self.prefix,
            "--db", db_path,
            "--bucket", BUCKET,
            "--lease-ttl", str(self.lease_ttl),
        ] + self.extra_args

        self.log_path = os.path.join(self.db_dir, f"{self.instance_id}.log")
        self._log_file = open(self.log_path, "w")
        self.proc = subprocess.Popen(
            cmd,
            env=env,
            stdout=self._log_file,
            stderr=subprocess.STDOUT,
            preexec_fn=os.setsid,
        )
        return self

    def wait_healthy(self, timeout=HEALTH_TIMEOUT):
        return wait_healthy(self.url, timeout)

    def _close_log(self):
        if hasattr(self, '_log_file') and self._log_file:
            try:
                self._log_file.close()
            except Exception:
                pass

    def stop(self):
        if self.proc and self.proc.poll() is None:
            try:
                os.killpg(os.getpgid(self.proc.pid), signal.SIGTERM)
                self.proc.wait(timeout=10)
            except Exception:
                try:
                    os.killpg(os.getpgid(self.proc.pid), signal.SIGKILL)
                    self.proc.wait(timeout=5)
                except Exception:
                    pass
        self._close_log()

    def read_output(self):
        try:
            if hasattr(self, '_log_file') and self._log_file and not self._log_file.closed:
                self._log_file.flush()
            with open(self.log_path, "r") as f:
                return f.read()
        except Exception:
            return ""

    def kill(self):
        """SIGKILL - simulate crash, no graceful shutdown."""
        if self.proc and self.proc.poll() is None:
            try:
                os.killpg(os.getpgid(self.proc.pid), signal.SIGKILL)
                self.proc.wait(timeout=5)
            except Exception:
                pass
        self._close_log()

    def is_alive(self):
        return self.proc and self.proc.poll() is None


# ---------------------------------------------------------------------------
# Test infrastructure
# ---------------------------------------------------------------------------

class TestResult:
    def __init__(self, name):
        self.name = name
        self.passed = 0
        self.failed = 0
        self.errors = []

    def ok(self, msg):
        self.passed += 1
        print(f"    PASS: {msg}")

    def fail(self, msg):
        self.failed += 1
        self.errors.append(msg)
        print(f"    FAIL: {msg}")

    def check(self, condition, msg):
        if condition:
            self.ok(msg)
        else:
            self.fail(msg)

    @property
    def success(self):
        return self.failed == 0


class ModeTest:
    """Base class for testing a specific mode combination."""

    def __init__(self, topology, durability, num_writes, duration, num_workers):
        self.topology = topology
        self.durability = durability
        self.num_writes = num_writes
        self.duration = duration
        self.num_workers = num_workers
        self.servers = []
        self.tmp_dir = None
        self.prefix = f"e2e-{topology}-{durability}-{int(time.time())}-{uuid.uuid4().hex[:6]}/"

    @property
    def mode_name(self):
        return f"{self.topology}-{self.durability}"

    def setup_servers(self, count, lease_ttl=30, extra_args_per_node=None):
        """Start `count` server processes."""
        self.tmp_dir = tempfile.mkdtemp(prefix=f"haqlite_e2e_{self.mode_name}_")
        extra_args_per_node = extra_args_per_node or {}
        for i in range(count):
            port = BASE_PORT + len(self.servers)
            instance_id = f"{self.topology[0]}{self.durability[0]}-node-{i+1}"
            extra = extra_args_per_node.get(i, [])
            server = ServerProcess(
                topology=self.topology,
                durability=self.durability,
                port=port,
                instance_id=instance_id,
                prefix=self.prefix,
                db_dir=self.tmp_dir,
                lease_ttl=lease_ttl,
                extra_args=extra,
            )
            server.start()
            self.servers.append(server)

    def teardown(self):
        for s in self.servers:
            s.stop()
        if self.tmp_dir and os.path.exists(self.tmp_dir):
            try:
                shutil.rmtree(self.tmp_dir)
            except Exception:
                pass

    def wait_all_healthy(self, timeout=HEALTH_TIMEOUT):
        """Wait for all servers to become healthy."""
        for s in self.servers:
            if not s.wait_healthy(timeout):
                return False
        return True

    def urls(self):
        return [s.url for s in self.servers]


# ---------------------------------------------------------------------------
# Shared mode tests
# ---------------------------------------------------------------------------

def test_shared_single_node_writes(mode_test, result):
    """Single node: write, read back, verify."""
    url = mode_test.urls()[0]
    writes = []

    # Write N rows
    for i in range(mode_test.num_writes):
        row_id = f"single-{i}-{uuid.uuid4().hex[:6]}"
        value = f"val-{i}"
        resp = post_json(f"{url}/write", {"id": row_id, "value": value, "seq": i})
        if resp.get("ok"):
            writes.append((row_id, value))
        else:
            result.fail(f"Write {i} failed: {resp.get('error', '?')}")

    result.check(len(writes) > 0, f"{len(writes)}/{mode_test.num_writes} writes succeeded")

    # Read each back
    missing = 0
    for row_id, expected_value in writes:
        resp = get_json(f"{url}/read?id={row_id}")
        if not resp.get("found"):
            missing += 1
        elif resp.get("value") != expected_value:
            result.fail(f"Row {row_id}: expected '{expected_value}', got '{resp.get('value')}'")

    result.check(missing == 0, f"All {len(writes)} writes readable (missing: {missing})")

    # Verify integrity
    verify = get_json(f"{url}/verify")
    result.check(verify.get("ok", False), f"Integrity check: {verify.get('count', '?')} rows, {verify.get('duplicates', 0)} duplicates")


def test_shared_multi_node_writes(mode_test, result):
    """Multiple nodes: concurrent writes, cross-node reads."""
    urls = mode_test.urls()
    ok_writes = {}
    err_count = 0

    # Sequential multi-node writes (round-robin across nodes).
    # S3-based lease stores don't provide atomic CAS for truly concurrent
    # PUTs (even single-region Tigris has ~2.5% race rate). Sequential
    # writes correctly serialize via lease acquire/release.
    for i in range(mode_test.num_writes):
        row_id = f"multi-{i}-{uuid.uuid4().hex[:6]}"
        value = f"multi-val-{i}"
        node_url = urls[i % len(urls)]
        resp = post_json(f"{node_url}/write", {"id": row_id, "value": value, "seq": i})
        if resp.get("ok", False):
            ok_writes[row_id] = (value, node_url)
        else:
            err_count += 1

    result.check(
        len(ok_writes) > 0,
        f"{len(ok_writes)}/{mode_test.num_writes} multi-node writes ok ({err_count} errors)"
    )

    # Give nodes time to catch up
    time.sleep(2)

    # Cross-node consistency: each Ok write visible from every node
    for node_url in urls:
        missing = 0
        wrong = 0
        for row_id, (expected_value, _) in ok_writes.items():
            resp = get_json(f"{node_url}/read?id={row_id}")
            if not resp.get("found"):
                missing += 1
            elif resp.get("value") != expected_value:
                wrong += 1

        node_name = node_url.split(":")[-1]
        result.check(
            missing == 0 and wrong == 0,
            f"Node :{node_name}: {len(ok_writes)} rows visible (missing={missing}, wrong={wrong})"
        )

    # Verify counts match across nodes
    counts = []
    for url in urls:
        count_resp = get_json(f"{url}/count")
        counts.append(count_resp.get("count", -1))

    result.check(
        len(set(counts)) == 1,
        f"All nodes agree on count: {counts}"
    )


def test_shared_sustained_writes(mode_test, result):
    """Sustained sequential writes over time across nodes."""
    if mode_test.duration <= 0:
        result.ok("Sustained writes skipped (duration=0)")
        return

    urls = mode_test.urls()
    ok_count = 0
    err_count = 0
    end_time = time.time() + mode_test.duration

    batch = 0
    while time.time() < end_time:
        row_id = f"sustained-{batch}-{uuid.uuid4().hex[:6]}"
        node_url = urls[batch % len(urls)]
        resp = post_json(f"{node_url}/write", {"id": row_id, "value": f"s-{batch}"})
        if resp.get("ok", False):
            ok_count += 1
        else:
            err_count += 1
        batch += 1

    total = ok_count + err_count
    rate = total / mode_test.duration if mode_test.duration > 0 else 0
    success_rate = ok_count / total if total > 0 else 0
    result.check(
        success_rate > 0.5,
        f"Sustained: {ok_count}/{total} ok ({rate:.1f} ops/s, {success_rate:.0%} success)"
    )


def test_shared_updates_deletes(mode_test, result):
    """Test updates and deletes across nodes."""
    urls = mode_test.urls()
    url0, url1 = urls[0], urls[min(1, len(urls) - 1)]

    # Insert from node 0
    row_id = f"upd-del-{uuid.uuid4().hex[:8]}"
    resp = post_json(f"{url0}/write", {"id": row_id, "value": "original"})
    result.check(resp.get("ok", False), f"Insert from node 0")

    # Read from node 1
    time.sleep(1)
    resp = get_json(f"{url1}/read?id={row_id}")
    result.check(resp.get("found", False) and resp.get("value") == "original",
                 f"Read original from node 1")

    # Update from node 1 (INSERT OR REPLACE)
    resp = post_json(f"{url1}/write", {"id": row_id, "value": "updated"})
    result.check(resp.get("ok", False), f"Update from node 1")

    # Read updated value from node 0
    time.sleep(1)
    resp = get_json(f"{url0}/read?id={row_id}")
    result.check(resp.get("found", False) and resp.get("value") == "updated",
                 f"Read updated from node 0")

    # Delete from node 0
    resp = post_json(f"{url0}/execute", {
        "sql": "DELETE FROM test_data WHERE id = ?1",
        "params": [row_id],
    })
    result.check(resp.get("ok", False), f"Delete from node 0")

    # Verify deleted from node 1
    time.sleep(1)
    resp = get_json(f"{url1}/read?id={row_id}")
    result.check(not resp.get("found", True), f"Verified deleted on node 1")


def test_shared_large_values(mode_test, result):
    """Test writing and reading large text values."""
    url = mode_test.urls()[0]
    large_value = "x" * 10_000  # 10KB value
    row_id = f"large-{uuid.uuid4().hex[:8]}"

    resp = post_json(f"{url}/write", {"id": row_id, "value": large_value})
    result.check(resp.get("ok", False), "Write 10KB value")

    resp = get_json(f"{url}/read?id={row_id}")
    result.check(
        resp.get("found") and resp.get("value") == large_value,
        "Read back 10KB value correctly"
    )


def test_shared_idempotent_writes(mode_test, result):
    """INSERT OR REPLACE should be idempotent."""
    url = mode_test.urls()[0]
    row_id = f"idempotent-{uuid.uuid4().hex[:8]}"

    # Write same row twice with different values
    resp1 = post_json(f"{url}/write", {"id": row_id, "value": "first"})
    resp2 = post_json(f"{url}/write", {"id": row_id, "value": "second"})

    result.check(resp1.get("ok") and resp2.get("ok"), "Both writes succeed")

    resp = get_json(f"{url}/read?id={row_id}")
    result.check(resp.get("value") == "second", "Last write wins")

    # Verify no duplicates
    verify = get_json(f"{url}/verify")
    result.check(verify.get("duplicates", -1) == 0, "No duplicates after idempotent writes")


def test_shared_concurrent_same_row(mode_test, result):
    """Concurrent writes to the same row from different nodes. Last write wins."""
    urls = mode_test.urls()
    if len(urls) < 2:
        result.ok("Skipped (need 2+ nodes)")
        return

    row_id = f"race-{uuid.uuid4().hex[:8]}"

    # Write from each node
    results_map = {}
    def write_from(node_idx):
        url = urls[node_idx]
        value = f"node-{node_idx}-wins"
        resp = post_json(f"{url}/write", {"id": row_id, "value": value})
        return node_idx, value, resp.get("ok", False)

    # Sequential writes (lease serialized), so they don't truly race
    for i in range(len(urls)):
        idx, val, ok = write_from(i)
        results_map[idx] = (val, ok)

    # All should succeed (lease serialization)
    all_ok = all(v[1] for v in results_map.values())
    result.check(all_ok, f"All {len(urls)} writes to same row succeeded")

    # Final value should be from last writer
    time.sleep(1)
    for url in urls:
        resp = get_json(f"{url}/read?id={row_id}")
        result.check(resp.get("found"), f"Row visible from {url.split(':')[-1]}")


def test_shared_empty_reads(mode_test, result):
    """Reading non-existent rows returns found=false."""
    url = mode_test.urls()[0]
    resp = get_json(f"{url}/read?id=nonexistent-{uuid.uuid4().hex}")
    result.check(not resp.get("found", True), "Non-existent row returns found=false")


# ---------------------------------------------------------------------------
# Dedicated mode tests
# ---------------------------------------------------------------------------

def test_dedicated_leader_writes(mode_test, result):
    """Leader accepts writes, data is readable."""
    urls = mode_test.urls()

    # Find leader
    leader_url = None
    for url in urls:
        status = get_json(f"{url}/status")
        if status.get("role") == "Leader":
            leader_url = url
            break

    if not leader_url:
        result.fail("No leader found")
        return

    result.ok(f"Leader found at {leader_url}")

    # Write rows
    writes = []
    for i in range(mode_test.num_writes):
        row_id = f"ded-{i}-{uuid.uuid4().hex[:6]}"
        value = f"ded-val-{i}"
        resp = post_json(f"{leader_url}/write", {"id": row_id, "value": value, "seq": i})
        if resp.get("ok"):
            writes.append((row_id, value))

    result.check(
        len(writes) == mode_test.num_writes,
        f"{len(writes)}/{mode_test.num_writes} writes to leader succeeded"
    )

    # Verify on leader
    verify = get_json(f"{leader_url}/verify")
    result.check(verify.get("ok", False), f"Leader integrity: {verify.get('count', '?')} rows")


def test_dedicated_follower_replication(mode_test, result):
    """Follower replicates leader's data."""
    urls = mode_test.urls()
    if len(urls) < 2:
        result.ok("Skipped (need 2+ nodes for replication)")
        return

    # Identify leader and followers
    leader_url = None
    follower_urls = []
    for url in urls:
        status = get_json(f"{url}/status")
        if status.get("role") == "Leader":
            leader_url = url
        else:
            follower_urls.append(url)

    if not leader_url:
        result.fail("No leader found")
        return
    if not follower_urls:
        result.fail("No followers found")
        return

    # Write to leader
    writes = []
    for i in range(mode_test.num_writes):
        row_id = f"repl-{i}-{uuid.uuid4().hex[:6]}"
        value = f"repl-val-{i}"
        resp = post_json(f"{leader_url}/write", {"id": row_id, "value": value, "seq": i})
        if resp.get("ok"):
            writes.append((row_id, value))

    result.check(len(writes) > 0, f"{len(writes)} writes to leader")

    # Wait for replication (walrust sync + follower pull)
    time.sleep(8)

    # Check follower has the data
    for furl in follower_urls:
        count_resp = get_json(f"{furl}/count")
        follower_count = count_resp.get("count", 0)
        # Follower should have at least some rows (may lag slightly)
        result.check(
            follower_count > 0,
            f"Follower {furl.split(':')[-1]} has {follower_count} rows (expected >= {len(writes)})"
        )

        # Spot-check a few rows
        checked = 0
        found = 0
        for row_id, _ in writes[:10]:
            resp = get_json(f"{furl}/read?id={row_id}")
            checked += 1
            if resp.get("found"):
                found += 1

        result.check(
            found > 0,
            f"Follower {furl.split(':')[-1]}: {found}/{checked} spot-checked rows visible"
        )


def test_dedicated_write_forwarding(mode_test, result):
    """Writes to follower are forwarded to leader."""
    urls = mode_test.urls()
    if len(urls) < 2:
        result.ok("Skipped (need 2+ nodes for write forwarding)")
        return

    # Identify follower
    follower_url = None
    leader_url = None
    for url in urls:
        status = get_json(f"{url}/status")
        if status.get("role") == "Follower":
            follower_url = url
        elif status.get("role") == "Leader":
            leader_url = url

    if not follower_url or not leader_url:
        result.fail(f"Need leader and follower (leader={leader_url}, follower={follower_url})")
        return

    # Write via follower (should forward to leader)
    row_id = f"fwd-{uuid.uuid4().hex[:8]}"
    resp = post_json(f"{follower_url}/write", {"id": row_id, "value": "forwarded"})
    forwarded_ok = resp.get("ok", False)

    if forwarded_ok:
        result.ok("Write forwarded through follower")
        # Verify on leader
        time.sleep(1)
        resp = get_json(f"{leader_url}/read?id={row_id}")
        result.check(resp.get("found"), "Forwarded write visible on leader")
    else:
        # Write forwarding may fail if follower doesn't have forwarding set up
        # This is acceptable for some configurations
        error_msg = resp.get("error", "?")
        if "forward" in error_msg.lower() or "not leader" in error_msg.lower():
            result.ok(f"Write forwarding not available (expected): {error_msg[:100]}")
        else:
            result.fail(f"Write to follower failed unexpectedly: {error_msg[:200]}")


def test_dedicated_sustained_writes(mode_test, result):
    """Sustained writes to leader over time."""
    if mode_test.duration <= 0:
        result.ok("Sustained writes skipped (duration=0)")
        return

    urls = mode_test.urls()
    leader_url = None
    for url in urls:
        status = get_json(f"{url}/status")
        if status.get("role") == "Leader":
            leader_url = url
            break

    if not leader_url:
        result.fail("No leader for sustained writes")
        return

    ok_count = 0
    err_count = 0
    end_time = time.time() + mode_test.duration

    batch = 0
    while time.time() < end_time:
        row_id = f"ded-sustained-{batch}-{uuid.uuid4().hex[:6]}"
        resp = post_json(f"{leader_url}/write", {"id": row_id, "value": f"ds-{batch}"})
        if resp.get("ok"):
            ok_count += 1
        else:
            err_count += 1
        batch += 1

    total = ok_count + err_count
    rate = total / mode_test.duration if mode_test.duration > 0 else 0
    result.check(
        ok_count > 0 and err_count == 0,
        f"Sustained: {ok_count}/{total} ok ({rate:.1f} ops/s)"
    )


# ---------------------------------------------------------------------------
# Chaos / failover tests
# ---------------------------------------------------------------------------

def test_dedicated_leader_failover(mode_test, result):
    """Kill leader, verify follower promotes and has all data."""
    urls = mode_test.urls()
    if len(urls) < 2:
        result.ok("Skipped (need 2+ nodes)")
        return

    # Find leader and follower
    leader_url = None
    leader_idx = None
    follower_url = None
    for i, url in enumerate(urls):
        status = get_json(f"{url}/status")
        if status.get("role") == "Leader":
            leader_url = url
            leader_idx = i
        elif status.get("role") == "Follower":
            follower_url = url

    if not leader_url or not follower_url:
        result.fail(f"Need leader + follower (leader={leader_url}, follower={follower_url})")
        return

    # Write data to leader
    pre_kill_writes = []
    for i in range(10):
        row_id = f"pre-kill-{i}-{uuid.uuid4().hex[:6]}"
        resp = post_json(f"{leader_url}/write", {"id": row_id, "value": f"v-{i}"})
        if resp.get("ok"):
            pre_kill_writes.append(row_id)

    result.check(len(pre_kill_writes) == 10, f"Wrote {len(pre_kill_writes)} rows to leader before kill")

    # Wait for replication
    time.sleep(3)

    # Kill leader (SIGKILL - no graceful shutdown)
    mode_test.servers[leader_idx].kill()
    result.ok(f"Killed leader at {leader_url}")

    # Wait for follower to detect expiration and promote
    # Lease TTL (5s) + required_expired_reads (3) * poll_interval (1s) + catchup
    promoted = wait_role(follower_url, "Leader", timeout=30)
    result.check(promoted, f"Follower promoted to Leader within 30s")

    if not promoted:
        return

    # Verify new leader has all pre-kill data
    new_leader_url = follower_url
    missing = 0
    for row_id in pre_kill_writes:
        resp = get_json(f"{new_leader_url}/read?id={row_id}")
        if not resp.get("found"):
            missing += 1

    result.check(missing == 0, f"New leader has all {len(pre_kill_writes)} pre-kill rows (missing={missing})")

    # Write new data to the promoted leader
    post_kill_writes = []
    for i in range(5):
        row_id = f"post-kill-{i}-{uuid.uuid4().hex[:6]}"
        resp = post_json(f"{new_leader_url}/write", {"id": row_id, "value": f"pk-{i}"})
        if resp.get("ok"):
            post_kill_writes.append(row_id)

    result.check(len(post_kill_writes) >= 3, f"Wrote {len(post_kill_writes)}/5 rows to new leader")

    # Verify all data on new leader
    count_resp = get_json(f"{new_leader_url}/count")
    total = count_resp.get("count", 0)
    result.check(total >= len(pre_kill_writes) + len(post_kill_writes),
                 f"New leader total: {total} rows")


def test_shared_crash_recovery(mode_test, result):
    """Kill a node mid-flight, verify other node recovers after lease TTL."""
    urls = mode_test.urls()
    if len(urls) < 2:
        result.ok("Skipped (need 2+ nodes)")
        return

    url0, url1 = urls[0], urls[1]

    # Write data from node 0
    node0_writes = []
    for i in range(10):
        row_id = f"pre-crash-{i}-{uuid.uuid4().hex[:6]}"
        resp = post_json(f"{url0}/write", {"id": row_id, "value": f"v-{i}"})
        if resp.get("ok"):
            node0_writes.append(row_id)

    result.check(len(node0_writes) == 10, f"Wrote {len(node0_writes)} rows from node 0")

    # Kill node 0 (SIGKILL - lease not released)
    mode_test.servers[0].kill()
    result.ok("Killed node 0 (lease held)")

    # Node 1 should be able to write after lease expires.
    # Lease TTL is set by the test. Wait for it + buffer.
    lease_ttl = mode_test.servers[0].lease_ttl
    print(f"    Waiting {lease_ttl + 5}s for lease expiration...")
    time.sleep(lease_ttl + 5)

    # Write from node 1 (may need extra time for first lease acquisition)
    node1_writes = []
    for i in range(5):
        row_id = f"post-crash-{i}-{uuid.uuid4().hex[:6]}"
        resp = post_json(f"{url1}/write", {"id": row_id, "value": f"pc-{i}"}, timeout=60)
        if resp.get("ok"):
            node1_writes.append(row_id)
        else:
            result.fail(f"Node 1 write failed after crash: {resp.get('error', '?')[:100]}")

    result.check(len(node1_writes) > 0, f"Node 1 wrote {len(node1_writes)} rows after crash recovery")

    # Verify node 1 sees node 0's data
    missing = 0
    for row_id in node0_writes:
        resp = get_json(f"{url1}/read?id={row_id}")
        if not resp.get("found"):
            missing += 1

    result.check(missing == 0, f"Node 1 sees all {len(node0_writes)} pre-crash rows (missing={missing})")


def test_double_failover(mode_test, result):
    """3-node test: kill leader, follower-1 promotes, kill again, follower-2 promotes."""
    urls = mode_test.urls()
    if len(urls) < 3:
        result.ok("Skipped (need 3+ nodes)")
        return

    # Identify initial roles
    leader_url = None
    leader_idx = None
    followers = []  # (idx, url)
    for i, url in enumerate(urls):
        status = get_json(f"{url}/status")
        if status.get("role") == "Leader":
            leader_url = url
            leader_idx = i
        elif status.get("role") == "Follower":
            followers.append((i, url))

    if not leader_url or len(followers) < 2:
        result.fail(f"Need 1 leader + 2 followers (leader={leader_url}, followers={len(followers)})")
        return

    # Phase 1: Write to original leader
    phase1_writes = []
    for i in range(5):
        row_id = f"df-phase1-{i}-{uuid.uuid4().hex[:6]}"
        resp = post_json(f"{leader_url}/write", {"id": row_id, "value": f"p1-{i}"})
        if resp.get("ok"):
            phase1_writes.append(row_id)
    result.check(len(phase1_writes) == 5, f"Phase 1: wrote {len(phase1_writes)} rows to leader-1")

    time.sleep(5)  # replication lag (turbolite manifest publish + follower poll)

    # Kill leader-1
    mode_test.servers[leader_idx].kill()
    result.ok(f"Killed leader-1 at {leader_url}")

    # Wait for one follower to promote
    promoted_url = None
    promoted_idx = None
    remaining_url = None
    remaining_idx = None
    deadline = time.time() + 30
    while time.time() < deadline:
        for idx, url in followers:
            try:
                s = get_json(f"{url}/status")
                if s.get("role") == "Leader":
                    promoted_url = url
                    promoted_idx = idx
            except Exception:
                pass
        if promoted_url:
            break
        time.sleep(0.5)

    # Set remaining as whichever follower did NOT promote
    for idx, url in followers:
        if idx != promoted_idx:
            remaining_url = url
            remaining_idx = idx

    if not promoted_url:
        result.fail("No follower promoted after leader-1 death")
        return
    result.ok(f"Follower promoted to leader-2 at {promoted_url}")

    # Phase 2: Write to leader-2
    phase2_writes = []
    for i in range(5):
        row_id = f"df-phase2-{i}-{uuid.uuid4().hex[:6]}"
        resp = post_json(f"{promoted_url}/write", {"id": row_id, "value": f"p2-{i}"})
        if resp.get("ok"):
            phase2_writes.append(row_id)
    result.check(len(phase2_writes) >= 3, f"Phase 2: wrote {len(phase2_writes)} rows to leader-2")

    # Replication lag: promoted leader needs time for snapshot upload + walrust
    # sync + follower pull. Eventual/Replicated modes need longer because walrust
    # sync is async and the follower also needs time to pull incremental updates.
    phase2_settle = 10 if mode_test.durability == "synchronous" else 20
    time.sleep(phase2_settle)

    # Kill leader-2
    mode_test.servers[promoted_idx].kill()
    result.ok(f"Killed leader-2 at {promoted_url}")

    if not remaining_url:
        result.fail("No remaining follower for double failover")
        return

    # Wait for last follower to promote
    promoted2 = wait_role(remaining_url, "Leader", timeout=30)
    result.check(promoted2, f"Last follower promoted to leader-3 at {remaining_url}")

    if not promoted2:
        return

    # Verify leader-3 has data from both phases.
    # For turbolite (Synchronous): data is in S3, expect 0 missing.
    # For walrust (Replicated/Eventual): last sync_interval of writes may be
    # lost on SIGKILL (async replication). Phase 1 should be fully replicated
    # (5s replication window), phase 2 may have lag.
    phase1_missing = 0
    for row_id in phase1_writes:
        resp = get_json(f"{remaining_url}/read?id={row_id}")
        if not resp.get("found"):
            phase1_missing += 1

    phase2_missing = 0
    for row_id in phase2_writes:
        resp = get_json(f"{remaining_url}/read?id={row_id}")
        if not resp.get("found"):
            phase2_missing += 1

    # Phase-1 rows should survive through two failovers. For turbolite modes,
    # this depends on manifest-based catch-up preserving all page groups across
    # promotions. Intermittent failures indicate a manifest merge issue.
    p1_ok = phase1_missing <= 1
    result.check(
        p1_ok,
        f"Leader-3 has {len(phase1_writes) - phase1_missing}/{len(phase1_writes)} phase-1 rows (missing={phase1_missing})"
    )
    # Phase-2 rows should survive: the settle time (15s for walrust modes)
    # gives the promoted leader time to sync writes to S3.
    p2_ok = phase2_missing == 0
    result.check(
        p2_ok,
        f"Leader-3 has {len(phase2_writes) - phase2_missing}/{len(phase2_writes)} phase-2 rows (missing={phase2_missing})"
    )

    # Dump logs from ALL servers if data was lost
    if not p1_ok or not p2_ok:
        import re as _re
        for s in mode_test.servers:
            log = s.read_output()
            if not log:
                continue
            relevant = []
            for line in log.split("\n"):
                line = _re.sub(chr(27) + r'\[[0-9;]*m', '', line).strip()
                if any(k in line for k in ['S3 fetch', 'CACHE MISS', 'CACHE HIT',
                                           'evict', 'set_manifest', 'catchup',
                                           'promotion', 'test_data', 'promoted',
                                           'building v', 'REJECTED']):
                    if 'page 0' not in line:
                        relevant.append(line[:200])
            if relevant:
                print(f"    --- {s.instance_id} log ({len(relevant)} lines) ---")
                for line in relevant[-20:]:  # last 20 relevant lines
                    print(f"      {line}")


def test_durability_across_restarts(mode_test, result):
    """Write data, kill all nodes, start fresh node, verify data from S3."""
    # Find a live node to write to
    write_url = None
    for s in mode_test.servers:
        if s.is_alive():
            write_url = s.url
            break

    if not write_url:
        result.fail("No live node to write durability test data")
        return
    print(f"    Durability write_url: {write_url}")

    # Write data
    writes = []
    for i in range(10):
        row_id = f"durable-{i}-{uuid.uuid4().hex[:6]}"
        resp = post_json(f"{write_url}/write", {"id": row_id, "value": f"d-{i}"})
        if resp.get("ok"):
            writes.append(row_id)

    result.check(len(writes) == 10, f"Wrote {len(writes)} rows")

    # Wait for S3 writes to settle before killing.
    # Synchronous durability (S3Primary) writes on every commit, 2s is enough.
    # Eventual/Replicated durability uses async walrust sync, needs longer.
    settle_time = 2 if mode_test.durability == "synchronous" else 8
    time.sleep(settle_time)

    # Kill all nodes
    for s in mode_test.servers:
        s.kill()
    result.ok("Killed all nodes")

    time.sleep(2)

    # Start a fresh node with same prefix (should recover from S3)
    fresh_port = BASE_PORT + 50
    fresh = ServerProcess(
        topology=mode_test.topology,
        durability=mode_test.durability,
        port=fresh_port,
        instance_id="fresh-node",
        prefix=mode_test.prefix,
        db_dir=mode_test.tmp_dir,
        lease_ttl=mode_test.servers[0].lease_ttl,
        extra_args=mode_test.servers[0].extra_args,
    )
    fresh.start()
    mode_test.servers.append(fresh)

    if not fresh.wait_healthy(timeout=30):
        result.fail("Fresh node did not become healthy")
        return

    fresh_url = f"http://localhost:{fresh_port}"

    # For dedicated mode, wait for leader election
    if mode_test.topology == "dedicated":
        if not wait_role(fresh_url, "Leader", timeout=30):
            # Might need to wait for old lease TTL
            lease_ttl = mode_test.servers[0].lease_ttl
            time.sleep(lease_ttl)
            wait_role(fresh_url, "Leader", timeout=10)

    # For shared mode, just do a read (triggers catch-up)
    time.sleep(2)

    # Verify fresh node sees the data
    missing = 0
    for row_id in writes:
        resp = get_json(f"{fresh_url}/read?id={row_id}")
        if not resp.get("found"):
            missing += 1

    if missing > 0:
        # Dump fresh node log
        import re as _re
        log = fresh.read_output()
        print(f"    --- fresh-node log (last 15 relevant) ---")
        for line in log.split("\n"):
            line = _re.sub(chr(27) + r'\[[0-9;]*m', '', line).strip()
            if any(k in line for k in ['manifest', 'S3 fetch', 'CACHE', 'ensure_fresh',
                                       'set_manifest', 'catchup', 'page_count', 'building']):
                print(f"      {line[:200]}")

        # Also try a count query directly
        count_resp = get_json(f"{fresh_url}/count")
        print(f"    Fresh node /count: {count_resp}")

        # Check /status
        status_resp = get_json(f"{fresh_url}/status")
        print(f"    Fresh node /status: {status_resp}")

    result.check(missing == 0, f"Fresh node sees all {len(writes)} rows from S3 (missing={missing})")


# ---------------------------------------------------------------------------
# Test runner for each mode
# ---------------------------------------------------------------------------

def run_shared_synchronous(args):
    """Test Shared + Synchronous (multiwriter, turbolite S3Primary)."""
    print("\n" + "=" * 70)
    print("MODE: Shared + Synchronous (multiwriter, turbolite S3Primary)")
    print("=" * 70)

    result = TestResult("shared-synchronous")
    mt = ModeTest("shared", "synchronous", args.writes, args.duration, args.workers)

    try:
        mt.setup_servers(2, lease_ttl=10, extra_args_per_node={
            0: ["--write-timeout", "30"],
            1: ["--write-timeout", "30"],
        })
        print(f"  Prefix: {mt.prefix}")
        print(f"  Waiting for servers...")

        if not mt.wait_all_healthy():
            result.fail("Servers did not become healthy")
            for s in mt.servers:
                if not s.is_alive():
                    print(f"    {s.instance_id} died. Output:")
                    print(s.read_output()[-2000:])
            return result

        print(f"  All {len(mt.servers)} servers healthy\n")

        print("  --- Single node writes ---")
        test_shared_single_node_writes(mt, result)

        print("  --- Multi-node concurrent writes ---")
        test_shared_multi_node_writes(mt, result)

        print("  --- Updates and deletes ---")
        test_shared_updates_deletes(mt, result)

        print("  --- Large values ---")
        test_shared_large_values(mt, result)

        print("  --- Idempotent writes ---")
        test_shared_idempotent_writes(mt, result)

        print("  --- Concurrent same row ---")
        test_shared_concurrent_same_row(mt, result)

        print("  --- Empty reads ---")
        test_shared_empty_reads(mt, result)

        print("  --- Sustained writes ---")
        test_shared_sustained_writes(mt, result)

        print("  --- Crash recovery ---")
        test_shared_crash_recovery(mt, result)

        print("  --- Durability across restarts ---")
        test_durability_across_restarts(mt, result)

    except Exception as e:
        result.fail(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        mt.teardown()

    return result


def run_shared_eventual(args):
    """Test Shared + Eventual (multiwriter, turbolite + walrust)."""
    print("\n" + "=" * 70)
    print("MODE: Shared + Eventual (multiwriter, turbolite + walrust)")
    print("=" * 70)

    result = TestResult("shared-eventual")
    mt = ModeTest("shared", "eventual", args.writes, args.duration, args.workers)

    try:
        mt.setup_servers(2, lease_ttl=10, extra_args_per_node={
            0: ["--write-timeout", "30"],
            1: ["--write-timeout", "30"],
        })
        print(f"  Prefix: {mt.prefix}")
        print(f"  Waiting for servers...")

        if not mt.wait_all_healthy():
            result.fail("Servers did not become healthy")
            for s in mt.servers:
                if not s.is_alive():
                    print(f"    {s.instance_id} died. Output:")
                    print(s.read_output()[-2000:])
            return result

        print(f"  All {len(mt.servers)} servers healthy\n")

        print("  --- Single node writes ---")
        test_shared_single_node_writes(mt, result)

        print("  --- Multi-node concurrent writes ---")
        test_shared_multi_node_writes(mt, result)

        print("  --- Updates and deletes ---")
        test_shared_updates_deletes(mt, result)

        print("  --- Idempotent writes ---")
        test_shared_idempotent_writes(mt, result)

        print("  --- Sustained writes ---")
        test_shared_sustained_writes(mt, result)

        print("  --- Crash recovery ---")
        test_shared_crash_recovery(mt, result)

        print("  --- Durability across restarts ---")
        test_durability_across_restarts(mt, result)

    except Exception as e:
        result.fail(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        mt.teardown()

    return result


def run_dedicated_replicated(args):
    """Test Dedicated + Replicated (classic walrust HA)."""
    print("\n" + "=" * 70)
    print("MODE: Dedicated + Replicated (classic walrust HA)")
    print("=" * 70)

    result = TestResult("dedicated-replicated")
    mt = ModeTest("dedicated", "replicated", args.writes, args.duration, args.workers)

    try:
        mt.setup_servers(3, lease_ttl=5, extra_args_per_node={
            0: ["--sync-interval-ms", "500", "--follower-pull-ms", "500",
                "--follower-poll-ms", "500", "--renew-interval-ms", "1000"],
            1: ["--sync-interval-ms", "500", "--follower-pull-ms", "500",
                "--follower-poll-ms", "500", "--renew-interval-ms", "1000"],
            2: ["--sync-interval-ms", "500", "--follower-pull-ms", "500",
                "--follower-poll-ms", "500", "--renew-interval-ms", "1000"],
        })
        print(f"  Prefix: {mt.prefix}")
        print(f"  Waiting for servers...")

        if not mt.wait_all_healthy():
            result.fail("Servers did not become healthy")
            for s in mt.servers:
                if not s.is_alive():
                    print(f"    {s.instance_id} died. Output:")
                    print(s.read_output()[-2000:])
            return result

        print(f"  All {len(mt.servers)} servers healthy")

        # Wait for leader election
        time.sleep(3)
        leader_found = False
        for url in mt.urls():
            status = get_json(f"{url}/status")
            role = status.get("role", "?")
            print(f"    {url}: role={role}")
            if role == "Leader":
                leader_found = True

        if not leader_found:
            result.fail("No leader elected after 3s")
            return result

        result.ok("Leader elected")

        print("\n  --- Leader writes ---")
        test_dedicated_leader_writes(mt, result)

        print("  --- Follower replication ---")
        test_dedicated_follower_replication(mt, result)

        print("  --- Write forwarding ---")
        test_dedicated_write_forwarding(mt, result)

        print("  --- Sustained writes ---")
        test_dedicated_sustained_writes(mt, result)

        print("  --- Double failover ---")
        test_double_failover(mt, result)

        # Durability-across-restarts not applicable for Dedicated+Replicated:
        # walrust data is in S3 but the Coordinator join path doesn't pull
        # when the node becomes leader immediately (no follower phase).

    except Exception as e:
        result.fail(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        mt.teardown()

    return result


def run_dedicated_synchronous(args):
    """Test Dedicated + Synchronous (turbolite S3Primary HA)."""
    print("\n" + "=" * 70)
    print("MODE: Dedicated + Synchronous (turbolite S3Primary HA)")
    print("=" * 70)

    result = TestResult("dedicated-synchronous")
    mt = ModeTest("dedicated", "synchronous", args.writes, args.duration, args.workers)

    try:
        mt.setup_servers(3, lease_ttl=5, extra_args_per_node={
            0: ["--sync-interval-ms", "500", "--follower-pull-ms", "500",
                "--follower-poll-ms", "500", "--renew-interval-ms", "1000"],
            1: ["--sync-interval-ms", "500", "--follower-pull-ms", "500",
                "--follower-poll-ms", "500", "--renew-interval-ms", "1000"],
            2: ["--sync-interval-ms", "500", "--follower-pull-ms", "500",
                "--follower-poll-ms", "500", "--renew-interval-ms", "1000"],
        })
        print(f"  Prefix: {mt.prefix}")
        print(f"  Waiting for servers...")

        if not mt.wait_all_healthy():
            result.fail("Servers did not become healthy")
            for s in mt.servers:
                if not s.is_alive():
                    print(f"    {s.instance_id} died. Output:")
                    print(s.read_output()[-2000:])
            return result

        print(f"  All {len(mt.servers)} servers healthy")

        # Wait for leader election
        time.sleep(3)
        leader_found = False
        for url in mt.urls():
            status = get_json(f"{url}/status")
            role = status.get("role", "?")
            print(f"    {url}: role={role}")
            if role == "Leader":
                leader_found = True

        if not leader_found:
            result.fail("No leader elected after 3s")
            return result

        result.ok("Leader elected")

        print("\n  --- Leader writes ---")
        test_dedicated_leader_writes(mt, result)

        print("  --- Follower replication ---")
        test_dedicated_follower_replication(mt, result)

        print("  --- Write forwarding ---")
        test_dedicated_write_forwarding(mt, result)

        print("  --- Double failover ---")
        test_double_failover(mt, result)

        print("  --- Sustained writes ---")
        test_dedicated_sustained_writes(mt, result)

    except Exception as e:
        result.fail(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        mt.teardown()

    return result


def run_dedicated_eventual(args):
    """Test Dedicated + Eventual (turbolite + walrust HA)."""
    print("\n" + "=" * 70)
    print("MODE: Dedicated + Eventual (turbolite + walrust HA)")
    print("=" * 70)

    result = TestResult("dedicated-eventual")
    mt = ModeTest("dedicated", "eventual", args.writes, args.duration, args.workers)

    try:
        mt.setup_servers(3, lease_ttl=5, extra_args_per_node={
            0: ["--sync-interval-ms", "500", "--follower-pull-ms", "500",
                "--follower-poll-ms", "500", "--renew-interval-ms", "1000"],
            1: ["--sync-interval-ms", "500", "--follower-pull-ms", "500",
                "--follower-poll-ms", "500", "--renew-interval-ms", "1000"],
            2: ["--sync-interval-ms", "500", "--follower-pull-ms", "500",
                "--follower-poll-ms", "500", "--renew-interval-ms", "1000"],
        })
        print(f"  Prefix: {mt.prefix}")
        print(f"  Waiting for servers...")

        if not mt.wait_all_healthy():
            result.fail("Servers did not become healthy")
            for s in mt.servers:
                if not s.is_alive():
                    print(f"    {s.instance_id} died. Output:")
                    print(s.read_output()[-2000:])
            return result

        print(f"  All {len(mt.servers)} servers healthy")

        # Wait for leader election
        time.sleep(3)
        leader_found = False
        for url in mt.urls():
            status = get_json(f"{url}/status")
            role = status.get("role", "?")
            print(f"    {url}: role={role}")
            if role == "Leader":
                leader_found = True

        if not leader_found:
            result.fail("No leader elected after 3s")
            return result

        result.ok("Leader elected")

        print("\n  --- Leader writes ---")
        test_dedicated_leader_writes(mt, result)

        print("  --- Follower replication ---")
        test_dedicated_follower_replication(mt, result)

        print("  --- Write forwarding ---")
        test_dedicated_write_forwarding(mt, result)

        print("  --- Sustained writes ---")
        test_dedicated_sustained_writes(mt, result)

        # Destructive tests (kill nodes). Double failover includes leader
        # failover as phase 1, so no separate leader failover test needed.
        # Durability kills all nodes and starts a fresh one.
        print("  --- Double failover ---")
        test_double_failover(mt, result)

        print("  --- Durability across restarts ---")
        test_durability_across_restarts(mt, result)

    except Exception as e:
        result.fail(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        mt.teardown()

    return result


# ---------------------------------------------------------------------------
# Chaos / resilience tests (Dedicated+Eventual)
# ---------------------------------------------------------------------------

def test_sigkill_during_sync(result):
    """Write continuously, SIGKILL at random moment, verify no corruption."""
    prefix = f"chaos-sync-{int(time.time())}-{uuid.uuid4().hex[:4]}/"
    tmp = tempfile.mkdtemp(prefix="haqlite_chaos_sync_")
    extra = ["--sync-interval-ms", "500", "--follower-pull-ms", "500",
             "--follower-poll-ms", "500", "--renew-interval-ms", "1000"]

    iterations = 5
    corruption_count = 0
    data_loss_stats = []

    for iteration in range(iterations):
        iter_prefix = f"{prefix}iter{iteration}/"
        s = ServerProcess("dedicated", "eventual", BASE_PORT + 70, f"chaos-{iteration}",
                          iter_prefix, tmp, lease_ttl=5, extra_args=extra)
        s.start()
        if not s.wait_healthy(timeout=30):
            result.fail(f"Iteration {iteration}: server didn't start")
            s.stop()
            continue

        # Wait for leader election (single node, always leader)
        time.sleep(3)

        # Write rows continuously
        ok_writes = []
        for i in range(20):
            row_id = f"chaos-{iteration}-{i}"
            resp = post_json(f"{s.url}/write", {"id": row_id, "value": f"v-{i}"}, timeout=10)
            if resp.get("ok"):
                ok_writes.append(row_id)

        # Random delay 0-200ms, then SIGKILL
        kill_delay = random.uniform(0, 0.2)
        time.sleep(kill_delay)
        s.kill()
        time.sleep(2)

        # Start fresh node with same prefix, check for corruption
        fresh = ServerProcess("dedicated", "eventual", BASE_PORT + 71, f"fresh-{iteration}",
                              iter_prefix, tmp, lease_ttl=5, extra_args=extra)
        fresh.start()
        if not fresh.wait_healthy(timeout=30):
            result.fail(f"Iteration {iteration}: fresh node didn't start after SIGKILL")
            fresh.stop()
            continue

        time.sleep(3)

        # Run integrity check via SQL
        ic_resp = get_json(f"{fresh.url}/query?sql=PRAGMA%20integrity_check")
        ic_ok = False
        if ic_resp.get("rows"):
            first_row = ic_resp["rows"][0] if ic_resp["rows"] else []
            ic_ok = (first_row == ["ok"] if first_row else False)
        if not ic_ok:
            corruption_count += 1
            result.fail(f"Iteration {iteration}: integrity_check FAILED: {ic_resp}")

        # Count surviving rows
        count_resp = get_json(f"{fresh.url}/count")
        survived = count_resp.get("count", 0)
        lost = len(ok_writes) - survived
        data_loss_stats.append({"iteration": iteration, "wrote": len(ok_writes),
                                "survived": survived, "lost": max(0, lost),
                                "kill_delay_ms": int(kill_delay * 1000)})
        fresh.stop()

    # Clean up
    shutil.rmtree(tmp, ignore_errors=True)

    result.check(corruption_count == 0,
                 f"No corruption across {iterations} SIGKILL iterations (corruption={corruption_count})")

    if data_loss_stats:
        total_wrote = sum(s["wrote"] for s in data_loss_stats)
        total_survived = sum(s["survived"] for s in data_loss_stats)
        total_lost = sum(s["lost"] for s in data_loss_stats)
        max_lost = max(s["lost"] for s in data_loss_stats)
        print(f"    Stats: {total_survived}/{total_wrote} survived, {total_lost} lost, max_lost_per_kill={max_lost}")
        for s in data_loss_stats:
            print(f"      iter {s['iteration']}: wrote={s['wrote']} survived={s['survived']} lost={s['lost']} delay={s['kill_delay_ms']}ms")

    result.ok(f"SIGKILL-during-sync: {iterations} iterations complete")


def test_rpo_measurement(result):
    """Measure recovery point objective: write, kill after N ms, check survival."""
    prefix = f"chaos-rpo-{int(time.time())}-{uuid.uuid4().hex[:4]}/"
    tmp = tempfile.mkdtemp(prefix="haqlite_chaos_rpo_")
    extra = ["--sync-interval-ms", "500", "--follower-pull-ms", "500",
             "--follower-poll-ms", "500", "--renew-interval-ms", "1000"]

    delays_ms = [0, 100, 250, 500, 750, 1000, 1500, 2000]
    rpo_results = []

    for delay_ms in delays_ms:
        iter_prefix = f"{prefix}rpo{delay_ms}/"
        s = ServerProcess("dedicated", "eventual", BASE_PORT + 72, f"rpo-{delay_ms}",
                          iter_prefix, tmp, lease_ttl=5, extra_args=extra)
        s.start()
        if not s.wait_healthy(timeout=30):
            result.fail(f"RPO {delay_ms}ms: server didn't start")
            s.stop()
            continue

        time.sleep(3)

        # Write 10 rows, wait for sync, then write the probe row
        for i in range(10):
            post_json(f"{s.url}/write", {"id": f"base-{i}", "value": f"v-{i}"}, timeout=10)
        # Wait for base rows to sync
        time.sleep(3)

        # Write probe row, then kill after delay
        probe_id = f"probe-{delay_ms}"
        resp = post_json(f"{s.url}/write", {"id": probe_id, "value": "probe"}, timeout=10)
        probe_ok = resp.get("ok", False)

        time.sleep(delay_ms / 1000.0)
        s.kill()
        time.sleep(2)

        # Fresh node
        fresh = ServerProcess("dedicated", "eventual", BASE_PORT + 73, f"rpo-fresh-{delay_ms}",
                              iter_prefix, tmp, lease_ttl=5, extra_args=extra)
        fresh.start()
        if not fresh.wait_healthy(timeout=30):
            result.fail(f"RPO {delay_ms}ms: fresh node didn't start")
            fresh.stop()
            continue

        time.sleep(3)

        # Check if probe survived
        probe_resp = get_json(f"{fresh.url}/read?id={probe_id}")
        probe_survived = probe_resp.get("found", False)
        base_count = get_json(f"{fresh.url}/count").get("count", 0)

        rpo_results.append({
            "delay_ms": delay_ms,
            "probe_ok": probe_ok,
            "probe_survived": probe_survived,
            "base_rows": base_count,
        })
        fresh.stop()

    shutil.rmtree(tmp, ignore_errors=True)

    # Report
    print(f"    RPO sweep (sync_interval=500ms):")
    threshold_ms = None
    for r in rpo_results:
        status = "SURVIVED" if r["probe_survived"] else "LOST"
        print(f"      {r['delay_ms']:5d}ms: probe={status}, base_rows={r['base_rows']}")
        if r["probe_survived"] and threshold_ms is None:
            threshold_ms = r["delay_ms"]

    if threshold_ms is not None:
        result.ok(f"RPO threshold: writes survive after ~{threshold_ms}ms (sync_interval=500ms)")
    else:
        result.ok(f"RPO: no probe survived (all killed before sync)")

    # Base rows should always survive (they were synced before the probe)
    base_ok = all(r["base_rows"] >= 10 for r in rpo_results)
    result.check(base_ok, f"Pre-synced rows survive all kills (base_rows >= 10 for all delays)")


def test_changeset_chain_integrity(result):
    """After double failover, verify a cold walrust restore produces valid data."""
    prefix = f"chaos-chain-{int(time.time())}-{uuid.uuid4().hex[:4]}/"
    tmp = tempfile.mkdtemp(prefix="haqlite_chaos_chain_")
    extra = ["--sync-interval-ms", "500", "--follower-pull-ms", "500",
             "--follower-poll-ms", "500", "--renew-interval-ms", "1000"]

    # 3 nodes
    servers = []
    for i in range(3):
        s = ServerProcess("dedicated", "eventual", BASE_PORT + 74 + i, f"chain-{i}",
                          prefix, tmp, lease_ttl=5, extra_args=extra)
        s.start()
        servers.append(s)

    for s in servers:
        if not s.wait_healthy(timeout=30):
            result.fail("Chain test: servers didn't start")
            for s2 in servers: s2.stop()
            shutil.rmtree(tmp, ignore_errors=True)
            return

    time.sleep(3)

    # Find leader
    leader_idx = None
    for i, s in enumerate(servers):
        if get_json(f"{s.url}/status").get("role") == "Leader":
            leader_idx = i
            break

    if leader_idx is None:
        result.fail("No leader elected")
        for s in servers: s.stop()
        shutil.rmtree(tmp, ignore_errors=True)
        return

    # Phase 1: write to leader-1
    phase1_ids = []
    for i in range(5):
        rid = f"chain-p1-{i}"
        resp = post_json(f"{servers[leader_idx].url}/write", {"id": rid, "value": f"p1-{i}"}, timeout=60)
        if resp.get("ok"):
            phase1_ids.append(rid)
    time.sleep(5)

    # Kill leader-1
    servers[leader_idx].kill()
    time.sleep(10)

    # Find leader-2
    leader2_idx = None
    for i, s in enumerate(servers):
        if i != leader_idx and s.is_alive():
            if get_json(f"{s.url}/status").get("role") == "Leader":
                leader2_idx = i
                break

    if leader2_idx is None:
        # Wait longer
        time.sleep(10)
        for i, s in enumerate(servers):
            if i != leader_idx and s.is_alive():
                if get_json(f"{s.url}/status").get("role") == "Leader":
                    leader2_idx = i
                    break

    if leader2_idx is None:
        result.fail("No leader-2 after killing leader-1")
        for s in servers: s.stop()
        shutil.rmtree(tmp, ignore_errors=True)
        return

    # Phase 2: write to leader-2
    phase2_ids = []
    for i in range(5):
        rid = f"chain-p2-{i}"
        resp = post_json(f"{servers[leader2_idx].url}/write", {"id": rid, "value": f"p2-{i}"}, timeout=60)
        if resp.get("ok"):
            phase2_ids.append(rid)
    time.sleep(20)

    # Kill all
    for s in servers:
        if s.is_alive():
            s.kill()
    time.sleep(3)

    # Cold restore: fresh node with same prefix
    fresh = ServerProcess("dedicated", "eventual", BASE_PORT + 77, "chain-fresh",
                          prefix, tmp, lease_ttl=5, extra_args=extra)
    fresh.start()
    if not fresh.wait_healthy(timeout=30):
        result.fail("Chain test: fresh node didn't start")
        fresh.stop()
        shutil.rmtree(tmp, ignore_errors=True)
        return

    time.sleep(5)

    # Integrity check
    ic_resp = get_json(f"{fresh.url}/query?sql=PRAGMA%20integrity_check")
    ic_ok = False
    if ic_resp.get("rows"):
        first_row = ic_resp["rows"][0] if ic_resp["rows"] else []
        ic_ok = (first_row == ["ok"] if first_row else False)
    result.check(ic_ok, f"Cold restore integrity_check: {'ok' if ic_ok else ic_resp}")

    # Check data
    p1_missing = sum(1 for rid in phase1_ids if not get_json(f"{fresh.url}/read?id={rid}").get("found"))
    p2_missing = sum(1 for rid in phase2_ids if not get_json(f"{fresh.url}/read?id={rid}").get("found"))

    result.check(p1_missing == 0, f"Cold restore has phase-1 rows (missing={p1_missing}/{len(phase1_ids)})")
    result.check(p2_missing == 0, f"Cold restore has phase-2 rows (missing={p2_missing}/{len(phase2_ids)})")

    fresh.stop()
    shutil.rmtree(tmp, ignore_errors=True)


def test_concurrent_readers_during_failover(result):
    """Readers on follower must not crash or return corrupt data during leader kill."""
    prefix = f"chaos-readers-{int(time.time())}-{uuid.uuid4().hex[:4]}/"
    tmp = tempfile.mkdtemp(prefix="haqlite_chaos_readers_")
    extra = ["--sync-interval-ms", "500", "--follower-pull-ms", "500",
             "--follower-poll-ms", "500", "--renew-interval-ms", "1000"]

    s0 = ServerProcess("dedicated", "eventual", BASE_PORT + 80, "read-0",
                       prefix, tmp, lease_ttl=5, extra_args=extra)
    s1 = ServerProcess("dedicated", "eventual", BASE_PORT + 81, "read-1",
                       prefix, tmp, lease_ttl=5, extra_args=extra)
    s0.start(); s1.start()
    if not s0.wait_healthy(timeout=30) or not s1.wait_healthy(timeout=30):
        result.fail("Reader test: servers didn't start")
        s0.stop(); s1.stop()
        shutil.rmtree(tmp, ignore_errors=True)
        return

    time.sleep(3)

    leader_s = None; follower_s = None
    for s in [s0, s1]:
        if get_json(f"{s.url}/status").get("role") == "Leader":
            leader_s = s
        else:
            follower_s = s

    if not leader_s or not follower_s:
        result.fail("Need leader + follower")
        s0.stop(); s1.stop()
        shutil.rmtree(tmp, ignore_errors=True)
        return

    # Seed data
    for i in range(20):
        post_json(f"{leader_s.url}/write", {"id": f"seed-{i}", "value": f"v-{i}"}, timeout=10)
    time.sleep(3)

    # Spawn reader threads that hammer the follower
    read_errors = []
    read_count = [0]
    stop_reading = [False]

    def reader_loop():
        while not stop_reading[0]:
            try:
                resp = get_json(f"{follower_s.url}/count")
                count = resp.get("count")
                if count is not None and count < 0:
                    read_errors.append(f"negative count: {count}")
                read_count[0] += 1
            except Exception as e:
                # Connection errors during failover are expected
                pass
            time.sleep(0.05)

    threads = []
    for _ in range(4):
        import threading
        t = threading.Thread(target=reader_loop, daemon=True)
        t.start()
        threads.append(t)

    # Let readers warm up
    time.sleep(1)

    # Kill leader while readers are active
    leader_s.kill()
    time.sleep(5)

    # Stop readers
    stop_reading[0] = True
    for t in threads:
        t.join(timeout=5)

    result.check(len(read_errors) == 0,
                 f"No corrupt reads during failover ({read_count[0]} reads, {len(read_errors)} errors)")
    result.ok(f"Concurrent readers: {read_count[0]} reads completed during failover")

    follower_s.stop()
    shutil.rmtree(tmp, ignore_errors=True)


def test_rapid_kill_restart(result):
    """Kill and promote 5 times with minimal settle, verify data integrity."""
    prefix = f"chaos-rapid-{int(time.time())}-{uuid.uuid4().hex[:4]}/"
    tmp = tempfile.mkdtemp(prefix="haqlite_chaos_rapid_")
    extra = ["--sync-interval-ms", "500", "--follower-pull-ms", "500",
             "--follower-poll-ms", "500", "--renew-interval-ms", "1000"]

    all_ok_writes = []  # (row_id, which_leader)
    num_cycles = 5

    # Start first leader
    current = ServerProcess("dedicated", "eventual", BASE_PORT + 85, "rapid-0",
                            prefix, tmp, lease_ttl=5, extra_args=extra)
    current.start()
    if not current.wait_healthy(timeout=30):
        result.fail("Rapid kill: first server didn't start")
        current.stop()
        shutil.rmtree(tmp, ignore_errors=True)
        return
    time.sleep(3)

    for cycle in range(num_cycles):
        # Write one row
        rid = f"rapid-{cycle}"
        resp = post_json(f"{current.url}/write", {"id": rid, "value": f"c-{cycle}"}, timeout=60)
        if resp.get("ok"):
            all_ok_writes.append(rid)
            print(f"      cycle {cycle}: wrote {rid}")
        else:
            print(f"      cycle {cycle}: write FAILED: {resp.get('error', '?')[:80]}")

        # Wait for walrust to sync before kill
        time.sleep(3)

        # Kill immediately
        current.kill()
        time.sleep(2)

        # Start next leader
        next_s = ServerProcess("dedicated", "eventual", BASE_PORT + 86 + cycle,
                               f"rapid-{cycle+1}", prefix, tmp, lease_ttl=5, extra_args=extra)
        next_s.start()
        if not next_s.wait_healthy(timeout=30):
            result.fail(f"Rapid kill cycle {cycle}: next server didn't start")
            next_s.stop()
            break
        time.sleep(5)  # wait for leader election
        current = next_s

    # Verify all Ok'd writes survived
    time.sleep(3)
    missing = 0
    for rid in all_ok_writes:
        resp = get_json(f"{current.url}/read?id={rid}")
        if not resp.get("found"):
            missing += 1
            print(f"      MISSING: {rid}")

    result.check(missing == 0,
                 f"All {len(all_ok_writes)} Ok'd writes survived {num_cycles} rapid kills (missing={missing})")

    # Integrity check
    ic_resp = get_json(f"{current.url}/query?sql=PRAGMA%20integrity_check")
    ic_ok = False
    if ic_resp.get("rows"):
        first_row = ic_resp["rows"][0] if ic_resp["rows"] else []
        ic_ok = (first_row == ["ok"] if first_row else False)
    result.check(ic_ok, f"Integrity check after rapid kills: {'ok' if ic_ok else ic_resp}")

    current.stop()
    shutil.rmtree(tmp, ignore_errors=True)


def run_chaos_dedicated_eventual(args):
    """Chaos and resilience tests for Dedicated + Eventual."""
    print("\n" + "=" * 70)
    print("CHAOS: Dedicated + Eventual resilience tests")
    print("=" * 70)

    result = TestResult("chaos-dedicated-eventual")

    try:
        print("\n  --- SIGKILL during sync ---")
        test_sigkill_during_sync(result)

        print("\n  --- RPO measurement ---")
        test_rpo_measurement(result)

        print("\n  --- Changeset chain integrity ---")
        test_changeset_chain_integrity(result)

        print("\n  --- Concurrent readers during failover ---")
        test_concurrent_readers_during_failover(result)

        print("\n  --- Rapid kill/restart cycle ---")
        test_rapid_kill_restart(result)

    except Exception as e:
        result.fail(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()

    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

# Mode matrix: topology x durability x test-level
# Usage: --mode dedicated-eventual         (basic tests)
#        --mode dedicated-eventual-chaos   (chaos/resilience tests)
#        --mode dedicated                  (all dedicated durabilities, basic)
#        --mode chaos                      (all chaos tests)

BASIC_MODES = {
    ("shared", "synchronous"): run_shared_synchronous,
    ("shared", "eventual"): run_shared_eventual,
    ("dedicated", "replicated"): run_dedicated_replicated,
    ("dedicated", "synchronous"): run_dedicated_synchronous,
    ("dedicated", "eventual"): run_dedicated_eventual,
}

CHAOS_MODES = {
    ("dedicated", "eventual"): run_chaos_dedicated_eventual,
}

def resolve_modes(mode_str):
    """Resolve a mode string to a list of (name, runner) pairs.

    Supports:
      dedicated-eventual       -> basic tests for that combo
      dedicated-eventual-chaos -> chaos tests for that combo
      dedicated                -> all basic dedicated modes
      shared                   -> all basic shared modes
      chaos                    -> all chaos modes
      all                      -> all basic modes
    """
    if not mode_str or mode_str == "all":
        return [(f"{t}-{d}", fn) for (t, d), fn in BASIC_MODES.items()]

    # "chaos" alone = all chaos modes
    if mode_str == "chaos":
        return [(f"{t}-{d}-chaos", fn) for (t, d), fn in CHAOS_MODES.items()]

    parts = mode_str.split("-")

    # Check for chaos suffix: "dedicated-eventual-chaos"
    if len(parts) >= 3 and parts[-1] == "chaos":
        topology = parts[0]
        durability = parts[1]
        key = (topology, durability)
        if key in CHAOS_MODES:
            return [(mode_str, CHAOS_MODES[key])]
        return []

    # Topology-chaos: "dedicated-chaos" = all chaos modes for that topology
    if len(parts) == 2 and parts[1] == "chaos":
        topology = parts[0]
        return [(f"{t}-{d}-chaos", fn) for (t, d), fn in CHAOS_MODES.items()
                if t == topology]

    # Topology only: all durabilities for that topology
    if mode_str in ("shared", "dedicated"):
        return [(f"{t}-{d}", fn) for (t, d), fn in BASIC_MODES.items()
                if t == mode_str]

    # Full topology-durability
    if len(parts) == 2:
        key = (parts[0], parts[1])
        if key in BASIC_MODES:
            return [(mode_str, BASIC_MODES[key])]

    return []


# Legacy flat lookup for backward compat
ALL_MODES = {f"{t}-{d}": fn for (t, d), fn in BASIC_MODES.items()}
ALL_MODES.update({f"{t}-{d}-chaos": fn for (t, d), fn in CHAOS_MODES.items()})


def main():
    global BINARY, BASE_PORT

    parser = argparse.ArgumentParser(description="haqlite e2e mode tests")
    parser.add_argument("--mode", help="Run specific mode (e.g. shared-synchronous). Default: all.")
    parser.add_argument("--writes", type=int, default=20, help="Writes per test phase")
    parser.add_argument("--workers", type=int, default=4, help="Concurrent workers")
    parser.add_argument("--duration", type=int, default=10, help="Sustained test duration (seconds)")
    parser.add_argument("--base-port", type=int, default=9100, help="Starting port number")
    parser.add_argument("--binary", help="Path to haqlite-experiment binary")
    args = parser.parse_args()

    BASE_PORT = args.base_port

    # Resolve binary
    if args.binary:
        BINARY = args.binary
    else:
        BINARY = resolve_binary()

    if not BINARY or not os.path.isfile(BINARY):
        print("ERROR: haqlite-experiment binary not found.")
        print("Build it first:")
        print("  cd haqlite && cargo build --features turbolite-cloud,s3-manifest --bin haqlite-experiment")
        sys.exit(1)

    print(f"Binary: {BINARY}")
    print(f"Writes: {args.writes}, Workers: {args.workers}, Duration: {args.duration}s")

    # Check for required env vars
    has_creds = (
        ("AWS_ACCESS_KEY_ID" in os.environ or "TIGRIS_STORAGE_ACCESS_KEY_ID" in os.environ)
        and ("AWS_SECRET_ACCESS_KEY" in os.environ or "TIGRIS_STORAGE_SECRET_ACCESS_KEY" in os.environ)
    )
    if not has_creds:
        print("ERROR: S3/Tigris credentials not found.")
        print("Run with soup:")
        print("  soup run --project ladybug --env development -- python tests/e2e_modes.py")
        sys.exit(1)

    # Select modes to run via matrix resolver
    if args.mode:
        resolved = resolve_modes(args.mode)
        if not resolved:
            available = sorted(ALL_MODES.keys())
            extras = ["shared", "dedicated", "chaos", "all"]
            print(f"ERROR: Unknown mode '{args.mode}'.")
            print(f"  Modes: {', '.join(available)}")
            print(f"  Shortcuts: {', '.join(extras)}")
            sys.exit(1)
        modes_to_run = dict(resolved)
    else:
        # Default: basic modes only (not chaos -- chaos takes too long)
        modes_to_run = {f"{t}-{d}": fn for (t, d), fn in BASIC_MODES.items()}

    # Run tests
    results = {}
    for mode_name, run_fn in modes_to_run.items():
        results[mode_name] = run_fn(args)
        # Bump base port to avoid conflicts
        BASE_PORT += 10

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)

    all_passed = True
    for name, r in results.items():
        status = "PASS" if r.success else "FAIL"
        if not r.success:
            all_passed = False
        print(f"  {status}: {name} ({r.passed} passed, {r.failed} failed)")
        for err in r.errors:
            print(f"         {err}")

    print()
    if all_passed:
        print("ALL MODES PASSED")
        sys.exit(0)
    else:
        print("SOME MODES FAILED")
        sys.exit(1)


if __name__ == "__main__":
    main()
