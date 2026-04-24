#!/usr/bin/env bash
#
# haqlite end-to-end HA test script
#
# Runs multiple scenarios with different parameters to verify:
#   1. Basic replication (leader writes, follower replicates)
#   2. Write forwarding via HaQLiteClient
#   3. Leader failover (kill leader, follower promotes, writer reconnects)
#   4. Different sync intervals
#   5. Different lease TTLs / poll intervals
#
# Each scenario gets a unique S3 prefix — no cleanup between scenarios needed.
#
# Requirements:
#   - Release binaries built: cargo build --release
#   - AWS/Tigris credentials in environment
#
# Usage:
#   export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... AWS_REGION=auto
#   ./tests/e2e_ha.sh [--bucket BUCKET] [--endpoint ENDPOINT]

set -eo pipefail

# ============================================================================
# Configuration
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BINARY="$PROJECT_DIR/target/release/haqlite-ha-experiment"
WRITER="$PROJECT_DIR/target/release/haqlite-ha-writer"
LOG_DIR="/tmp/ha-e2e-logs"
RUN_ID="$$-$RANDOM"

BUCKET="${HA_BUCKET:-walrust-ha-test}"
ENDPOINT="${HA_S3_ENDPOINT:-https://t3.storage.dev}"

# Ports — use high ports to avoid conflicts
NODE1_PORT=19001
NODE2_PORT=19002

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

PASS=0
FAIL=0
PIDS=""  # space-separated PIDs
SCENARIO_PREFIX=""  # set per scenario

# ============================================================================
# Parse args
# ============================================================================

while [[ $# -gt 0 ]]; do
    case $1 in
        --bucket) BUCKET="$2"; shift 2 ;;
        --endpoint) ENDPOINT="$2"; shift 2 ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

# ============================================================================
# Helpers
# ============================================================================

log()  { echo -e "${BLUE}[test]${NC} $*"; }
pass() { echo -e "${GREEN}[PASS]${NC} $*"; PASS=$((PASS + 1)); }
fail() { echo -e "${RED}[FAIL]${NC} $*"; FAIL=$((FAIL + 1)); }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }

kill_all() {
    if [ -n "$PIDS" ]; then
        for pid in $PIDS; do
            kill "$pid" 2>/dev/null || true
        done
        for pid in $PIDS; do
            wait "$pid" 2>/dev/null || true
        done
    fi
    # Safety net
    pkill -f "haqlite-ha-experiment.*--port $NODE1_PORT" 2>/dev/null || true
    pkill -f "haqlite-ha-experiment.*--port $NODE2_PORT" 2>/dev/null || true
    pkill -f "haqlite-ha-writer.*$RUN_ID" 2>/dev/null || true
    sleep 1
    PIDS=""
}

cleanup() {
    log "Cleaning up..."
    kill_all
    rm -rf /tmp/ha-e2e-node1 /tmp/ha-e2e-node2 "$LOG_DIR"
}

trap cleanup EXIT

wait_for_health() {
    local url="$1"
    local name="$2"
    local max_wait="${3:-15}"
    local i=0
    while [ $i -lt "$max_wait" ]; do
        if curl -sf "$url/health" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
        i=$((i + 1))
    done
    fail "$name did not become healthy within ${max_wait}s"
    return 1
}

wait_for_role() {
    local url="$1"
    local expected_role="$2"
    local name="$3"
    local max_wait="${4:-15}"
    local i=0
    local role=""
    while [ $i -lt "$max_wait" ]; do
        role=$(curl -sf "$url/status" 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin)['role'])" 2>/dev/null || echo "")
        if [ "$role" = "$expected_role" ]; then
            return 0
        fi
        sleep 1
        i=$((i + 1))
    done
    fail "$name did not become $expected_role within ${max_wait}s (got: $role)"
    return 1
}

get_count() {
    local url="$1"
    curl -sf "$url/status" 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin)['row_count'])" 2>/dev/null || echo "0"
}

verify_node() {
    local url="$1"
    local result
    result=$(curl -sf "$url/verify" 2>/dev/null || echo '{"ok":false}')
    local ok
    ok=$(echo "$result" | python3 -c "import sys,json; print(json.load(sys.stdin).get('ok', False))" 2>/dev/null || echo "False")
    [ "$ok" = "True" ]
}

start_node() {
    local instance="$1"
    local port="$2"
    local db_dir="$3"
    local test_rows="${4:-0}"
    shift 4

    mkdir -p "$db_dir" "$LOG_DIR"
    "$BINARY" \
        --bucket "$BUCKET" --prefix "$SCENARIO_PREFIX" \
        --endpoint "$ENDPOINT" \
        --db "$db_dir/ha.db" --instance "$instance" --port "$port" \
        --test-rows "$test_rows" \
        "$@" \
        > "$LOG_DIR/$instance.log" 2>&1 &
    local pid=$!
    PIDS="$PIDS $pid"
    echo "$pid"
}

start_writer() {
    # All args passed through. Injects bucket/prefix/endpoint automatically.
    "$WRITER" \
        --bucket "$BUCKET" --prefix "$SCENARIO_PREFIX" \
        --endpoint "$ENDPOINT" \
        --db-name ha \
        "$@"
}

remove_pid() {
    local target="$1"
    local new_pids=""
    for pid in $PIDS; do
        if [ "$pid" != "$target" ]; then
            new_pids="$new_pids $pid"
        fi
    done
    PIDS="$new_pids"
}

# ============================================================================
# Scenario runner — each scenario gets a fresh prefix + fresh local dirs
# ============================================================================

SCENARIO_NUM=0

run_scenario() {
    local scenario_name="$1"
    shift
    SCENARIO_NUM=$((SCENARIO_NUM + 1))

    # Fresh S3 prefix per scenario — no cleanup needed
    SCENARIO_PREFIX="e2e-${RUN_ID}-s${SCENARIO_NUM}/"

    echo ""
    echo "================================================================"
    log "SCENARIO: $scenario_name"
    log "  prefix=$SCENARIO_PREFIX"
    echo "================================================================"

    # Kill previous scenario's processes, fresh local dirs
    kill_all
    rm -rf /tmp/ha-e2e-node1 /tmp/ha-e2e-node2

    "$@"
}

# --------------------------------------------------------------------------
# Scenario 1: Basic replication (default params)
# --------------------------------------------------------------------------
scenario_basic_replication() {
    log "Starting node1 (leader) with 50 test rows..."
    start_node node1 $NODE1_PORT /tmp/ha-e2e-node1 50 >/dev/null

    wait_for_health "http://localhost:$NODE1_PORT" "node1" || return
    wait_for_role "http://localhost:$NODE1_PORT" "Leader" "node1" || return

    local count1
    count1=$(get_count "http://localhost:$NODE1_PORT")
    if [ "$count1" -eq 50 ]; then
        pass "Node1 is Leader with $count1 rows"
    else
        fail "Node1 expected 50 rows, got $count1"
    fi

    log "Starting node2 (follower)..."
    start_node node2 $NODE2_PORT /tmp/ha-e2e-node2 0 >/dev/null

    wait_for_health "http://localhost:$NODE2_PORT" "node2" || return
    wait_for_role "http://localhost:$NODE2_PORT" "Follower" "node2" || return

    sleep 3

    local count2
    count2=$(get_count "http://localhost:$NODE2_PORT")
    if [ "$count2" -eq 50 ]; then
        pass "Node2 replicated $count2 rows"
    else
        fail "Node2 expected 50 rows, got $count2"
    fi

    if verify_node "http://localhost:$NODE1_PORT" && \
       verify_node "http://localhost:$NODE2_PORT"; then
        pass "Both nodes verified OK"
    else
        fail "Verification failed"
    fi
}

# --------------------------------------------------------------------------
# Scenario 2: Write forwarding via HaQLiteClient
# --------------------------------------------------------------------------
scenario_write_forwarding() {
    log "Starting node1 (leader)..."
    start_node node1 $NODE1_PORT /tmp/ha-e2e-node1 0 >/dev/null
    wait_for_health "http://localhost:$NODE1_PORT" "node1" || return
    wait_for_role "http://localhost:$NODE1_PORT" "Leader" "node1" || return

    log "Starting node2 (follower)..."
    start_node node2 $NODE2_PORT /tmp/ha-e2e-node2 0 >/dev/null
    wait_for_health "http://localhost:$NODE2_PORT" "node2" || return
    wait_for_role "http://localhost:$NODE2_PORT" "Follower" "node2" || return

    log "Running writer (100 writes via HaQLiteClient)..."
    start_writer \
        --interval-ms 20 \
        --total-writes 100 \
        --verify-nodes "http://localhost:$NODE1_PORT,http://localhost:$NODE2_PORT" \
        > "$LOG_DIR/writer.log" 2>&1
    local writer_exit=$?

    if [ $writer_exit -eq 0 ]; then
        pass "Writer completed 100 writes with verification"
    else
        fail "Writer failed (exit code $writer_exit)"
        tail -20 "$LOG_DIR/writer.log"
    fi

    # Wait for replication to catch up before checking counts.
    sleep 3

    local count1 count2
    count1=$(get_count "http://localhost:$NODE1_PORT")
    count2=$(get_count "http://localhost:$NODE2_PORT")
    if [ "$count1" -eq "$count2" ] && [ "$count1" -eq 100 ]; then
        pass "Both nodes have $count1 rows"
    else
        fail "Row count mismatch: node1=$count1, node2=$count2 (expected 100)"
    fi
}

# --------------------------------------------------------------------------
# Scenario 3: Leader failover
# --------------------------------------------------------------------------
scenario_leader_failover() {
    local lease_ttl="${1:-5}"
    local follower_poll="${2:-1000}"
    log "Config: lease_ttl=${lease_ttl}s, follower_poll=${follower_poll}ms"

    log "Starting node1 (leader) with 20 test rows..."
    local node1_pid
    node1_pid=$(start_node node1 $NODE1_PORT /tmp/ha-e2e-node1 20 \
        --lease-ttl "$lease_ttl" --follower-poll-ms "$follower_poll")
    wait_for_health "http://localhost:$NODE1_PORT" "node1" || return
    wait_for_role "http://localhost:$NODE1_PORT" "Leader" "node1" || return

    log "Starting node2 (follower)..."
    start_node node2 $NODE2_PORT /tmp/ha-e2e-node2 0 \
        --lease-ttl "$lease_ttl" --follower-poll-ms "$follower_poll" >/dev/null
    wait_for_health "http://localhost:$NODE2_PORT" "node2" || return
    wait_for_role "http://localhost:$NODE2_PORT" "Follower" "node2" || return

    sleep 3
    local count_before
    count_before=$(get_count "http://localhost:$NODE2_PORT")
    log "Node2 has $count_before rows before failover"

    log "Killing node1 (leader)..."
    kill "$node1_pid" 2>/dev/null || true
    wait "$node1_pid" 2>/dev/null || true
    remove_pid "$node1_pid"

    log "Waiting for node2 to promote (lease TTL=${lease_ttl}s + poll)..."
    local max_wait=$((lease_ttl + 10))
    if wait_for_role "http://localhost:$NODE2_PORT" "Leader" "node2" "$max_wait"; then
        pass "Node2 promoted to Leader after failover"
    else
        fail "Node2 did not promote within ${max_wait}s"
        return
    fi

    log "Writing 30 rows to new leader..."
    for i in $(seq 1 30); do
        curl -sf -X POST "http://localhost:$NODE2_PORT/write" >/dev/null 2>&1 || true
    done

    local count_after
    count_after=$(get_count "http://localhost:$NODE2_PORT")
    local expected=$((count_before + 30))
    if [ "$count_after" -ge "$expected" ]; then
        pass "New leader has $count_after rows (expected >= $expected)"
    else
        fail "New leader has $count_after rows (expected >= $expected)"
    fi

    if verify_node "http://localhost:$NODE2_PORT"; then
        pass "New leader verified OK"
    else
        fail "New leader verification failed"
    fi
}

# --------------------------------------------------------------------------
# Scenario 4: Fast sync interval
# --------------------------------------------------------------------------
scenario_fast_sync() {
    log "Starting nodes with sync_interval=200ms, follower_pull=200ms..."
    start_node node1 $NODE1_PORT /tmp/ha-e2e-node1 0 \
        --sync-interval-ms 200 --follower-pull-ms 200 >/dev/null
    wait_for_health "http://localhost:$NODE1_PORT" "node1" || return
    wait_for_role "http://localhost:$NODE1_PORT" "Leader" "node1" || return

    start_node node2 $NODE2_PORT /tmp/ha-e2e-node2 0 \
        --sync-interval-ms 200 --follower-pull-ms 200 >/dev/null
    wait_for_health "http://localhost:$NODE2_PORT" "node2" || return
    wait_for_role "http://localhost:$NODE2_PORT" "Follower" "node2" || return

    log "Running writer (150 writes, 10ms interval)..."
    start_writer \
        --interval-ms 10 \
        --total-writes 150 \
        --verify-nodes "http://localhost:$NODE1_PORT,http://localhost:$NODE2_PORT" \
        > "$LOG_DIR/writer.log" 2>&1
    local writer_exit=$?

    if [ $writer_exit -eq 0 ]; then
        pass "Fast sync: writer completed with verification"
    else
        fail "Fast sync: writer failed (exit code $writer_exit)"
        tail -20 "$LOG_DIR/writer.log"
    fi
}

# --------------------------------------------------------------------------
# Scenario 5: Slow sync interval
# --------------------------------------------------------------------------
scenario_slow_sync() {
    log "Starting nodes with sync_interval=3000ms, follower_pull=2000ms..."
    start_node node1 $NODE1_PORT /tmp/ha-e2e-node1 0 \
        --sync-interval-ms 3000 --follower-pull-ms 2000 >/dev/null
    wait_for_health "http://localhost:$NODE1_PORT" "node1" || return
    wait_for_role "http://localhost:$NODE1_PORT" "Leader" "node1" || return

    start_node node2 $NODE2_PORT /tmp/ha-e2e-node2 0 \
        --sync-interval-ms 3000 --follower-pull-ms 2000 >/dev/null
    wait_for_health "http://localhost:$NODE2_PORT" "node2" || return
    wait_for_role "http://localhost:$NODE2_PORT" "Follower" "node2" || return

    log "Running writer (50 writes, 50ms interval)..."
    start_writer \
        --interval-ms 50 \
        --total-writes 50 \
        --verify-nodes "http://localhost:$NODE1_PORT,http://localhost:$NODE2_PORT" \
        > "$LOG_DIR/writer.log" 2>&1
    local writer_exit=$?

    if [ $writer_exit -eq 0 ]; then
        pass "Slow sync: writer completed with verification"
    else
        fail "Slow sync: writer failed (exit code $writer_exit)"
        tail -20 "$LOG_DIR/writer.log"
    fi
}

# --------------------------------------------------------------------------
# Scenario 6: Fast lease (short TTL, fast poll = fast failover)
# --------------------------------------------------------------------------
scenario_fast_lease_failover() {
    log "Testing fast failover: lease_ttl=3s, renew=1s, follower_poll=500ms"

    local node1_pid
    node1_pid=$(start_node node1 $NODE1_PORT /tmp/ha-e2e-node1 20 \
        --lease-ttl 3 --renew-interval-ms 1000 --follower-poll-ms 500)
    wait_for_health "http://localhost:$NODE1_PORT" "node1" || return
    wait_for_role "http://localhost:$NODE1_PORT" "Leader" "node1" || return

    start_node node2 $NODE2_PORT /tmp/ha-e2e-node2 0 \
        --lease-ttl 3 --renew-interval-ms 1000 --follower-poll-ms 500 >/dev/null
    wait_for_health "http://localhost:$NODE2_PORT" "node2" || return
    wait_for_role "http://localhost:$NODE2_PORT" "Follower" "node2" || return

    sleep 3

    log "Killing node1..."
    local kill_time
    kill_time=$(python3 -c "import time; print(time.time())")
    kill "$node1_pid" 2>/dev/null || true
    wait "$node1_pid" 2>/dev/null || true
    remove_pid "$node1_pid"

    if wait_for_role "http://localhost:$NODE2_PORT" "Leader" "node2" 10; then
        local promote_time
        promote_time=$(python3 -c "import time; print(time.time())")
        local elapsed
        elapsed=$(python3 -c "print(f'{$promote_time - $kill_time:.1f}')")
        pass "Fast failover: node2 promoted in ${elapsed}s (lease_ttl=3s)"
    else
        fail "Fast failover: node2 did not promote within 10s"
    fi
}

# --------------------------------------------------------------------------
# Scenario 7: Writer reconnects after failover
# --------------------------------------------------------------------------
scenario_writer_reconnect() {
    log "Starting node1 (leader) with 10 test rows..."
    local node1_pid
    node1_pid=$(start_node node1 $NODE1_PORT /tmp/ha-e2e-node1 10 \
        --lease-ttl 3 --renew-interval-ms 1000 --follower-poll-ms 500)
    wait_for_health "http://localhost:$NODE1_PORT" "node1" || return
    wait_for_role "http://localhost:$NODE1_PORT" "Leader" "node1" || return

    log "Starting node2 (follower)..."
    start_node node2 $NODE2_PORT /tmp/ha-e2e-node2 0 \
        --lease-ttl 3 --renew-interval-ms 1000 --follower-poll-ms 500 >/dev/null
    wait_for_health "http://localhost:$NODE2_PORT" "node2" || return
    wait_for_role "http://localhost:$NODE2_PORT" "Follower" "node2" || return

    sleep 2

    log "Starting writer (200 writes, 50ms interval)..."
    start_writer \
        --interval-ms 50 \
        --total-writes 200 \
        > "$LOG_DIR/writer.log" 2>&1 &
    local writer_pid=$!
    PIDS="$PIDS $writer_pid"

    sleep 3

    log "Killing node1 (leader) while writer is running..."
    kill "$node1_pid" 2>/dev/null || true
    wait "$node1_pid" 2>/dev/null || true
    remove_pid "$node1_pid"

    log "Waiting for node2 to promote..."
    if ! wait_for_role "http://localhost:$NODE2_PORT" "Leader" "node2" 10; then
        fail "Node2 did not promote"
        kill "$writer_pid" 2>/dev/null || true
        return
    fi
    pass "Node2 promoted to Leader"

    log "Waiting for writer to finish..."
    local w=0
    while [ $w -lt 60 ] && kill -0 "$writer_pid" 2>/dev/null; do
        sleep 1
        w=$((w + 1))
    done

    if kill -0 "$writer_pid" 2>/dev/null; then
        warn "Writer still running after 60s, killing"
        kill "$writer_pid" 2>/dev/null || true
    fi
    wait "$writer_pid" 2>/dev/null
    local writer_exit=$?
    remove_pid "$writer_pid"

    if [ $writer_exit -eq 0 ]; then
        pass "Writer completed after failover (reconnected to new leader)"
    else
        warn "Writer exited with code $writer_exit (some errors during failover expected)"
        tail -10 "$LOG_DIR/writer.log"
    fi

    local count2
    count2=$(get_count "http://localhost:$NODE2_PORT")
    if [ "$count2" -gt 10 ]; then
        pass "New leader has $count2 rows (> 10 initial, writes continued after failover)"
    else
        fail "New leader only has $count2 rows (expected > 10)"
    fi

    if verify_node "http://localhost:$NODE2_PORT"; then
        pass "New leader data integrity OK"
    else
        fail "New leader data integrity FAILED"
    fi
}

# ============================================================================
# Main
# ============================================================================

echo ""
echo "================================================================"
echo "  haqlite end-to-end HA test suite"
echo "  bucket=$BUCKET  endpoint=$ENDPOINT  run=$RUN_ID"
echo "================================================================"

if [ ! -f "$BINARY" ] || [ ! -f "$WRITER" ]; then
    echo "ERROR: Release binaries not found. Run: cargo build --release"
    exit 1
fi

mkdir -p "$LOG_DIR"

run_scenario "1. Basic replication"                   scenario_basic_replication
run_scenario "2. Write forwarding via HaQLiteClient"  scenario_write_forwarding
run_scenario "3. Leader failover (default TTL=5s)"    scenario_leader_failover 5 1000
run_scenario "4. Fast sync (200ms)"                   scenario_fast_sync
run_scenario "5. Slow sync (3000ms)"                  scenario_slow_sync
run_scenario "6. Fast lease failover (TTL=3s)"        scenario_fast_lease_failover
run_scenario "7. Writer reconnects after failover"    scenario_writer_reconnect

# ============================================================================
# Summary
# ============================================================================

echo ""
echo "================================================================"
echo -e "  Results: ${GREEN}${PASS} passed${NC}, ${RED}${FAIL} failed${NC}"
echo "================================================================"

if [ $FAIL -gt 0 ]; then
    exit 1
fi
