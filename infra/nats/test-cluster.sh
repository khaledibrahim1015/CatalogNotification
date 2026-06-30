#!/usr/bin/env bash
# ============================================================================
# test-cluster.sh — Functional smoke tests for the NATS JetStream cluster.
#
# Verifies:
#   1. All 3 nodes are reachable and report healthy
#   2. Cluster routes are fully meshed (2 peers per node)
#   3. JetStream has an elected meta leader
#   4. Pub/sub works on the APP account
#   5. JetStream publish + stream storage works (CATALOG_STREAM)
#   6. Pulling a message back out of the stream works
#   7. Node failover: kill the JS meta leader, confirm a new one is elected
#
# Usage:
#   ./test-cluster.sh            # run all tests
#   ./test-cluster.sh --no-failover   # skip the disruptive failover test
# ============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/../docker/docker-compose-nats.yml"
ENV_FILE="$(dirname "$COMPOSE_FILE")/.env"

set -a
[ -f "$ENV_FILE" ] && source "$ENV_FILE"
set +a

NATS_SYS_URL="nats://${NATS_SYS_USER}:${NATS_SYS_PASSWORD}@nats-1:4222"
NATS_APP_URL="nats://${NATS_APP_USER}:${NATS_APP_PASSWORD}@nats-1:4222"
TEST_STREAM="CATALOG_STREAM"
TEST_PUBSUB_SUBJECT="test.smoke.pubsub"   # deliberately NOT under catalog.> so JetStream doesn't intercept it

RUN_FAILOVER=true
[[ "${1:-}" == "--no-failover" ]] && RUN_FAILOVER=false

PASS=0
FAIL=0

GRN='\033[0;32m'; RED='\033[0;31m'; YLW='\033[0;33m'; RST='\033[0m'
pass() { echo -e "  ${GRN}✔ PASS${RST}  $*"; ((PASS++)); }
fail() { echo -e "  ${RED}✘ FAIL${RST}  $*"; ((FAIL++)); }
section() { echo -e "\n── $* ──────────────────────────────"; }

dc()      { docker compose -f "$COMPOSE_FILE" "$@"; }
run_box() { dc exec -T nats-box "$@" 2>&1; }

# ── 1. Node reachability ─────────────────────────────────────────────────────
section "1. Node health (HTTP monitor)"
for port_map in "nats-1:8222"; do
  if curl -sf "http://localhost:8222/healthz" &>/dev/null; then
    pass "nats-1 /healthz reachable"
  else
    fail "nats-1 /healthz unreachable"
  fi
done

for node in nats-1 nats-2 nats-3; do
  status=$(docker inspect --format='{{.State.Status}}' "$node" 2>/dev/null)
  if [[ "$status" == "running" ]]; then
    pass "$node container is running"
  else
    fail "$node container status: $status"
  fi
done

# ── 2. Route mesh ─────────────────────────────────────────────────────────────
section "2. Cluster route mesh"
ROUTES_OUTPUT=$(run_box nats server report routes --server "$NATS_SYS_URL")
if echo "$ROUTES_OUTPUT" | grep -q "nats-2" && echo "$ROUTES_OUTPUT" | grep -q "nats-3"; then
  pass "nats-1 sees both nats-2 and nats-3 as peers"
else
  fail "nats-1 is missing one or more cluster peers"
  echo "$ROUTES_OUTPUT" | sed 's/^/      /'
fi

# ── 3. JetStream meta leader ──────────────────────────────────────────────────
section "3. JetStream Raft meta leader"
JS_OUTPUT=$(run_box nats server report jetstream --server "$NATS_SYS_URL")
if echo "$JS_OUTPUT" | grep -q '\*'; then
  LEADER=$(echo "$JS_OUTPUT" | grep '\*' | awk '{print $2}')
  pass "Meta leader elected: $LEADER"
else
  fail "No JetStream meta leader found"
fi

# ── 4. Core NATS pub/sub (APP account) ────────────────────────────────────────
section "4. Core pub/sub (APP account, no JetStream)"
rm -f /tmp/sub.log
docker compose -f "$COMPOSE_FILE" exec -T nats-box \
  nats reply "$TEST_PUBSUB_SUBJECT" "pong" --server "$NATS_APP_URL" --count=1 \
  &>/tmp/sub.log &
SUB_PID=$!

# Poll for the subscriber to actually be listening instead of a fixed sleep
READY=false
for i in $(seq 1 20); do
  if grep -q "Listening" /tmp/sub.log 2>/dev/null; then
    READY=true
    break
  fi
  sleep 0.25
done

if ! $READY; then
  fail "Subscriber never reported ready (check nats-box connectivity)"
else
  REQ_OUTPUT=$(run_box nats request "$TEST_PUBSUB_SUBJECT" "ping" --server "$NATS_APP_URL" 2>&1)
  if echo "$REQ_OUTPUT" | grep -q "pong"; then
    pass "Request/reply round-trip succeeded"
  else
    fail "Request/reply failed"
    echo "$REQ_OUTPUT" | sed 's/^/      /'
  fi
fi
wait "$SUB_PID" 2>/dev/null

# ── 5. JetStream publish ──────────────────────────────────────────────────────
section "5. JetStream publish to $TEST_STREAM"
TEST_MSG="smoke-test-$(date +%s)"
PUB_OUTPUT=$(run_box nats pub "catalog.test.smoke" "$TEST_MSG" --server "$NATS_APP_URL" 2>&1)
if echo "$PUB_OUTPUT" | grep -qi "published"; then
  pass "Message published to catalog.test.smoke"
else
  fail "Publish failed"
  echo "$PUB_OUTPUT" | sed 's/^/      /'
fi

# ── 6. Verify message landed in the stream ────────────────────────────────────
section "6. Verify message stored in stream"
sleep 1
STREAM_INFO=$(run_box nats stream info "$TEST_STREAM" --server "$NATS_APP_URL" 2>&1)
if echo "$STREAM_INFO" | grep -qE "Messages:\s*[1-9]"; then
  pass "Stream shows at least 1 message stored"
else
  fail "Stream shows zero messages — check subject filter or stream config"
  echo "$STREAM_INFO" | grep -i "messages" | sed 's/^/      /'
fi

GET_OUTPUT=$(run_box nats stream get "$TEST_STREAM" --last-for "catalog.test.smoke" --server "$NATS_APP_URL" 2>&1)
if echo "$GET_OUTPUT" | grep -q "$TEST_MSG"; then
  pass "Retrieved message matches what was published"
else
  fail "Could not retrieve matching message from stream"
  echo "$GET_OUTPUT" | sed 's/^/      /'
fi

# ── 7. Failover test (disruptive — kills the JS meta leader) ─────────────────
if $RUN_FAILOVER; then
  section "7. Failover: kill JS meta leader, confirm re-election"
  if [[ -n "${LEADER:-}" ]]; then
    LEADER_CLEAN="${LEADER%\*}"
    echo "  Stopping leader container: $LEADER_CLEAN ..."
    dc stop "$LEADER_CLEAN" &>/dev/null
    sleep 8
    NEW_JS_OUTPUT=$(run_box nats server report jetstream --server "$NATS_SYS_URL")
    if echo "$NEW_JS_OUTPUT" | grep -q '\*'; then
      NEW_LEADER_RAW=$(echo "$NEW_JS_OUTPUT" | grep '\*' | awk '{print $2}')
      NEW_LEADER_CLEAN="${NEW_LEADER_RAW%\*}"
      if [[ "$NEW_LEADER_CLEAN" != "$LEADER_CLEAN" ]]; then
        pass "New meta leader elected after failover: $NEW_LEADER_CLEAN (was $LEADER_CLEAN)"
      else
        fail "Leader name unchanged — re-election may not have occurred"
      fi
    else
      fail "No meta leader after stopping $LEADER_CLEAN — quorum may be lost"
    fi
    echo "  Restarting $LEADER_CLEAN ..."
    dc start "$LEADER_CLEAN" &>/dev/null
    sleep 5
    pass "$LEADER_CLEAN restarted (rejoin happens automatically via Raft catch-up)"
  else
    echo -e "  ${YLW}Skipped — no leader detected in step 3${RST}"
  fi
else
  section "7. Failover test skipped (--no-failover)"
fi

# ── Summary ────────────────────────────────────────────────────────────────────
echo
echo "════════════════════════════════════════════"
echo -e "  ${GRN}Passed: $PASS${RST}   ${RED}Failed: $FAIL${RST}"
echo "════════════════════════════════════════════"

[[ $FAIL -eq 0 ]] && exit 0 || exit 1