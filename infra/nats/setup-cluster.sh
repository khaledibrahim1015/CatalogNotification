#!/usr/bin/env bash
# ============================================================================
# setup-cluster.sh — One-shot orchestrator for the NATS JetStream cluster.
#
# Pipeline:
#   1. docker compose up (the 3-node NATS cluster + nats-box sidecar)
#   2. wait for cluster health (routing + JetStream meta leader)
#   3. create/update CATALOG_STREAM with the chosen missed-message policy
#   4. run the monitor report
#
# Usage:
#   ./setup-cluster.sh                      # defaults to LastOnly
#   ./setup-cluster.sh AllMissed
#   ./setup-cluster.sh TimeBounded 72h
#   ./setup-cluster.sh --skip-stream         # bring up + monitor only
#   ./setup-cluster.sh --down                # tear down cluster
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/../docker/docker-compose-nats.yml"
ENV_FILE="$(dirname "$COMPOSE_FILE")/.env"
CREATE_STREAM_SCRIPT="$SCRIPT_DIR/create-stream.sh"
MONITOR_SCRIPT="$SCRIPT_DIR/cluster-monitor.sh"

RED='\033[0;31m'; GRN='\033[0;32m'; YLW='\033[0;33m'; BLU='\033[0;34m'; BLD='\033[1m'; RST='\033[0m'
log()  { echo -e "${BLU}[$(date '+%H:%M:%S')]${RST} $*"; }
ok()   { echo -e "${GRN}✔${RST}  $*"; }
err()  { echo -e "${RED}✘${RST}  $*" >&2; }
sep()  { echo -e "${BLU}$(printf '─%.0s' {1..60})${RST}"; }
header(){ sep; echo -e "${BLD}  $*${RST}"; sep; }

# ── Args ──────────────────────────────────────────────────────────────────
POLICY="LastOnly"
MAX_AGE="0"
SKIP_STREAM=false
TEARDOWN=false

case "${1:-}" in
  --down)
    TEARDOWN=true
    ;;
  --skip-stream)
    SKIP_STREAM=true
    ;;
  "")
    ;;
  *)
    POLICY="$1"
    MAX_AGE="${2:-0}"
    ;;
esac

# ── Pre-flight ────────────────────────────────────────────────────────────
preflight() {
  command -v docker &>/dev/null || { err "docker not found"; exit 1; }
  command -v curl   &>/dev/null || { err "curl not found"; exit 1; }
  command -v jq     &>/dev/null || { err "jq not found"; exit 1; }
  docker info &>/dev/null || { err "Docker daemon is not running"; exit 1; }
  [[ -f "$COMPOSE_FILE" ]] || { err "Compose file not found: $COMPOSE_FILE"; exit 1; }
  ok "Pre-flight checks passed"
}

dc() { docker compose -f "$COMPOSE_FILE" "$@"; }

# ── Teardown path ─────────────────────────────────────────────────────────
if $TEARDOWN; then
  preflight
  header "Tearing down cluster"
  dc down
  ok "Cluster stopped"
  exit 0
fi

preflight

# ── 1. Bring up the cluster ─────────────────────────────────────────────────
header "Starting NATS JetStream Cluster"
dc up -d --remove-orphans
ok "Containers started"

# ── 2. Wait for health ──────────────────────────────────────────────────────
header "Waiting for cluster health"

set -a
[ -f "$ENV_FILE" ] && source "$ENV_FILE"
set +a

TIMEOUT=90
INTERVAL=3
ELAPSED=0

log "Waiting for nats-1 HTTP monitor (max ${TIMEOUT}s)…"
until curl -sf "http://localhost:8222/healthz" &>/dev/null; do
  if (( ELAPSED >= TIMEOUT )); then
    err "nats-1 did not become healthy in ${TIMEOUT}s"
    dc logs --tail 40
    exit 1
  fi
  sleep "$INTERVAL"; (( ELAPSED += INTERVAL ))
done
ok "nats-1 HTTP monitor is up"

if [[ -n "${NATS_SYS_USER:-}" && -n "${NATS_SYS_PASSWORD:-}" ]]; then
  NATS_URL="nats://${NATS_SYS_USER}:${NATS_SYS_PASSWORD}@nats-1:4222"
  log "Waiting for JetStream meta leader election…"
  ELAPSED=0
  until dc exec -T nats-box nats server report jetstream --server "$NATS_URL" 2>/dev/null | grep -q '\*'; do
    if (( ELAPSED >= TIMEOUT )); then
      err "No JetStream meta leader elected within ${TIMEOUT}s"
      exit 1
    fi
    sleep "$INTERVAL"; (( ELAPSED += INTERVAL ))
  done
  ok "JetStream cluster has a meta leader"
else
  log "NATS_SYS_USER/PASSWORD not set in $ENV_FILE — skipping leader-election check"
fi

# ── 3. Create / update stream ───────────────────────────────────────────────
if $SKIP_STREAM; then
  log "Skipping stream creation (--skip-stream)"
else
  header "Provisioning CATALOG_STREAM (policy=$POLICY)"
  if [[ -x "$CREATE_STREAM_SCRIPT" ]]; then
    "$CREATE_STREAM_SCRIPT" "$POLICY" "$MAX_AGE"
  else
    err "create-stream.sh not found or not executable at $CREATE_STREAM_SCRIPT"
    exit 1
  fi
fi

# ── 4. Monitor ───────────────────────────────────────────────────────────────
header "Cluster Monitor Report"
if [[ -x "$MONITOR_SCRIPT" ]]; then
  "$MONITOR_SCRIPT"
else
  err "cluster-monitor.sh not found or not executable at $MONITOR_SCRIPT — skipping"
fi

ok "Setup complete"