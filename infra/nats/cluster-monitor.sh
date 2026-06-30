#!/usr/bin/env bash
# =============================================================================
# monitor.sh — Quick NATS cluster health & status commands
# Run after: docker compose -f docker-compose-nats.yml up -d
# Credentials are read from .env (NATS_SYS_USER / NATS_SYS_PASSWORD)
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/../docker/docker-compose-nats.yml"
ENV_FILE="$(dirname "$COMPOSE_FILE")/.env"

set -a
[ -f "$ENV_FILE" ] && source "$ENV_FILE"
set +a

NATS_SYS_URL="nats://${NATS_SYS_USER}:${NATS_SYS_PASSWORD}@nats-1:4222"
NATS_APP_URL="nats://${NATS_APP_USER}:${NATS_APP_PASSWORD}@nats-1:4222"

sep() { echo -e "\n──────────────────────────────────────────"; }

run_box() {
  docker compose -f "$COMPOSE_FILE" exec -T nats-box "$@" 2>/dev/null
}

# ── 1. Cluster routes (requires SYS account) ─────────────────────────────────
sep; echo "🔵 Cluster Routes Report"
run_box nats server report routes --server "$NATS_SYS_URL" \
  || echo "  Not available yet"

# ── 2. JetStream summary across all nodes (requires SYS account) ────────────
sep; echo "💾 JetStream Info"
run_box nats server report jetstream --server "$NATS_SYS_URL" \
  || echo "  Not available yet"

# ── 3. Streams in the APP account (where JetStream is actually enabled) ─────
sep; echo "📋 Stream List (APP account)"
run_box nats stream list --server "$NATS_APP_URL" \
  || echo "  No streams yet"

# ── 4. Consumers (per stream — edit STREAM_NAME or loop) ─────────────────────
# sep; echo "👥 Consumers"
# run_box nats consumer list <STREAM_NAME> --server "$NATS_APP_URL" \
#   || echo "  No consumers yet"

# ── 5. Connections (requires SYS account) ────────────────────────────────────
sep; echo "📡 Connections Report"
run_box nats server report connections --server "$NATS_SYS_URL" \
  || echo "  No connections yet"

# ── 6. Server info (client-level, no SYS needed) ─────────────────────────────
sep; echo "ℹ️  Server Info (nats-1)"
run_box nats server info --server "$NATS_SYS_URL" \
  || echo "  Not available yet"

# ── 7. Raw monitoring endpoints (no nats CLI needed) ─────────────────────────
sep; echo "🌐 HTTP Monitoring — varz"
curl -sf http://localhost:8222/varz | jq '{version, uptime, connections, in_msgs, out_msgs}'

sep; echo "🌐 HTTP Monitoring — routez"
curl -sf http://localhost:8222/routez | jq '{num_routes, routes: [.routes[]? | {remote_name, ip, port}]}'

sep; echo "🌐 HTTP Monitoring — jsz"
curl -sf http://localhost:8222/jsz | jq '{streams, consumers, memory, store}'

sep; echo "✅ Done"