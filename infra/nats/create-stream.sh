#!/usr/bin/env bash
# ============================================================================
# Creates (or updates) the CATALOG_STREAM JetStream stream.
#
# MissedMessagePolicy controls retention/replay semantics for reconnecting
# mobile clients (see diagram: "Stream config — LastOnly vs AllMissed"):
#
#   LastOnly     -> MaxMsgsPerSubject=1   (only latest catalog state kept)
#   AllMissed    -> MaxMsgsPerSubject=-1  (full history, replay every change)
#   TimeBounded  -> AllMissed + MaxAge=N hours (configurable retention window)
#
# Runs through the nats-box sidecar container (the `nats` CLI isn't installed
# on the host or in the nats-server images), and authenticates using the
# SYS account credentials from infra/docker/.env.
#
# Usage:
#   ./create-stream.sh LastOnly
#   ./create-stream.sh AllMissed
#   ./create-stream.sh TimeBounded 72h
# ============================================================================
set -euo pipefail

# ── Locate compose file + .env relative to this script ──────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/../docker/docker-compose-nats.yml"
ENV_FILE="$(dirname "$COMPOSE_FILE")/.env"

if [[ ! -f "$COMPOSE_FILE" ]]; then
  echo "Compose file not found: $COMPOSE_FILE" >&2
  exit 1
fi

set -a
[ -f "$ENV_FILE" ] && source "$ENV_FILE"
set +a

if [[ -z "${NATS_APP_USER:-}" || -z "${NATS_APP_PASSWORD:-}" ]]; then
  echo "NATS_APP_USER / NATS_APP_PASSWORD not set (expected in $ENV_FILE)" >&2
  exit 1
fi

POLICY="${1:-LastOnly}"
MAX_AGE="${2:-0}"   # e.g. 72h ; 0 = unlimited
# Streams live in the APP account (JetStream is NOT enabled on SYS)
NATS_URL="${NATS_URL:-nats://${NATS_APP_USER}:${NATS_APP_PASSWORD}@nats-1:4222}"
STREAM_NAME="CATALOG_STREAM"

case "$POLICY" in
  LastOnly)
    MAX_MSGS_PER_SUBJECT=1
    AGE_FLAG=""
    ;;
  AllMissed)
    MAX_MSGS_PER_SUBJECT=-1
    AGE_FLAG=""
    ;;
  TimeBounded)
    MAX_MSGS_PER_SUBJECT=-1
    if [ "$MAX_AGE" = "0" ]; then
      echo "TimeBounded requires a max age, e.g.: ./create-stream.sh TimeBounded 72h" >&2
      exit 1
    fi
    AGE_FLAG="--max-age=$MAX_AGE"
    ;;
  *)
    echo "Unknown policy: $POLICY (expected LastOnly | AllMissed | TimeBounded)" >&2
    exit 1
    ;;
esac

run_box() {
  docker compose -f "$COMPOSE_FILE" exec -T nats-box "$@"
}

# ── Idempotency: does the stream already exist? ──────────────────────────────
if run_box nats stream info "$STREAM_NAME" --server "$NATS_URL" &>/dev/null; then
  echo "Stream '$STREAM_NAME' already exists — updating config (policy=$POLICY) ..."

  run_box nats stream edit "$STREAM_NAME" \
    --server "$NATS_URL" \
    --subjects "catalog.>" \
    --max-msgs-per-subject "$MAX_MSGS_PER_SUBJECT" \
    --max-msgs=-1 \
    --max-bytes=-1 \
    --max-msg-size=-1 \
    --dupe-window=5m \
    $AGE_FLAG \
    --no-allow-rollup \
    --deny-delete \
    --allow-direct \
    --force

  echo "Stream '$STREAM_NAME' updated."
else
  echo "Creating $STREAM_NAME with policy=$POLICY (MaxMsgsPerSubject=$MAX_MSGS_PER_SUBJECT) ..."

  run_box nats stream add "$STREAM_NAME" \
    --server "$NATS_URL" \
    --subjects "catalog.>" \
    --storage file \
    --replicas 3 \
    --retention limits \
    --max-msgs-per-subject "$MAX_MSGS_PER_SUBJECT" \
    --max-msgs=-1 \
    --max-bytes=-1 \
    --max-msg-size=-1 \
    --dupe-window=5m \
    --compression s2 \
    $AGE_FLAG \
    --discard old \
    --no-allow-rollup \
    --deny-delete \
    --allow-direct \
    --defaults

  echo "Stream '$STREAM_NAME' created."
fi

echo
echo "View with:"
echo "  docker compose -f \"$COMPOSE_FILE\" exec nats-box nats stream info $STREAM_NAME --server \"$NATS_URL\""