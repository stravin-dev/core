#!/bin/sh
set -eu

NATS_URL="${NATS_URL:-nats://nats:4222}"
STREAM="${STRAVIN_NATS_STREAM:-STRAVIN_CORE}"
SUBJECTS="${STRAVIN_NATS_SUBJECTS:-stravin.core.>}"
MAX_AGE="${STRAVIN_NATS_MAX_AGE:-168h}"
DUPE_WINDOW="${STRAVIN_NATS_DUPE_WINDOW:-2m}"
REPLICAS="${STRAVIN_NATS_REPLICAS:-1}"

echo "[nats-init] Bootstrapping JetStream at ${NATS_URL}"

# Idempotent STRAVIN_CORE stream bootstrap (ADR-0001).
if nats --server "${NATS_URL}" stream info "${STREAM}" >/dev/null 2>&1; then
  echo "[nats-init] JetStream stream '${STREAM}' already exists"
else
  nats --server "${NATS_URL}" stream add "${STREAM}" \
    --subjects "${SUBJECTS}" \
    --storage file \
    --retention limits \
    --discard old \
    --max-age "${MAX_AGE}" \
    --dupe-window "${DUPE_WINDOW}" \
    --replicas "${REPLICAS}" \
    --defaults >/dev/null
  echo "[nats-init] Created JetStream stream '${STREAM}' (subjects=${SUBJECTS}, max_age=${MAX_AGE}, dupe_window=${DUPE_WINDOW})"
fi

echo "[nats-init] Bootstrap complete."
