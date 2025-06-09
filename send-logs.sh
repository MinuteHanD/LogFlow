#!/usr/bin/env bash
set -euo pipefail

# ─── CONFIG: tweak these if you like ──────────────────────────────────────────
ENDPOINT="http://localhost:8081/log"
DEFAULT_LEVEL="info"
DEFAULT_SERVICE="test-service"
MSG_PREFIX="Simulated log"
TOTAL_LOGS=5
DELAY_SEC=1
# ───────────────────────────────────────────────────────────────────────────────

# Usage: ./send-logs.sh [TOTAL_LOGS] [DELAY_SEC]
if [[ $# -ge 1 ]]; then TOTAL_LOGS=$1; fi
if [[ $# -ge 2 ]]; then DELAY_SEC=$2; fi

# optional overrides via env:
LEVEL="${LEVEL:-$DEFAULT_LEVEL}"
SERVICE="${SERVICE:-$DEFAULT_SERVICE}"

echo "→ Sending $TOTAL_LOGS logs to $ENDPOINT (level=$LEVEL service=$SERVICE) every ${DELAY_SEC}s"

for i in $(seq 1 $TOTAL_LOGS); do
  TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  MESSAGE="$MSG_PREFIX #$i"

  curl -s -XPOST "$ENDPOINT" \
    -H 'Content-Type: application/json' \
    -d "{\"level\":\"$LEVEL\",\"service\":\"$SERVICE\",\"message\":\"$MESSAGE\",\"timestamp\":\"$TIMESTAMP\"}" \
  && echo " $i ✔" \
  || echo " $i ✘"

  sleep $DELAY_SEC
done

echo "✅ Done."
