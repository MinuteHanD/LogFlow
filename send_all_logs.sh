#!/usr/bin/env bash
set -euo pipefail

ENDPOINT="http://localhost:8081/log"
SERVICE="test-service-all"
DELAY_SEC=1

LEVELS=("DEBUG" "INFO" "WARN" "WARNING" "ERROR" "FATAL" "PANIC")

echo "→ Sending logs of all levels to $ENDPOINT (service=$SERVICE) every ${DELAY_SEC}s"

for LEVEL in "${LEVELS[@]}"; do
  TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  MESSAGE="This is a $LEVEL log."

  curl -s -XPOST "$ENDPOINT" \
    -H 'Content-Type: application/json' \
    -d "{\"level\":\"$LEVEL\",\"service\":\"$SERVICE\",\"message\":\"$MESSAGE\",\"timestamp\":\"$TIMESTAMP\"}" \
  && echo " $LEVEL ✔" \
  || echo " $LEVEL ✘"

  sleep $DELAY_SEC
done

echo "✅ Done."

