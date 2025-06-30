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

echo "
→ Sending a log with an invalid timestamp to trigger the parser DLQ"
curl -s -XPOST "$ENDPOINT" \
  -H 'Content-Type: application/json' \
  -d '{
    "level": "info",
    "service": "test-service-dlq",
    "message": "This log has a bad timestamp and should go to the DLQ",
    "timestamp": "2024-02-31T12:00:00Z"
  }' \
&& echo " ✔ Sent (check parser logs and DLQ)" \
|| echo " ✘ Failed to send"

sleep $DELAY_SEC

echo "
→ Sending a malformed JSON body to trigger an ingestor error"
curl -s -XPOST "$ENDPOINT" \
  -H 'Content-Type: application/json' \
  -d 'this is not json' \
&& echo " ✔ Sent (check ingestor response)" \
|| echo " ✘ Failed to send"


echo "
✅ Done."

