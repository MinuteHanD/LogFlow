#!/bin/bash

# Default values
LEVEL="info"
SERVICE="test-service"
MESSAGE="This is a test message"
TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --level)
      LEVEL="$2"
      shift 2
      ;;
    --service)
      SERVICE="$2"
      shift 2
      ;;
    --message)
      MESSAGE="$2"
      shift 2
      ;;
    --timestamp)
      TIMESTAMP="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Send the log
curl -X POST http://localhost:8081/log \
  -H "Content-Type: application/json" \
  -d "{
    \"level\": \"$LEVEL\",
    \"service\": \"$SERVICE\",
    \"message\": \"$MESSAGE\",
    \"timestamp\": \"$TIMESTAMP\"
  }"

echo 