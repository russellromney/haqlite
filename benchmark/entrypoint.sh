#!/bin/bash
set -o pipefail

if [ -d /data ]; then
  export TMPDIR=/data
fi

BENCH_CMD="${BENCH_BIN:-haqlite-turbolite-bench}"

if [ -n "$TIERED_TEST_BUCKET" ]; then
  TIMESTAMP=$(date +%Y%m%d_%H%M%S)
  MACHINE_ID=$(hostname || echo "unknown")
  LOG_FILE="/tmp/haqlite_turbolite_bench_${TIMESTAMP}_${MACHINE_ID}.log"
  S3_LOG_PATH="s3://${TIERED_TEST_BUCKET}/bench/logs/haqlite_turbolite_bench_${TIMESTAMP}_${MACHINE_ID}.log"

  if command -v aws >/dev/null 2>&1; then
    (
      while sleep 10; do
        [ -f "$LOG_FILE" ] && aws s3 cp "$LOG_FILE" "$S3_LOG_PATH" ${AWS_ENDPOINT_URL:+--endpoint-url=$AWS_ENDPOINT_URL} 2>/dev/null || true
      done
    ) &
    UPLOADER_PID=$!
  fi

  "$BENCH_CMD" "$@" 2>&1 | tee "$LOG_FILE"
  EXIT_CODE=$?

  [ -n "$UPLOADER_PID" ] && kill "$UPLOADER_PID" 2>/dev/null || true

  if command -v aws >/dev/null 2>&1; then
    aws s3 cp "$LOG_FILE" "$S3_LOG_PATH" ${AWS_ENDPOINT_URL:+--endpoint-url=$AWS_ENDPOINT_URL} 2>/dev/null || true
  fi

  exit "$EXIT_CODE"
else
  exec "$BENCH_CMD" "$@"
fi
