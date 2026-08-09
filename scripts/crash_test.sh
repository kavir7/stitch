#!/usr/bin/env bash
#
# Fill a queue, consume it under load, SIGKILL the server at a random point,
# restart it, and reconcile what came back against what the server confirmed.
#
# The kill is a real kill -9. Nothing is flushed, nothing is closed, and the
# process does not get to run any shutdown code. Whatever the recovered log
# says is the whole of what survived.

set -uo pipefail

ADDR="${ADDR:-127.0.0.1:18123}"
OUT="${OUT:-.crashtest}"
COUNT="${COUNT:-100000}"
PRODUCERS="${PRODUCERS:-32}"
CONSUMERS="${CONSUMERS:-16}"
FSYNC="${FSYNC:-group}"

cd "$(dirname "$0")/.."

DATA="$OUT/data"
rm -rf "$OUT"
mkdir -p "$DATA"

echo "building"
go build -o "$OUT/queued" ./cmd/queued || exit 1
go build -o "$OUT/loadgen" ./cmd/loadgen || exit 1

start_server() {
  "$OUT/queued" -addr "$ADDR" -data "$DATA" -fsync "$FSYNC" >>"$1" 2>&1 &
  SERVER_PID=$!
}

cleanup() {
  if [ -n "${SERVER_PID:-}" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null
  fi
}
trap cleanup EXIT

echo "starting server on $ADDR with fsync=$FSYNC"
start_server "$OUT/server-before.log"
FIRST_PID=$SERVER_PID

echo "running load: $COUNT messages, $PRODUCERS producers, $CONSUMERS consumers"
"$OUT/loadgen" crash -addr "$ADDR" -out "$OUT" -n "$COUNT" \
  -producers "$PRODUCERS" -consumers "$CONSUMERS" &
LOAD_PID=$!

# Wait for the fill phase to finish before opening the kill window, so that a
# lost enqueue response can never be mistaken for a lost message.
for _ in $(seq 1 1200); do
  [ -f "$OUT/ready" ] && break
  if ! kill -0 "$LOAD_PID" 2>/dev/null; then
    echo "FAIL: the load generator exited before the queue was filled"
    exit 1
  fi
  sleep 0.5
done
if [ ! -f "$OUT/ready" ]; then
  echo "FAIL: the queue was not filled within the time limit"
  exit 1
fi

KILL_DELAY_MS=$((300 + RANDOM % 2700))
KILL_DELAY=$(awk -v ms="$KILL_DELAY_MS" 'BEGIN { printf "%.3f", ms / 1000 }')
echo "consumers are running; killing the server in ${KILL_DELAY}s"
sleep "$KILL_DELAY"

ACKED_AT_KILL=$(wc -l <"$OUT/acked.ids" | tr -d ' ')
echo "SIGKILL to pid $FIRST_PID (about $ACKED_AT_KILL acks confirmed so far)"
kill -9 "$FIRST_PID"

wait "$LOAD_PID"
if kill -0 "$FIRST_PID" 2>/dev/null; then
  echo "FAIL: the server survived kill -9, which should not be possible"
  exit 1
fi
echo "server is dead; restarting on the same log"

RESTART_START=$(date +%s)
start_server "$OUT/server-after.log"
"$OUT/loadgen" verify -addr "$ADDR" -out "$OUT"
VERIFY_STATUS=$?
RESTART_END=$(date +%s)

echo "recovery and drain took $((RESTART_END - RESTART_START))s"
echo "server logs: $OUT/server-before.log and $OUT/server-after.log"
grep -E "wal:|queue:" "$OUT/server-after.log" | head -5

if [ $VERIFY_STATUS -ne 0 ]; then
  echo
  echo "CRASH TEST FAILED"
  exit 1
fi
echo
echo "CRASH TEST PASSED"
