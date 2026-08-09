#!/usr/bin/env bash
#
# Run the queue benchmarks with a fixed iteration count and print the results
# as a markdown table. The raw go test output is kept above the table so the
# numbers in the README can be checked against it.

set -uo pipefail

cd "$(dirname "$0")/.."

ITERS="${ITERS:-1000x}"

echo "go test -run '^\$' -bench . -benchtime=$ITERS ./internal/queue/"
echo

RAW=$(go test -run '^$' -bench . -benchtime="$ITERS" ./internal/queue/ 2>&1)
STATUS=$?
echo "$RAW"
if [ $STATUS -ne 0 ]; then
  echo "benchmarks failed"
  exit 1
fi

echo
echo "| operation | fsync | clients | msg/s | p50 ms | p99 ms | records per fsync |"
echo "| --- | --- | --- | ---: | ---: | ---: | ---: |"

echo "$RAW" | awk '
/^Benchmark/ {
  name = $1
  sub(/-[0-9]+$/, "", name)
  split(name, part, "/")
  op = part[1]
  sub(/^Benchmark/, "", op)

  delete metric
  for (i = 3; i < NF; i += 2) metric[$(i + 1)] = $i

  printf "| %s | %s | %s | %.0f | %.2f | %.2f | %.1f |\n",
    op, part[2], substr(part[3], 2),
    metric["msg/s"], metric["p50_ms"], metric["p99_ms"], metric["rec/fsync"]
}'
