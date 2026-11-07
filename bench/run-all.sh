#!/usr/bin/env bash
# Runs the full benchmark suite. Every scenario writes one JSON record to
# bench/runs/; RESULTS.md is regenerated from those records at the end.
#
# Usage: EVALRUNNER_DSN=postgres://... bench/run-all.sh [scenario ...]
# Scenarios: overhead scaling cache faults crash textcad (default: all but textcad)
set -euo pipefail
cd "$(dirname "$0")/.."
DSN=${EVALRUNNER_DSN:-postgres://eval:eval@localhost:5432/eval?sslmode=disable}
go build -o bin/ ./cmd/...
B() { ./bin/evalbench -dsn "$DSN" "$@"; }

overhead() {
  # Scheduler ceiling: executor returns immediately, so every millisecond
  # measured is runner overhead. A/B on result batching, 3 reps each.
  for rep in 1 2 3; do
    B -name overhead-batched   -desc "zero-latency mock, batched set-based reports" -batch-reports=true  -workers 4 -concurrency 32 -n 100000
    B -name overhead-unbatched -desc "zero-latency mock, one report RPC per job"    -batch-reports=false -workers 4 -concurrency 32 -n 100000
  done
}

scaling() {
  # Horizontal scaling with an eval-like latency (lognormal, median 250 ms).
  for w in 1 2 4 8; do
    B -name "scaling-w$w" -desc "lognormal latency median 250ms sigma 0.5, 16 slots per worker" \
      -workers "$w" -concurrency 16 -n 6000 -config '{"latency_median_ms":250,"latency_sigma":0.5}'
  done
}

cache() {
  # 30% of cases repeat an earlier payload; then an identical rerun; then a
  # rerun where 10% of cases changed (the common "edit a few evals" loop).
  B -name cache -desc "30% duplicate payloads, then warm rerun, then 10% changed" \
    -workers 4 -concurrency 16 -n 5000 -unique 3500 -warm -changed 0.1 \
    -config '{"latency_median_ms":100,"latency_sigma":0.5}'
}

faults() {
  # 10% of attempts fail transiently; up to 4 attempts with backoff.
  B -name faults -desc "10% transient failure rate per attempt, max 4 attempts" \
    -workers 4 -concurrency 16 -n 5000 -max-attempts 4 \
    -config '{"latency_median_ms":100,"latency_sigma":0.5,"fail_rate":0.1}'
}

crash() {
  # SIGKILL one of four workers mid-run; its leases expire and are re-run.
  B -name crash -desc "SIGKILL 1 of 4 workers 5s into the run, 3s lease" \
    -workers 4 -concurrency 16 -n 8000 -lease 3s -kill-worker-after 5s \
    -config '{"latency_median_ms":100,"latency_sigma":0.5}'
}

textcad() {
  # Real evals from the text-to-CAD project through the exec executor.
  : "${TEXTCAD_CASES:?set TEXTCAD_CASES to the cases JSONL}" "${TEXTCAD_CONFIG:?set TEXTCAD_CONFIG to the exec config JSON}"
  B -name textcad -desc "text-to-CAD geometric evals via exec executor" -type exec \
    -cases "$TEXTCAD_CASES" -config "$TEXTCAD_CONFIG" -workers "${TEXTCAD_WORKERS:-2}" -concurrency "${TEXTCAD_CONC:-4}" -warm
}

if [ $# -eq 0 ]; then set -- overhead scaling cache faults crash; fi
for s in "$@"; do "$s"; done
./bin/evalbench summarize bench/runs bench/runs/dev > bench/RESULTS.md
echo "wrote bench/RESULTS.md"
