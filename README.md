# go-eval-runner

A distributed runner for evaluation suites, written in Go. A coordinator keeps a durable job queue in Postgres and hands jobs to a pool of workers over gRPC. Workers run evals concurrently, renew their leases while they work, and report results back. Failed attempts are retried with exponential backoff, results are cached by content hash so unchanged cases never run twice, and everything is exported to Prometheus.

The runner does not know what an eval is. A job is a JSON payload plus an executor name; the executor turns it into a JSON result with an optional `score` and `passed`. Two executors ship: `mock` (a synthetic eval for load testing) and `exec` (one OS process per case, speaking JSON over stdin and stdout), which is how it runs the text-to-CAD geometric evals or any other eval written in any language.

## Measured results

All numbers below come from `bench/run-all.sh` and are logged run by run, with full setup, in [`bench/RESULTS.md`](bench/RESULTS.md) and the JSON records in [`bench/runs/`](bench/runs/). Nothing here is estimated.

**Setup.** One host: 4 vCPU Intel Xeon @ 2.30GHz, 15 GB RAM, Linux, Go 1.26.0. One coordinator process and N worker processes talking gRPC over loopback; Postgres 16.15 in a Docker container on the same host. Evals are the `mock` executor (sleeps for a lognormal latency, then returns a deterministic score), so these numbers measure the runner, not a model. Each scenario starts from empty tables.

| Scenario | Setup | Result |
|---|---|---|
| Horizontal scaling | 6,000 jobs, latency lognormal (median 250 ms, sigma 0.5), 16 slots per worker | 1 worker: 3,362 jobs/min. 2: 6,564. 4: 12,948. 8: 25,004 (7.44x for 8x workers). Overhead p95 3.2 to 4.2 ms |
| Scheduler ceiling, A/B | 100,000 zero-latency jobs, 4 workers x 32 slots, 3 reps each | Batched set-based reports: median 492,175 jobs/min. One report RPC per job: median 156,593 jobs/min (3.1x) |
| Result cache | 5,000 jobs, 30% repeat an earlier payload, latency median 100 ms | Cold: 1,500 of 5,000 jobs (30.0%) answered by in-flight dedup, 3,500 executor calls. Identical rerun: 5,000 of 5,000 from cache, 0 executor calls, done in 16 ms. Rerun with ~10% of cases changed: 474 executor calls, 90.5% of jobs from cache |
| Transient faults | 5,000 jobs, 10% of attempts fail, max 4 attempts | 604 retries, 4,998 succeeded, 2 failed after 4 attempts. Overhead p95 6.4 ms |
| Worker crash | 8,000 jobs, 4 workers, SIGKILL one worker 5 s in, 3 s lease | 17 leases reclaimed and re-run, 8,000 of 8,000 succeeded, 0 lost |
| Real processes | 500 Python box-IoU cases via `exec`, 2 workers x 8 | 9,976 jobs/min cold (one `python3` per case); identical rerun 500 of 500 from cache |
| Text-to-CAD suite | 160 offline cases from the text-to-CAD project (CadQuery 2.8.0, CPU-bound), text-to-cad commit 85d928c, via `exec` | 2 workers x 2 slots: 268.6 s wall vs 830.4 s for 1 worker x 1 slot (3.1x on 4 vCPU), 160 of 160 completed, 0 retries. Identical rerun: 160 of 160 from cache, 0 executor calls. An earlier run on the previous text-to-cad commit found one case exceeding the 120 s timeout, which that project then fixed (both runs are in the log) |

Definitions. *jobs/min* counts jobs that reached a terminal state over the run's wall time (submit to last result). *Attempt latency* runs from lease grant to result report on the coordinator's clock. *Overhead* is attempt latency minus the executor time the worker measured, so it is what the runner itself adds: gRPC, worker scheduling, Postgres. *Cache saved* is jobs answered without calling an executor divided by total jobs.

Caveats worth knowing. The scaling scenario is latency-bound, so 8 workers fit on 4 cores; CPU-heavy evals would not scale that way on one host. The zero-latency overhead runs saturate the queue on purpose, which is why their p95 overhead (about 40 ms) is higher than in the realistic scenarios (under 7 ms). The first `overhead-batched` rep was slower (342,325 jobs/min) than the other two; the table reports the median. The crash scenario reclaimed 17 leases from a worker with 16 slots because one job had finished but its batched report had not been sent when the process died, so it was correctly re-run.

## Architecture

```
            evalctl / evalbench
                   | SubmitRun, GetRun (gRPC)
                   v
  +--------------------------------+          +-------------------------+
  |          coordinator           |  SQL     |        Postgres         |
  |  stateless gRPC service        |<-------->|  runs, jobs, attempts,  |
  |  long-poll leasing, reaper     |          |  result_cache           |
  |  /metrics (Prometheus)         |          +-------------------------+
  +--------------------------------+
      ^  LeaseJobs (long poll), RenewLeases, ReportResults
      |
  +---+--------------+   +------------------+   +------------------+
  | worker 1         |   | worker 2         |   | worker N         |
  | C goroutine slots|   | ...              |   | ...              |
  | executors:       |   |                  |   |                  |
  |  mock, exec      |   |                  |   |                  |
  +------------------+   +------------------+   +------------------+
```

**Queue.** Jobs live in Postgres. `LeaseJobs` claims up to N runnable jobs with `FOR UPDATE SKIP LOCKED`, so concurrent lease calls never block on each other or hand out the same job twice (an end-to-end test runs 400 jobs on 4 workers x 16 slots and asserts exactly 400 executor calls). The coordinator holds no scheduling state; restarting it loses nothing.

**Leases and fencing.** A leased job carries an expiry. Workers renew every third of the lease while a job runs. A reaper reclaims expired leases (crashed or hung workers) and requeues them, or fails them once attempts are used up. Every report is fenced on `(job, worker, attempt)`: a worker whose lease was reclaimed cannot overwrite the newer attempt, and a resent batch is harmless.

**Retries.** An executor error is retryable unless marked permanent (bad input, exit code 3 for `exec`). Retryable failures are requeued with exponential backoff and equal jitter (`base * 2^(attempt-1)`, capped, half fixed and half random).

**Result cache.** The cache key is `sha256(job_type, canonical(config), canonical(payload))`, with JSON canonicalized so key order and whitespace do not matter. At submit, jobs whose key is cached complete immediately with status `cached`. Jobs whose key is already queued or running anywhere are parked as `waiting` and are answered by the leader's result when it lands, so a suite with duplicate cases makes one call per distinct case. If a leader fails, one waiter is promoted to run in its place. `-no-cache` forces execution.

**Batched reporting.** Workers send results through one reporter goroutine per worker; whatever finished while the previous RPC was in flight goes out together in one `ReportResults` call. The coordinator applies a batch with `unnest()`-driven `UPDATE` and `INSERT` CTEs: a fixed number of statements and one commit whatever the batch size. This is the change behind the 3.1x scheduler ceiling above. `-batch-reports=false` keeps the one-RPC-per-job path for comparison.

## Dashboard and HTTP API

![Live run in the dashboard](docs/dashboard-live.png)

`cmd/apiserver` is a stateless HTTP service in front of the same Postgres:

| Endpoint | What it does |
|---|---|
| `GET /api/runs` | Recent runs with status counts |
| `POST /api/runs` | Submit a suite as JSON; forwarded to the coordinator over gRPC |
| `GET /api/runs/{id}` | Full stats for one run |
| `GET /api/runs/{id}/ws` | WebSocket stream of the run's stats, pushed every 500 ms until it finishes |

Run stats are an aggregate over every job and attempt in the run, so a big live run costs hundreds of milliseconds per computation. With `-redis` set, replicas share a Redis cache with stale-while-revalidate: entries carry a freshness deadline (250 ms for live runs, an hour once finished), stale copies keep being served, and one caller across all replicas recomputes under a Redis `SET NX` lock, so a burst of viewers never stampedes Postgres. If Redis is down, reads fall back to Postgres.

`web/` is a Next.js 16 / React 19 dashboard in TypeScript: a server-rendered run list refreshed from the REST API, a form that launches mock suites, and a run page fed by the WebSocket stream.

```bash
./bin/apiserver -redis localhost:6379 &              # :8080, metrics :9092
cd web && npm ci && npm run dev                      # http://localhost:3000
```

Measured on one 4 vCPU host against a live 50,000-job run, two apiserver replicas ([`bench/api/RESULTS.md`](bench/api/RESULTS.md), `cmd/apiload`):

| | Postgres only | Redis, stale-while-revalidate |
|---|---:|---:|
| GET /api/runs/{id}, 64 clients: requests/s | 104 | 18,537 |
| p50 / p99 latency | 606 / 990 ms | 2.7 / 13.8 ms |
| Postgres stats queries/s | 104 | 5.4 |
| 1,000 WebSocket viewers: updates delivered of 40,000 | 577 | 40,000 |

### Kubernetes

[`deploy/k8s/evalrunner.yaml`](deploy/k8s/evalrunner.yaml) runs Postgres, Redis, the coordinator, a worker Deployment (scale with `kubectl scale`), two apiserver replicas behind a Service, and the dashboard. CI builds both images, starts a kind cluster, deploys, and runs [`deploy/k8s/smoke.sh`](deploy/k8s/smoke.sh): a Job submits 2,000 cases, then the script reads the run back through the load-balanced apiserver Service and the dashboard, and prints how many attempts each worker pod ran.

## The job interface

Any eval plugs in through the `exec` executor. The run config names the command:

```json
{"command": ["python", "-m", "text_to_cad.eval_case"], "dir": "/path/to/project", "env": ["SEED=1"]}
```

For each case the worker starts the command and writes one JSON object to its stdin:

```json
{"case_id": "cube-20mm", "payload": {"prompt": "a 20 mm cube", "seed": 1}, "attempt": 1}
```

The process prints one JSON object on stdout (earlier log lines are ignored) and exits 0:

```json
{"score": 0.93, "passed": true, "metrics": {"iou": 0.93, "volume_rel_err": 0.02}}
```

A nonzero exit, a timeout, or `{"error": "...", "retryable": true}` is retried. Exit code 3 or `{"error": "..."}` without `retryable` fails the job immediately. Cases are submitted as JSONL, one `{"case_id", "payload"}` per line. Put everything that changes the outcome (model id, seed, prompt version) in the payload or config so the cache key changes with it.

[`examples/exec-python/`](examples/exec-python/) is a working example: a standard-library Python case that scores a predicted 3D box against a reference by volumetric IoU, plus 500 cases.

In Go, an executor is a two-method interface:

```go
type Executor interface {
    Name() string
    Run(ctx context.Context, job Job) (Result, error) // wrap err with executor.Permanent to skip retries
}
```

## Running it

Requirements: Go 1.26+, Docker (for Postgres), and `protoc` only if you change the proto.

```bash
# Postgres
docker run -d --name evpg -e POSTGRES_USER=eval -e POSTGRES_PASSWORD=eval -e POSTGRES_DB=eval -p 5432:5432 postgres:16-alpine

go build -o bin/ ./cmd/...
./bin/coordinator &                                  # gRPC :7070, metrics :9090, migrates on start
./bin/worker -concurrency 16 -metrics-addr :9191 &
./bin/worker -concurrency 16 -metrics-addr :9192 &

./bin/evalctl gen-mock -n 1000 -unique 700 > cases.jsonl
./bin/evalctl submit -suite demo -type mock -config '{"latency_median_ms":100}' -cases cases.jsonl -wait

./bin/evalctl submit -suite boxes -type exec -config @examples/exec-python/config.json \
  -cases examples/exec-python/cases.jsonl -wait
```

`-wait` prints the run's stats as JSON: counts by status, retries, lease expirations, wall time, jobs/min, attempt, executor, and overhead percentiles, mean score, pass rate, and the cache-saved fraction.

### Docker Compose

```bash
docker compose -f deploy/docker-compose.yml up -d --build --scale worker=4
docker compose -f deploy/docker-compose.yml run --rm -T evalctl gen-mock -n 1000 > cases.jsonl
docker compose -f deploy/docker-compose.yml run --rm -T evalctl submit -suite demo -type mock -wait < cases.jsonl
```

This starts Postgres, the coordinator, the workers, and Prometheus on [localhost:9099](http://localhost:9099), which discovers every worker replica through Compose DNS. Workers run `exec` jobs inside their own container, so for a Python eval build a worker image that also contains the eval's environment (for example, `FROM` the eval's image and copy in `/usr/local/bin/worker`). Workers accept `-exec-allow` to restrict which commands they will run.

### Metrics

Coordinator: `evalrunner_jobs_submitted_total`, `evalrunner_cache_hits_total{source="submit"|"inflight_dedup"}`, `evalrunner_jobs_finished_total{status}`, `evalrunner_retries_scheduled_total`, `evalrunner_lease_expired_total`, `evalrunner_stale_reports_total`, `evalrunner_queue_depth{status}`, `evalrunner_lease_rpc_seconds`. Workers: `evalrunner_worker_exec_seconds{job_type,outcome}`, `evalrunner_worker_inflight`, `evalrunner_worker_lost_leases_total`. Both serve `/debug/pprof/` on the metrics port.

### Tests

```bash
go test ./...                                                     # unit tests
EVALRUNNER_TEST_DSN=postgres://eval:eval@localhost:5432/evaltest?sslmode=disable \
  go test -race ./...                                             # plus end-to-end against Postgres
```

The end-to-end tests run a real coordinator and workers in-process against Postgres and cover retries and permanent failures, cache hits across runs (including reordered config keys), in-flight dedup (30 jobs over 3 payloads make exactly 3 calls), exactly-once leasing under 4 x 16 concurrency, lease expiry with stale-report fencing, recovery from a crashed worker, and mixed-outcome batch reports. The `exec` executor tests cover the stdin and stdout contract, error classification, timeout kill, and the command allowlist.

### Benchmarks

```bash
EVALRUNNER_DSN=postgres://eval:eval@<postgres-host>:5432/eval?sslmode=disable bench/run-all.sh
bench/run-all.sh scaling cache        # or just some scenarios
```

`evalbench` starts a coordinator and N worker processes from `bin/`, submits a generated or given suite, optionally SIGKILLs a worker mid-run, waits, and writes a JSON record (host, git commit, flags, and every stat) to `bench/runs/`. `evalbench summarize` regenerates `bench/RESULTS.md` from those records. On a Docker host, point the DSN at the Postgres container's IP rather than `localhost`: the userland port proxy cost about 70% of a core and roughly 20% of throughput in the early runs logged in `bench/RESULTS.md`.

## Layout

```
proto/evalrunner/v1/   gRPC service definition (generated code in gen/)
internal/store/        Postgres queue, leases, reaper, result cache, stats (SQL lives here)
internal/coordinator/  gRPC server, long-poll leasing, reaper loop
internal/worker/       bounded worker pool, lease renewal, batched reporter
internal/executor/     Executor interface, mock and exec executors
internal/cachekey/     canonical JSON and cache keys
internal/metrics/      Prometheus collectors
internal/e2e/          end-to-end tests against Postgres
cmd/                   coordinator, worker, evalctl, evalbench
internal/api/          REST + WebSocket handlers, Redis stats cache
cmd/                   also apiserver (HTTP) and apiload (HTTP/WebSocket load generator)
web/                   Next.js dashboard
deploy/                Docker Compose, Prometheus config, Kubernetes manifests and smoke test
bench/                 suite script, run records, generated RESULTS.md
```

## Limits and next steps

- A whole suite is sent in one `SubmitRun` message (limit raised to 256 MB). Very large suites should stream cases instead.
- All coordination happens in SQL, so several coordinators can in principle share one database, but that is untested. Long-poll wake-ups are in-process, so a second coordinator would only see new work on its 50 ms poll.
- Dedup of identical in-flight jobs is decided at submit time; two submits racing on the same new key can both run it. The reaper repairs any waiter whose leader vanished.
- Not yet measured: multi-host deployment, and text-to-CAD cases that call Claude (those need an API key; only the 160 offline cases have run).

## Draft resume bullets

Every number below is from the logged runs above (synthetic evals on one 4 vCPU host); say so if asked.

- Built a distributed eval runner in Go (gRPC, Postgres, Docker, Prometheus) with SKIP LOCKED job leasing, fenced lease renewal, and exponential-backoff retries; scaled from 1 to 8 workers at 93% linear efficiency (3.4k to 25k jobs/min) with p95 scheduling overhead under 5 ms.
- Added a content-addressed result cache with in-flight dedup so identical reruns made 0 eval calls and a rerun with 10% of cases changed skipped 90.5% of calls; recovered all 8,000 jobs after SIGKILLing a worker mid-run.
- Built a Next.js/React dashboard on a REST and WebSocket API with a Redis stale-while-revalidate cache, raising stats reads from 104 to 18.5k requests/s (p50 606 ms to 2.7 ms) and cutting Postgres load 19x.
- Deployed the coordinator, autoscalable workers, and replicated API servers on Kubernetes, verified end to end in CI on a kind cluster.
- Raised scheduler throughput 3.1x (157k to 492k jobs/min, median of 3) after pprof showed the coordinator waiting on per-statement Postgres round trips, by replacing per-job result writes with set-based batched transactions.
