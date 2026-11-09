# apiserver load results

Setup: one host, 4 vCPU, Postgres 16 and Redis 7 on the same host, the coordinator plus one worker (16 slots) executing a live 50,000-job mock run (median 1 s per job) while the load runs. Two apiserver replicas per configuration, requests spread round-robin across them. Records: `records.jsonl` (one JSON object per run, produced by `cmd/apiload`). "DB stats queries" is the apiservers' own `evalrunner_api_stats_db_queries_total` counter, read before and after each run.

The run's stats aggregate scans 50,000 jobs and their attempts, so one computation takes hundreds of milliseconds under load. That is the cost the cache exists to share.

## HTTP GET /api/runs/{id}, 64 concurrent clients, 15 s, 3 reps

| Config | Requests/s (median) | p50 | p95 | p99 | DB stats queries/s |
|---|---:|---:|---:|---:|---:|
| Postgres only | 104 | 606 ms | 894 ms | 990 ms | 104 |
| Redis, stale-while-revalidate (250 ms freshness) | 18,537 | 2.7 ms | 9.1 ms | 13.8 ms | 5.4 |

Requests/s per rep: Postgres only 103.7, 106.3, 97.6; Redis 18,372, 18,918, 18,537.

## WebSocket viewers of the live run, 1,000 viewers, push every 500 ms, 20 s

| Config | Messages delivered | Expected on schedule | DB stats queries/s |
|---|---:|---:|---:|
| Postgres only | 577 | 40,000 | 79.1 |
| Redis, stale-while-revalidate | 40,000 | 40,000 | 2.7 |

Without the cache each viewer's tick runs its own aggregate, Postgres saturates, and viewers get under 2% of their updates. With it, every viewer gets every update.

## The stampede fix

The first Redis version cached with a plain TTL. Under the same GET load it served 127,051 requests in 15.2 s (8,370/s, p50 2.3 ms) but still ran 1,168 stats queries (77/s), barely fewer than no cache: every expiry sent all 64 clients to Postgres at once while the first recompute was still running (dev run, not in records.jsonl). The current version stores a freshness deadline inside the entry, keeps serving the stale copy, and lets one caller across all replicas recompute under a Redis `SET NX` lock. `TestRedisCacheNoStampede` checks that 200 concurrent readers of a cold or stale entry cause exactly one recompute.
