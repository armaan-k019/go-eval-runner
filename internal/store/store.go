// Package store is the Postgres-backed job queue, attempt log, and result
// cache. All scheduling state lives here so that the coordinator is
// stateless and can be restarted without losing work.
package store

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/armaan-k019/go-eval-runner/internal/cachekey"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

type Store struct {
	pool *pgxpool.Pool
}

func Open(ctx context.Context, dsn string, maxConns int32) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

// Migrate applies every embedded migration not yet recorded. A session-level
// advisory lock keeps concurrent coordinators from racing.
func (s *Store) Migrate(ctx context.Context) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock(7357001)`); err != nil {
		return err
	}
	defer conn.Exec(context.Background(), `SELECT pg_advisory_unlock(7357001)`)

	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (name text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return err
	}
	entries, err := migrationFS.ReadDir("migrations")
	if err != nil {
		return err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	for _, name := range names {
		var exists bool
		if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE name=$1)`, name).Scan(&exists); err != nil {
			return err
		}
		if exists {
			continue
		}
		sql, err := migrationFS.ReadFile("migrations/" + name)
		if err != nil {
			return err
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, string(sql)); err != nil {
			tx.Rollback(ctx)
			return fmt.Errorf("migration %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (name) VALUES ($1)`, name); err != nil {
			tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Truncate wipes all runs, jobs, attempts, and cached results. Used by the
// benchmark harness to give each scenario a clean slate.
func (s *Store) Truncate(ctx context.Context, keepCache bool) error {
	q := `TRUNCATE attempts, jobs, runs`
	if !keepCache {
		q += `, result_cache`
	}
	_, err := s.pool.Exec(ctx, q)
	return err
}

type RunSpec struct {
	Suite       string
	JobType     string
	Config      []byte
	MaxAttempts int
	TimeoutMS   int
	NoCache     bool
}

type CaseIn struct {
	CaseID  string
	Payload []byte
}

type SubmitResult struct {
	RunID     string
	Jobs      int
	CacheHits int
	Deduped   int
}

type cached struct {
	result []byte
	score  *float64
	passed *bool
}

// CreateRun inserts a run and its jobs in one transaction. Each job is
// either answered from the result cache (status cached), parked behind an
// identical job that is already queued or running (status waiting), or
// queued for execution.
func (s *Store) CreateRun(ctx context.Context, spec RunSpec, cases []CaseIn) (SubmitResult, error) {
	var out SubmitResult
	if len(cases) == 0 {
		return out, errors.New("run has no cases")
	}
	config, err := cachekey.Canonical(spec.Config)
	if err != nil {
		return out, fmt.Errorf("config: %w", err)
	}
	keys := make([]string, len(cases))
	payloads := make([][]byte, len(cases))
	for i, c := range cases {
		p, err := cachekey.Canonical(c.Payload)
		if err != nil {
			return out, fmt.Errorf("case %q: %w", c.CaseID, err)
		}
		payloads[i] = p
		if keys[i], err = cachekey.Key(spec.JobType, config, p); err != nil {
			return out, err
		}
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return out, err
	}
	defer tx.Rollback(ctx)

	if err := tx.QueryRow(ctx,
		`INSERT INTO runs (suite, job_type, config, max_attempts, timeout_ms, no_cache)
		 VALUES ($1,$2,$3,$4,$5,$6) RETURNING id::text`,
		spec.Suite, spec.JobType, config, spec.MaxAttempts, spec.TimeoutMS, spec.NoCache,
	).Scan(&out.RunID); err != nil {
		return out, err
	}

	hits := map[string]cached{}
	inflight := map[string]bool{}
	if !spec.NoCache {
		rows, err := tx.Query(ctx, `SELECT cache_key, result, score, passed FROM result_cache WHERE cache_key = ANY($1)`, keys)
		if err != nil {
			return out, err
		}
		for rows.Next() {
			var k string
			var c cached
			if err := rows.Scan(&k, &c.result, &c.score, &c.passed); err != nil {
				rows.Close()
				return out, err
			}
			hits[k] = c
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return out, err
		}
		rows, err = tx.Query(ctx, `SELECT DISTINCT cache_key FROM jobs WHERE status IN ('queued','leased') AND cache_key = ANY($1)`, keys)
		if err != nil {
			return out, err
		}
		for rows.Next() {
			var k string
			if err := rows.Scan(&k); err != nil {
				rows.Close()
				return out, err
			}
			inflight[k] = true
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return out, err
		}
	}

	now := time.Now()
	hitCount := map[string]int64{}
	rowsIn := make([][]any, len(cases))
	for i, c := range cases {
		k := keys[i]
		status := "queued"
		var result []byte
		var score *float64
		var passed *bool
		var finished *time.Time
		switch {
		case spec.NoCache:
		case hits[k].result != nil:
			h := hits[k]
			status, result, score, passed, finished = "cached", h.result, h.score, h.passed, &now
			hitCount[k]++
			out.CacheHits++
		case inflight[k]:
			status = "waiting"
			out.Deduped++
		default:
			inflight[k] = true
		}
		rowsIn[i] = []any{out.RunID, c.CaseID, spec.JobType, payloads[i], k, status, spec.MaxAttempts, result, score, passed, finished}
	}
	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"jobs"},
		[]string{"run_id", "case_id", "job_type", "payload", "cache_key", "status", "max_attempts", "result", "score", "passed", "finished_at"},
		pgx.CopyFromRows(rowsIn)); err != nil {
		return out, err
	}
	if len(hitCount) > 0 {
		ks := make([]string, 0, len(hitCount))
		ns := make([]int64, 0, len(hitCount))
		for k, n := range hitCount {
			ks = append(ks, k)
			ns = append(ns, n)
		}
		if _, err := tx.Exec(ctx,
			`UPDATE result_cache c SET hits = c.hits + u.n FROM unnest($1::text[], $2::bigint[]) AS u(k, n) WHERE c.cache_key = u.k`,
			ks, ns); err != nil {
			return out, err
		}
	}
	out.Jobs = len(cases)
	return out, tx.Commit(ctx)
}

type LeasedJob struct {
	JobID     int64
	RunID     string
	CaseID    string
	JobType   string
	Config    []byte
	Payload   []byte
	Attempt   int
	TimeoutMS int
}

// Lease claims up to n runnable jobs for a worker. FOR UPDATE SKIP LOCKED
// lets many coordinators or lease calls run concurrently without blocking
// on each other or handing out the same job twice.
func (s *Store) Lease(ctx context.Context, workerID string, n int, jobTypes []string, lease time.Duration) ([]LeasedJob, error) {
	if jobTypes == nil {
		jobTypes = []string{}
	}
	rows, err := s.pool.Query(ctx, `
		WITH picked AS (
			SELECT id FROM jobs
			WHERE status = 'queued' AND next_run_at <= clock_timestamp()
			  AND (cardinality($3::text[]) = 0 OR job_type = ANY($3::text[]))
			ORDER BY next_run_at, id
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		UPDATE jobs j SET
			status = 'leased',
			attempts = j.attempts + 1,
			leased_by = $1,
			leased_at = clock_timestamp(),
			lease_expires_at = clock_timestamp() + ($4::bigint * interval '1 millisecond')
		FROM picked, runs r
		WHERE j.id = picked.id AND r.id = j.run_id
		RETURNING j.id, j.run_id::text, j.case_id, j.job_type, r.config, j.payload, j.attempts, r.timeout_ms`,
		workerID, n, jobTypes, lease.Milliseconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []LeasedJob
	for rows.Next() {
		var j LeasedJob
		if err := rows.Scan(&j.JobID, &j.RunID, &j.CaseID, &j.JobType, &j.Config, &j.Payload, &j.Attempt, &j.TimeoutMS); err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	sort.Slice(out, func(a, b int) bool { return out[a].JobID < out[b].JobID })
	return out, rows.Err()
}

// Renew extends leases still held by workerID and returns the ids it lost.
func (s *Store) Renew(ctx context.Context, workerID string, ids []int64, lease time.Duration) ([]int64, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := s.pool.Query(ctx, `
		UPDATE jobs SET lease_expires_at = clock_timestamp() + ($3::bigint * interval '1 millisecond')
		WHERE id = ANY($2) AND status = 'leased' AND leased_by = $1
		RETURNING id`, workerID, ids, lease.Milliseconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	held := map[int64]bool{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		held[id] = true
	}
	var lost []int64
	for _, id := range ids {
		if !held[id] {
			lost = append(lost, id)
		}
	}
	return lost, rows.Err()
}

type Report struct {
	WorkerID  string
	JobID     int64
	Attempt   int
	Success   bool
	Result    []byte
	Error     string
	Retryable bool
	ExecUS    int64
}

// ReportOutcome says what a report did to the job.
type ReportOutcome struct {
	Accepted bool
	Status   string // succeeded, queued (retry scheduled), failed
	RunID    string
	Resolved int // waiting jobs answered by this result
}

// resultSummary pulls the optional score and passed fields out of an
// executor result so they can be queried without parsing JSON.
func resultSummary(result []byte) (*float64, *bool) {
	var r struct {
		Score  *float64 `json:"score"`
		Passed *bool    `json:"passed"`
	}
	_ = json.Unmarshal(result, &r)
	return r.Score, r.Passed
}

// Backoff is exponential with equal jitter: half the window fixed, half random.
func Backoff(attempt int, base, max time.Duration) time.Duration {
	d := base << (attempt - 1)
	if d <= 0 || d > max {
		d = max
	}
	return d/2 + time.Duration(rand.Int64N(int64(d/2)+1))
}

type ReapResult struct {
	Expired  int // leases that timed out
	Failed   int // of those, jobs out of attempts
	Promoted int // orphaned waiters requeued
	Resolved int // orphaned waiters answered from cache
}

// Reap requeues jobs whose lease expired (worker crashed or hung) and
// repairs waiting jobs whose leader vanished. Safe to run concurrently.
func (s *Store) Reap(ctx context.Context) (ReapResult, error) {
	var out ReapResult
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return out, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		WITH exp AS (
			SELECT id, leased_by, leased_at, attempts, run_id FROM jobs
			WHERE status='leased' AND lease_expires_at < clock_timestamp()
			FOR UPDATE SKIP LOCKED
		)
		UPDATE jobs j SET
			status      = CASE WHEN j.attempts < j.max_attempts THEN 'queued' ELSE 'failed' END,
			next_run_at = clock_timestamp(),
			finished_at = CASE WHEN j.attempts < j.max_attempts THEN NULL ELSE clock_timestamp() END,
			error = 'lease expired: worker lost or too slow',
			leased_by = NULL, lease_expires_at = NULL
		FROM exp WHERE j.id = exp.id
		RETURNING j.id, exp.attempts, exp.run_id, exp.leased_by, exp.leased_at, j.status`)
	if err != nil {
		return out, err
	}
	type exp struct {
		id       int64
		attempt  int
		runID    string
		worker   string
		leasedAt time.Time
		status   string
	}
	var expired []exp
	for rows.Next() {
		var e exp
		if err := rows.Scan(&e.id, &e.attempt, &e.runID, &e.worker, &e.leasedAt, &e.status); err != nil {
			rows.Close()
			return out, err
		}
		expired = append(expired, e)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return out, err
	}
	for _, e := range expired {
		out.Expired++
		if e.status == "failed" {
			out.Failed++
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO attempts (job_id, attempt, run_id, worker_id, leased_at, outcome, error)
			VALUES ($1,$2,$3,$4,$5,'lease_expired','lease expired') ON CONFLICT DO NOTHING`,
			e.id, e.attempt, e.runID, e.worker, e.leasedAt); err != nil {
			return out, err
		}
	}

	// Waiters whose key landed in the cache (their leader committed while
	// the waiter's own submit transaction was still open).
	tag, err := tx.Exec(ctx, `
		UPDATE jobs w SET status='cached', result=c.result, score=c.score, passed=c.passed, finished_at=clock_timestamp()
		FROM result_cache c WHERE w.status='waiting' AND c.cache_key = w.cache_key`)
	if err != nil {
		return out, err
	}
	out.Resolved = int(tag.RowsAffected())
	// Waiters with no live leader: promote one per key.
	tag, err = tx.Exec(ctx, `
		UPDATE jobs SET status='queued', next_run_at=clock_timestamp()
		WHERE id IN (
			SELECT DISTINCT ON (w.cache_key) w.id FROM jobs w
			WHERE w.status='waiting'
			  AND NOT EXISTS (SELECT 1 FROM jobs l WHERE l.cache_key = w.cache_key AND l.status IN ('queued','leased'))
			ORDER BY w.cache_key, w.id
		)`)
	if err != nil {
		return out, err
	}
	out.Promoted = int(tag.RowsAffected())
	return out, tx.Commit(ctx)
}

// Depth counts non-terminal jobs by status, for the queue gauge.
func (s *Store) Depth(ctx context.Context) (map[string]int64, error) {
	rows, err := s.pool.Query(ctx, `SELECT status, count(*) FROM jobs WHERE status IN ('queued','leased','waiting') GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{"queued": 0, "leased": 0, "waiting": 0}
	for rows.Next() {
		var st string
		var n int64
		if err := rows.Scan(&st, &n); err != nil {
			return nil, err
		}
		out[st] = n
	}
	return out, rows.Err()
}
