package store

import (
	"context"
	"time"
)

// Report records the result of one attempt. See ReportBatch.
func (s *Store) Report(ctx context.Context, r Report, backoffBase, backoffMax time.Duration) (ReportOutcome, error) {
	outs, err := s.ReportBatch(ctx, []Report{r}, backoffBase, backoffMax)
	if err != nil {
		return ReportOutcome{}, err
	}
	return outs[0], nil
}

// ReportBatch applies attempt results with set-based SQL: a fixed number of
// statements and one commit per batch, whatever its size.
//
// Every report is fenced on (job, worker, attempt): a worker whose lease
// expired and was handed to someone else gets Accepted=false and its result
// is dropped. That fence also makes a resent batch harmless.
//
// On success the result goes into result_cache and answers every job
// waiting on the same cache key. On a retryable error with attempts left the
// job is requeued with exponential backoff; otherwise it fails and one
// waiter (if any) is promoted to run in its place.
func (s *Store) ReportBatch(ctx context.Context, rs []Report, backoffBase, backoffMax time.Duration) ([]ReportOutcome, error) {
	outs := make([]ReportOutcome, len(rs))
	if len(rs) == 0 {
		return outs, nil
	}
	idx := make(map[int64]int, len(rs))
	var (
		sIDs, fIDs           []int64
		sWorkers, fWorkers   []string
		sAttempts, fAttempts []int32
		sResults             []string
		sScores              []*float64
		sPassed              []*bool
		sExec, fExec         []int64
		fRetry               []bool
		fErrors              []string
		fDelay               []int64
	)
	for i, r := range rs {
		idx[r.JobID] = i
		if r.Success {
			res := r.Result
			if len(res) == 0 {
				res = []byte("{}")
			}
			score, passed := resultSummary(res)
			sIDs, sWorkers, sAttempts = append(sIDs, r.JobID), append(sWorkers, r.WorkerID), append(sAttempts, int32(r.Attempt))
			sResults, sScores, sPassed, sExec = append(sResults, string(res)), append(sScores, score), append(sPassed, passed), append(sExec, r.ExecUS)
		} else {
			fIDs, fWorkers, fAttempts = append(fIDs, r.JobID), append(fWorkers, r.WorkerID), append(fAttempts, int32(r.Attempt))
			fRetry, fErrors, fExec = append(fRetry, r.Retryable), append(fErrors, r.Error), append(fExec, r.ExecUS)
			fDelay = append(fDelay, Backoff(r.Attempt, backoffBase, backoffMax).Microseconds())
		}
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	if len(sIDs) > 0 {
		rows, err := tx.Query(ctx, `
			WITH input AS (
				SELECT * FROM unnest($1::bigint[], $2::text[], $3::int[], $4::text[], $5::float8[], $6::bool[], $7::bigint[])
					AS t(job_id, worker_id, attempt, result, score, passed, exec_us)
			), upd AS (
				UPDATE jobs j SET status='succeeded', finished_at=clock_timestamp(), result=i.result::jsonb,
				       score=i.score, passed=i.passed, error=NULL, leased_by=NULL, lease_expires_at=NULL
				FROM input i
				WHERE j.id=i.job_id AND j.status='leased' AND j.leased_by=i.worker_id AND j.attempts=i.attempt
				RETURNING j.id, j.run_id, j.cache_key, j.leased_at, i.worker_id, i.attempt, i.result, i.score, i.passed, i.exec_us
			), att AS (
				INSERT INTO attempts (job_id, attempt, run_id, worker_id, leased_at, outcome, exec_us)
				SELECT id, attempt, run_id, worker_id, leased_at, 'success', exec_us FROM upd
			), firsts AS (
				SELECT DISTINCT ON (cache_key) id, cache_key, result, score, passed FROM upd ORDER BY cache_key, id
			), cache AS (
				INSERT INTO result_cache (cache_key, result, score, passed, source_job)
				SELECT cache_key, result::jsonb, score, passed, id FROM firsts
				ON CONFLICT (cache_key) DO NOTHING
			), waiters AS (
				UPDATE jobs w SET status='cached', result=f.result::jsonb, score=f.score, passed=f.passed, finished_at=clock_timestamp()
				FROM firsts f WHERE w.status='waiting' AND w.cache_key=f.cache_key
				RETURNING w.cache_key
			), wc AS (
				SELECT cache_key, count(*) AS n FROM waiters GROUP BY cache_key
			)
			SELECT u.id, u.run_id::text, u.cache_key,
			       CASE WHEN f.id IS NULL THEN 0 ELSE coalesce(wc.n, 0) END
			FROM upd u
			LEFT JOIN firsts f ON f.id = u.id
			LEFT JOIN wc ON wc.cache_key = u.cache_key`,
			sIDs, sWorkers, sAttempts, sResults, sScores, sPassed, sExec)
		if err != nil {
			return nil, err
		}
		var hitKeys []string
		var hitNs []int64
		for rows.Next() {
			var id, n int64
			var runID, key string
			if err := rows.Scan(&id, &runID, &key, &n); err != nil {
				rows.Close()
				return nil, err
			}
			o := &outs[idx[id]]
			o.Accepted, o.Status, o.RunID, o.Resolved = true, "succeeded", runID, int(n)
			if n > 0 {
				hitKeys, hitNs = append(hitKeys, key), append(hitNs, n)
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
		if len(hitKeys) > 0 {
			if _, err := tx.Exec(ctx,
				`UPDATE result_cache c SET hits = c.hits + u.n FROM unnest($1::text[], $2::bigint[]) AS u(k, n) WHERE c.cache_key = u.k`,
				hitKeys, hitNs); err != nil {
				return nil, err
			}
		}
	}

	if len(fIDs) > 0 {
		rows, err := tx.Query(ctx, `
			WITH input AS (
				SELECT * FROM unnest($1::bigint[], $2::text[], $3::int[], $4::bool[], $5::text[], $6::bigint[], $7::bigint[])
					AS t(job_id, worker_id, attempt, retryable, error, delay_us, exec_us)
			), upd AS (
				UPDATE jobs j SET
					status      = CASE WHEN i.retryable AND j.attempts < j.max_attempts THEN 'queued' ELSE 'failed' END,
					next_run_at = CASE WHEN i.retryable AND j.attempts < j.max_attempts THEN clock_timestamp() + (i.delay_us * interval '1 microsecond') ELSE j.next_run_at END,
					finished_at = CASE WHEN i.retryable AND j.attempts < j.max_attempts THEN NULL ELSE clock_timestamp() END,
					error = i.error, leased_by = NULL, lease_expires_at = NULL
				FROM input i
				WHERE j.id=i.job_id AND j.status='leased' AND j.leased_by=i.worker_id AND j.attempts=i.attempt
				RETURNING j.id, j.run_id, j.cache_key, j.leased_at, j.status, i.worker_id, i.attempt, i.error, i.exec_us
			), att AS (
				INSERT INTO attempts (job_id, attempt, run_id, worker_id, leased_at, outcome, error, exec_us)
				SELECT id, attempt, run_id, worker_id, leased_at, 'error', nullif(error, ''), exec_us FROM upd
			)
			SELECT id, run_id::text, cache_key, status FROM upd`,
			fIDs, fWorkers, fAttempts, fRetry, fErrors, fDelay, fExec)
		if err != nil {
			return nil, err
		}
		var failedKeys []string
		for rows.Next() {
			var id int64
			var runID, key, status string
			if err := rows.Scan(&id, &runID, &key, &status); err != nil {
				rows.Close()
				return nil, err
			}
			o := &outs[idx[id]]
			o.Accepted, o.Status, o.RunID = true, status, runID
			if status == "failed" {
				failedKeys = append(failedKeys, key)
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
		if len(failedKeys) > 0 {
			// Promote one waiter per key so identical jobs still get a chance.
			if _, err := tx.Exec(ctx, `
				UPDATE jobs SET status='queued', next_run_at=clock_timestamp()
				WHERE id IN (
					SELECT DISTINCT ON (cache_key) id FROM jobs
					WHERE status='waiting' AND cache_key = ANY($1)
					ORDER BY cache_key, id
				)`, failedKeys); err != nil {
				return nil, err
			}
		}
	}
	return outs, tx.Commit(ctx)
}
