package store

import (
	"context"
	"time"
)

// RunStats is everything the report and benchmark harness need about a run,
// computed in SQL from the jobs and attempts tables.
type RunStats struct {
	RunID            string  `json:"run_id"`
	Suite            string  `json:"suite"`
	Total            int64   `json:"total"`
	Queued           int64   `json:"queued"`
	Leased           int64   `json:"leased"`
	Waiting          int64   `json:"waiting"`
	Succeeded        int64   `json:"succeeded"`
	Cached           int64   `json:"cached"`
	Failed           int64   `json:"failed"`
	Retries          int64   `json:"retries"`
	LeaseExpirations int64   `json:"lease_expirations"`
	ExecutorCalls    int64   `json:"executor_calls"`
	WallSeconds      float64 `json:"wall_seconds"`
	JobsPerMinute    float64 `json:"jobs_per_minute"`
	AttemptP50MS     float64 `json:"attempt_p50_ms"`
	AttemptP95MS     float64 `json:"attempt_p95_ms"`
	AttemptP99MS     float64 `json:"attempt_p99_ms"`
	ExecP50MS        float64 `json:"exec_p50_ms"`
	ExecP95MS        float64 `json:"exec_p95_ms"`
	OverheadP50MS    float64 `json:"overhead_p50_ms"`
	OverheadP95MS    float64 `json:"overhead_p95_ms"`
	MeanScore        float64 `json:"mean_score"`
	PassRate         float64 `json:"pass_rate"`
	CacheSaved       float64 `json:"cache_saved"`
}

func (r RunStats) Done() bool { return r.Queued+r.Leased+r.Waiting == 0 }

func (s *Store) RunStats(ctx context.Context, runID string) (RunStats, error) {
	st := RunStats{RunID: runID}
	var created time.Time
	var lastFinished *time.Time
	var meanScore, passRate *float64
	err := s.pool.QueryRow(ctx, `
		SELECT r.suite, r.created_at,
		       count(j.id),
		       count(*) FILTER (WHERE j.status='queued'),
		       count(*) FILTER (WHERE j.status='leased'),
		       count(*) FILTER (WHERE j.status='waiting'),
		       count(*) FILTER (WHERE j.status='succeeded'),
		       count(*) FILTER (WHERE j.status='cached'),
		       count(*) FILTER (WHERE j.status='failed'),
		       max(j.finished_at),
		       avg(j.score) FILTER (WHERE j.status IN ('succeeded','cached')),
		       avg(CASE WHEN j.passed THEN 1.0 ELSE 0.0 END) FILTER (WHERE j.status IN ('succeeded','cached') AND j.passed IS NOT NULL)
		FROM runs r LEFT JOIN jobs j ON j.run_id = r.id
		WHERE r.id = $1
		GROUP BY r.suite, r.created_at`, runID,
	).Scan(&st.Suite, &created, &st.Total, &st.Queued, &st.Leased, &st.Waiting,
		&st.Succeeded, &st.Cached, &st.Failed, &lastFinished, &meanScore, &passRate)
	if err != nil {
		return st, err
	}
	if meanScore != nil {
		st.MeanScore = *meanScore
	}
	if passRate != nil {
		st.PassRate = *passRate
	}
	end := time.Now()
	if st.Done() && lastFinished != nil {
		end = *lastFinished
	}
	st.WallSeconds = end.Sub(created).Seconds()
	completed := st.Succeeded + st.Cached + st.Failed
	if st.WallSeconds > 0 {
		st.JobsPerMinute = float64(completed) / st.WallSeconds * 60
	}
	if st.Total > 0 {
		st.CacheSaved = float64(st.Cached) / float64(st.Total)
	}

	var a50, a95, a99, e50, e95, o50, o95 *float64
	err = s.pool.QueryRow(ctx, `
		WITH a AS (
			SELECT extract(epoch FROM reported_at - leased_at) * 1000.0 AS lat_ms,
			       exec_us / 1000.0 AS exec_ms, outcome, attempt
			FROM attempts WHERE run_id = $1
		)
		SELECT count(*),
		       count(*) FILTER (WHERE attempt > 1),
		       count(*) FILTER (WHERE outcome = 'lease_expired'),
		       percentile_cont(0.50) WITHIN GROUP (ORDER BY lat_ms) FILTER (WHERE outcome <> 'lease_expired'),
		       percentile_cont(0.95) WITHIN GROUP (ORDER BY lat_ms) FILTER (WHERE outcome <> 'lease_expired'),
		       percentile_cont(0.99) WITHIN GROUP (ORDER BY lat_ms) FILTER (WHERE outcome <> 'lease_expired'),
		       percentile_cont(0.50) WITHIN GROUP (ORDER BY exec_ms) FILTER (WHERE outcome <> 'lease_expired'),
		       percentile_cont(0.95) WITHIN GROUP (ORDER BY exec_ms) FILTER (WHERE outcome <> 'lease_expired'),
		       percentile_cont(0.50) WITHIN GROUP (ORDER BY lat_ms - exec_ms) FILTER (WHERE outcome <> 'lease_expired'),
		       percentile_cont(0.95) WITHIN GROUP (ORDER BY lat_ms - exec_ms) FILTER (WHERE outcome <> 'lease_expired')
		FROM a`, runID,
	).Scan(&st.ExecutorCalls, &st.Retries, &st.LeaseExpirations, &a50, &a95, &a99, &e50, &e95, &o50, &o95)
	if err != nil {
		return st, err
	}
	for _, p := range []struct {
		dst *float64
		src *float64
	}{{&st.AttemptP50MS, a50}, {&st.AttemptP95MS, a95}, {&st.AttemptP99MS, a99}, {&st.ExecP50MS, e50}, {&st.ExecP95MS, e95}, {&st.OverheadP50MS, o50}, {&st.OverheadP95MS, o95}} {
		if p.src != nil {
			*p.dst = *p.src
		}
	}
	return st, nil
}

// RunSummary is one row of the run list: identity plus status counts, cheap
// enough to compute for many runs in one query.
type RunSummary struct {
	RunID     string    `json:"run_id"`
	Suite     string    `json:"suite"`
	JobType   string    `json:"job_type"`
	CreatedAt time.Time `json:"created_at"`
	Total     int64     `json:"total"`
	Pending   int64     `json:"pending"`
	Succeeded int64     `json:"succeeded"`
	Cached    int64     `json:"cached"`
	Failed    int64     `json:"failed"`
}

// ListRuns returns the most recent runs, newest first.
func (s *Store) ListRuns(ctx context.Context, limit int) ([]RunSummary, error) {
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	rows, err := s.pool.Query(ctx, `
		SELECT r.id::text, r.suite, r.job_type, r.created_at,
		       count(j.id),
		       count(*) FILTER (WHERE j.status IN ('queued','leased','waiting')),
		       count(*) FILTER (WHERE j.status='succeeded'),
		       count(*) FILTER (WHERE j.status='cached'),
		       count(*) FILTER (WHERE j.status='failed')
		FROM (SELECT * FROM runs ORDER BY created_at DESC LIMIT $1) r
		LEFT JOIN jobs j ON j.run_id = r.id
		GROUP BY r.id, r.suite, r.job_type, r.created_at
		ORDER BY r.created_at DESC`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []RunSummary{}
	for rows.Next() {
		var r RunSummary
		if err := rows.Scan(&r.RunID, &r.Suite, &r.JobType, &r.CreatedAt, &r.Total, &r.Pending, &r.Succeeded, &r.Cached, &r.Failed); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
