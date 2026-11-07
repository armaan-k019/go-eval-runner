package store

import (
	"context"
	"time"
)

// RunStats is everything the report and benchmark harness need about a run,
// computed in SQL from the jobs and attempts tables.
type RunStats struct {
	RunID            string
	Suite            string
	Total            int64
	Queued           int64
	Leased           int64
	Waiting          int64
	Succeeded        int64
	Cached           int64
	Failed           int64
	Retries          int64
	LeaseExpirations int64
	ExecutorCalls    int64
	WallSeconds      float64
	JobsPerMinute    float64
	AttemptP50MS     float64
	AttemptP95MS     float64
	AttemptP99MS     float64
	ExecP50MS        float64
	ExecP95MS        float64
	OverheadP50MS    float64
	OverheadP95MS    float64
	MeanScore        float64
	PassRate         float64
	CacheSaved       float64
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
