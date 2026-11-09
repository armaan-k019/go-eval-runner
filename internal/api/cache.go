package api

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/armaan-k019/go-eval-runner/internal/store"
)

// StatsSource computes the stats for one run.
type StatsSource interface {
	RunStats(ctx context.Context, runID string) (store.RunStats, error)
}

// RedisStats caches run stats in Redis so that many dashboard viewers, spread
// over several API replicas, share one Postgres aggregate per run per TTL.
// Finished runs never change, so they are kept for DoneTTL instead.
type RedisStats struct {
	Src     StatsSource
	RDB     *redis.Client
	TTL     time.Duration // live runs
	DoneTTL time.Duration // finished runs
	OnMiss  func()        // called once per Postgres computation (metrics)
}

func statsKey(runID string) string { return "evalrunner:stats:" + runID }

func (c *RedisStats) RunStats(ctx context.Context, runID string) (store.RunStats, error) {
	var st store.RunStats
	if b, err := c.RDB.Get(ctx, statsKey(runID)).Bytes(); err == nil {
		if json.Unmarshal(b, &st) == nil {
			return st, nil
		}
	}
	// Miss, or Redis unavailable: fall back to Postgres. A Redis outage costs
	// load, not correctness.
	st, err := c.Src.RunStats(ctx, runID)
	if err != nil {
		return st, err
	}
	if c.OnMiss != nil {
		c.OnMiss()
	}
	ttl := c.TTL
	if st.Done() {
		ttl = c.DoneTTL
	}
	if b, err := json.Marshal(st); err == nil {
		c.RDB.Set(ctx, statsKey(runID), b, ttl)
	}
	return st, nil
}

// countingSource counts Postgres computations when no cache is configured,
// so the benchmark can compare database load with and without Redis.
type countingSource struct {
	src    StatsSource
	onCall func()
}

func (c countingSource) RunStats(ctx context.Context, runID string) (store.RunStats, error) {
	st, err := c.src.RunStats(ctx, runID)
	if err == nil && c.onCall != nil {
		c.onCall()
	}
	return st, err
}
