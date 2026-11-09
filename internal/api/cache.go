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
//
// Entries carry their own freshness deadline and outlive it in Redis, which
// gives stale-while-revalidate: when an entry goes stale, one caller across
// all replicas wins a short Redis lock and recomputes, and everyone else keeps
// serving the stale copy instead of stampeding Postgres. Finished runs never
// change, so they stay fresh for DoneTTL.
type RedisStats struct {
	Src     StatsSource
	RDB     *redis.Client
	TTL     time.Duration // freshness of a live run's stats
	DoneTTL time.Duration // freshness of a finished run's stats
	OnMiss  func()        // called once per Postgres computation (metrics)
}

type cacheEntry struct {
	Stats      store.RunStats `json:"stats"`
	FreshUntil int64          `json:"fresh_until_ms"`
}

const (
	staleKeep   = 30 * time.Second // how long a stale live entry may still be served
	lockTTL     = 5 * time.Second  // bounds a recompute whose owner died
	missWait    = 2 * time.Second  // how long a cold-miss loser waits for the winner
	missPollGap = 10 * time.Millisecond
)

func statsKey(runID string) string { return "evalrunner:stats:" + runID }
func lockKey(runID string) string  { return "evalrunner:stats-lock:" + runID }

func (c *RedisStats) get(ctx context.Context, runID string) (cacheEntry, bool) {
	var e cacheEntry
	b, err := c.RDB.Get(ctx, statsKey(runID)).Bytes()
	if err != nil || json.Unmarshal(b, &e) != nil {
		return e, false
	}
	return e, true
}

func (c *RedisStats) RunStats(ctx context.Context, runID string) (store.RunStats, error) {
	now := time.Now().UnixMilli()
	e, ok := c.get(ctx, runID)
	if ok && now < e.FreshUntil {
		return e.Stats, nil
	}
	won, err := c.RDB.SetNX(ctx, lockKey(runID), 1, lockTTL).Result()
	if err != nil {
		// Redis unavailable: fall back to Postgres. An outage costs load,
		// not correctness.
		return c.compute(ctx, runID)
	}
	if won {
		defer c.RDB.Del(context.WithoutCancel(ctx), lockKey(runID))
		return c.compute(ctx, runID)
	}
	if ok {
		return e.Stats, nil // someone else is refreshing; serve stale
	}
	// Cold miss and another caller is computing: wait for its result.
	deadline := time.Now().Add(missWait)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return store.RunStats{}, ctx.Err()
		case <-time.After(missPollGap):
		}
		if e, ok := c.get(ctx, runID); ok {
			return e.Stats, nil
		}
	}
	return c.compute(ctx, runID)
}

func (c *RedisStats) compute(ctx context.Context, runID string) (store.RunStats, error) {
	st, err := c.Src.RunStats(ctx, runID)
	if err != nil {
		return st, err
	}
	if c.OnMiss != nil {
		c.OnMiss()
	}
	fresh, keep := c.TTL, c.TTL+staleKeep
	if st.Done() {
		fresh, keep = c.DoneTTL, c.DoneTTL
	}
	e := cacheEntry{Stats: st, FreshUntil: time.Now().Add(fresh).UnixMilli()}
	if b, err := json.Marshal(e); err == nil {
		c.RDB.Set(ctx, statsKey(runID), b, keep)
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
