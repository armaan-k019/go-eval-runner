package executor

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"time"
)

// MockConfig shapes the synthetic workload. It lets the runner be load
// tested without any real eval or API key.
type MockConfig struct {
	// Simulated executor latency is lognormal with this median and sigma.
	LatencyMedianMS float64 `json:"latency_median_ms"`
	LatencySigma    float64 `json:"latency_sigma"`
	// Probability that an attempt fails with a transient (retryable) error.
	FailRate float64 `json:"fail_rate"`
	// Score threshold for passed.
	PassThreshold float64 `json:"pass_threshold"`
}

// MockPayload fields the mock reads; everything else is ignored.
type mockPayload struct {
	// Force the first N attempts to fail, for deterministic retry tests.
	FailAttempts int `json:"fail_attempts"`
	// Fail permanently, for non-retry tests.
	Permanent bool `json:"permanent"`
	// Override latency for this case.
	LatencyMS *float64 `json:"latency_ms"`
}

// Mock is a synthetic eval: score is a deterministic hash of the payload,
// latency and transient failures are random per attempt.
type Mock struct{}

func (Mock) Name() string { return "mock" }

func payloadHash(payload []byte) uint64 {
	h := sha256.Sum256(payload)
	return binary.LittleEndian.Uint64(h[:8])
}

func (Mock) Run(ctx context.Context, job Job) (Result, error) {
	cfg := MockConfig{PassThreshold: 0.5}
	if len(job.Config) > 0 {
		if err := json.Unmarshal(job.Config, &cfg); err != nil {
			return Result{}, Permanent(fmt.Errorf("mock config: %w", err))
		}
	}
	var p mockPayload
	if len(job.Payload) > 0 {
		if err := json.Unmarshal(job.Payload, &p); err != nil {
			return Result{}, Permanent(fmt.Errorf("mock payload: %w", err))
		}
	}
	// Latency and transient failures are independent random draws per
	// attempt (the score, below, is deterministic per payload).

	latency := 0.0
	switch {
	case p.LatencyMS != nil:
		latency = *p.LatencyMS
	case cfg.LatencyMedianMS > 0:
		latency = cfg.LatencyMedianMS * math.Exp(cfg.LatencySigma*rand.NormFloat64())
	}
	if latency > 0 {
		t := time.NewTimer(time.Duration(latency * float64(time.Millisecond)))
		select {
		case <-ctx.Done():
			t.Stop()
			return Result{}, ctx.Err()
		case <-t.C:
		}
	}

	if p.Permanent {
		return Result{}, Permanent(errors.New("mock: permanent failure requested"))
	}
	if job.Attempt <= p.FailAttempts {
		return Result{}, fmt.Errorf("mock: forced transient failure on attempt %d", job.Attempt)
	}
	if cfg.FailRate > 0 && rand.Float64() < cfg.FailRate {
		return Result{}, errors.New("mock: random transient failure")
	}

	h1 := payloadHash(job.Payload)
	score := float64(h1%10000) / 10000.0
	raw, _ := json.Marshal(map[string]any{
		"score":   score,
		"passed":  score >= cfg.PassThreshold,
		"metrics": map[string]float64{"latency_ms": latency},
	})
	return Result{Raw: raw}, nil
}
