// Package e2e runs the coordinator and workers in-process against a real
// Postgres. Set EVALRUNNER_TEST_DSN to enable; tests skip otherwise.
package e2e

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	pb "github.com/armaan-k019/go-eval-runner/gen/evalrunnerv1"
	"github.com/armaan-k019/go-eval-runner/internal/coordinator"
	"github.com/armaan-k019/go-eval-runner/internal/executor"
	"github.com/armaan-k019/go-eval-runner/internal/metrics"
	"github.com/armaan-k019/go-eval-runner/internal/store"
	"github.com/armaan-k019/go-eval-runner/internal/worker"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func openStore(t *testing.T) *store.Store {
	t.Helper()
	dsn := os.Getenv("EVALRUNNER_TEST_DSN")
	if dsn == "" {
		t.Skip("EVALRUNNER_TEST_DSN not set")
	}
	ctx := context.Background()
	st, err := store.Open(ctx, dsn, 16)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := st.Truncate(ctx, false); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	return st
}

// countingExec wraps an executor and counts calls per case payload.
type countingExec struct {
	inner executor.Executor
	mu    sync.Mutex
	calls map[string]int
	total int
}

func (c *countingExec) Name() string { return c.inner.Name() }
func (c *countingExec) Run(ctx context.Context, j executor.Job) (executor.Result, error) {
	c.mu.Lock()
	if c.calls == nil {
		c.calls = map[string]int{}
	}
	c.calls[string(j.Payload)]++
	c.total++
	c.mu.Unlock()
	return c.inner.Run(ctx, j)
}

type cluster struct {
	client pb.CoordinatorClient
	srv    *coordinator.Server
	exec   *countingExec
	stop   func()
}

func startCluster(t *testing.T, st *store.Store, workers, conc int, cfg coordinator.Config) *cluster {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	srv := coordinator.New(st, cfg, metrics.NewCoordinator(prometheus.NewRegistry()), quiet)
	go srv.RunReaper(ctx)
	lis := bufconn.Listen(1 << 20)
	g := grpc.NewServer()
	pb.RegisterCoordinatorServer(g, srv)
	go g.Serve(lis)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	client := pb.NewCoordinatorClient(conn)
	ce := &countingExec{inner: executor.Mock{}}
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		reg := executor.Registry{}
		reg.Register(ce)
		w := worker.New(worker.Config{ID: fmt.Sprintf("w%d", i), Concurrency: conc, LeaseWait: 200 * time.Millisecond},
			client, reg, metrics.NewWorker(prometheus.NewRegistry()), quiet)
		wg.Add(1)
		go func() { defer wg.Done(); w.Run(ctx) }()
	}
	c := &cluster{client: client, srv: srv, exec: ce}
	c.stop = func() { cancel(); wg.Wait(); g.Stop(); conn.Close() }
	t.Cleanup(c.stop)
	return c
}

func testConfig() coordinator.Config {
	cfg := coordinator.DefaultConfig()
	cfg.BackoffBase = 5 * time.Millisecond
	cfg.BackoffMax = 20 * time.Millisecond
	cfg.ReapInterval = 50 * time.Millisecond
	cfg.PollInterval = 10 * time.Millisecond
	return cfg
}

func cases(payloads ...string) []*pb.Case {
	out := make([]*pb.Case, len(payloads))
	for i, p := range payloads {
		out[i] = &pb.Case{CaseId: fmt.Sprintf("c%d", i), PayloadJson: []byte(p)}
	}
	return out
}

func submitAndWait(t *testing.T, c pb.CoordinatorClient, req *pb.SubmitRunRequest) *pb.RunStatus {
	t.Helper()
	ctx := context.Background()
	resp, err := c.SubmitRun(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		st, err := c.GetRun(ctx, &pb.GetRunRequest{RunId: resp.RunId})
		if err != nil {
			t.Fatal(err)
		}
		if st.Done {
			return st
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("run %s did not finish", resp.RunId)
	return nil
}

func TestRetriesAndPermanentFailures(t *testing.T) {
	st := openStore(t)
	c := startCluster(t, st, 1, 4, testConfig())
	res := submitAndWait(t, c.client, &pb.SubmitRunRequest{
		Suite: "retry", JobType: "mock", MaxAttempts: 3,
		Cases: cases(`{"id":"ok"}`, `{"id":"flaky","fail_attempts":2}`, `{"id":"dead","fail_attempts":9}`, `{"id":"perm","permanent":true}`),
	})
	if res.Succeeded != 2 || res.Failed != 2 {
		t.Fatalf("want 2 succeeded 2 failed, got %+v", res)
	}
	// ok:1, flaky:3, dead:3, perm:1 attempts.
	if res.ExecutorCalls != 8 {
		t.Fatalf("want 8 executor calls, got %d", res.ExecutorCalls)
	}
	if res.Retries != 4 {
		t.Fatalf("want 4 retries, got %d", res.Retries)
	}
}

func TestResultCacheAcrossRuns(t *testing.T) {
	st := openStore(t)
	c := startCluster(t, st, 2, 4, testConfig())
	var ps []string
	for i := 0; i < 20; i++ {
		ps = append(ps, fmt.Sprintf(`{"task":%d}`, i))
	}
	req := &pb.SubmitRunRequest{Suite: "cache", JobType: "mock", ConfigJson: []byte(`{"pass_threshold":0.5}`), MaxAttempts: 2, Cases: cases(ps...)}
	cold := submitAndWait(t, c.client, req)
	if cold.Succeeded != 20 || cold.Cached != 0 {
		t.Fatalf("cold run: %+v", cold)
	}
	// Same cases, config keys reordered: still a cache hit.
	req.ConfigJson = []byte(`{ "pass_threshold" : 0.5 }`)
	warm := submitAndWait(t, c.client, req)
	if warm.Cached != 20 || warm.ExecutorCalls != 0 || warm.CacheSavedFraction != 1 {
		t.Fatalf("warm run: %+v", warm)
	}
	if math.Abs(warm.MeanScore-cold.MeanScore) > 1e-9 {
		t.Fatalf("cached scores differ: %v vs %v", warm.MeanScore, cold.MeanScore)
	}
	// no_cache forces execution.
	req.NoCache = true
	forced := submitAndWait(t, c.client, req)
	if forced.Cached != 0 || forced.ExecutorCalls != 20 {
		t.Fatalf("no_cache run: %+v", forced)
	}
}

func TestInflightDedup(t *testing.T) {
	st := openStore(t)
	c := startCluster(t, st, 2, 8, testConfig())
	var ps []string
	for i := 0; i < 30; i++ {
		ps = append(ps, fmt.Sprintf(`{"task":%d,"latency_ms":20}`, i%3))
	}
	res := submitAndWait(t, c.client, &pb.SubmitRunRequest{Suite: "dedup", JobType: "mock", MaxAttempts: 2, Cases: cases(ps...)})
	if res.Succeeded != 3 || res.Cached != 27 || res.ExecutorCalls != 3 {
		t.Fatalf("want 3 executed and 27 cached, got %+v", res)
	}
	for p, n := range c.exec.calls {
		if n != 1 {
			t.Errorf("payload %s executed %d times", p, n)
		}
	}
}

func TestEachJobLeasedOnceUnderConcurrency(t *testing.T) {
	st := openStore(t)
	c := startCluster(t, st, 4, 16, testConfig())
	var ps []string
	for i := 0; i < 400; i++ {
		ps = append(ps, fmt.Sprintf(`{"task":%d,"latency_ms":2}`, i))
	}
	res := submitAndWait(t, c.client, &pb.SubmitRunRequest{Suite: "conc", JobType: "mock", MaxAttempts: 1, Cases: cases(ps...)})
	if res.Succeeded != 400 || res.ExecutorCalls != 400 {
		t.Fatalf("got %+v", res)
	}
	if c.exec.total != 400 {
		t.Fatalf("executor ran %d times for 400 jobs", c.exec.total)
	}
}

func TestLeaseExpiryAndStaleReportFencing(t *testing.T) {
	st := openStore(t)
	ctx := context.Background()
	sub, err := st.CreateRun(ctx, store.RunSpec{Suite: "lease", JobType: "mock", Config: []byte(`{}`), MaxAttempts: 3, TimeoutMS: 1000},
		[]store.CaseIn{{CaseID: "a", Payload: []byte(`{"x":1}`)}})
	if err != nil {
		t.Fatal(err)
	}
	// Worker "ghost" leases the job and dies.
	jobs, err := st.Lease(ctx, "ghost", 1, nil, 50*time.Millisecond)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("lease: %v %v", jobs, err)
	}
	time.Sleep(80 * time.Millisecond)
	rr, err := st.Reap(ctx)
	if err != nil || rr.Expired != 1 {
		t.Fatalf("reap: %+v %v", rr, err)
	}
	// Renew from the dead worker reports the job as lost.
	lost, err := st.Renew(ctx, "ghost", []int64{jobs[0].JobID}, time.Second)
	if err != nil || len(lost) != 1 {
		t.Fatalf("renew: %v %v", lost, err)
	}
	// A live worker picks it up as attempt 2.
	again, err := st.Lease(ctx, "live", 1, nil, time.Minute)
	if err != nil || len(again) != 1 || again[0].Attempt != 2 {
		t.Fatalf("re-lease: %+v %v", again, err)
	}
	// The ghost's late report is rejected.
	out, err := st.Report(ctx, store.Report{WorkerID: "ghost", JobID: jobs[0].JobID, Attempt: 1, Success: true, Result: []byte(`{"score":0}`)}, time.Millisecond, time.Millisecond)
	if err != nil || out.Accepted {
		t.Fatalf("stale report accepted: %+v %v", out, err)
	}
	out, err = st.Report(ctx, store.Report{WorkerID: "live", JobID: jobs[0].JobID, Attempt: 2, Success: true, Result: []byte(`{"score":0.9,"passed":true}`)}, time.Millisecond, time.Millisecond)
	if err != nil || !out.Accepted || out.Status != "succeeded" {
		t.Fatalf("live report: %+v %v", out, err)
	}
	stats, err := st.RunStats(ctx, sub.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if !stats.Done() || stats.Succeeded != 1 || stats.LeaseExpirations != 1 || stats.MeanScore != 0.9 {
		t.Fatalf("stats: %+v", stats)
	}
}

func TestWorkerCrashRecovery(t *testing.T) {
	st := openStore(t)
	cfg := testConfig()
	cfg.LeaseDuration = 300 * time.Millisecond
	ctx := context.Background()
	// A job leased by a worker that never comes back.
	_, err := st.CreateRun(ctx, store.RunSpec{Suite: "crash", JobType: "mock", Config: []byte(`{}`), MaxAttempts: 3, TimeoutMS: 1000},
		[]store.CaseIn{{CaseID: "a", Payload: []byte(`{"y":1}`)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Lease(ctx, "crashed", 1, nil, cfg.LeaseDuration); err != nil {
		t.Fatal(err)
	}
	c := startCluster(t, st, 1, 2, cfg)
	// A second run behind it; the cluster must finish both.
	res := submitAndWait(t, c.client, &pb.SubmitRunRequest{Suite: "after", JobType: "mock", MaxAttempts: 2, Cases: cases(`{"z":1}`)})
	if res.Succeeded != 1 {
		t.Fatalf("%+v", res)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		d, _ := st.Depth(ctx)
		if d["queued"]+d["leased"]+d["waiting"] == 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("crashed worker's job was never recovered")
}
