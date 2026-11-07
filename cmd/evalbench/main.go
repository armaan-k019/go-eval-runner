// Command evalbench runs one load-test scenario end to end and writes a JSON
// record of the setup and the measured results to bench/runs/.
//
// It starts a real coordinator process and W real worker processes (the
// binaries in ./bin), talks to them over gRPC on localhost, and reads the
// stats the coordinator computes from Postgres. Nothing is simulated except
// the eval itself, which is the mock executor unless -type exec is given.
//
//	evalbench -name scale-w4 -workers 4 -concurrency 16 -n 4000 -config '{"latency_median_ms":200,"latency_sigma":0.5}'
//	evalbench summarize > bench/RESULTS.md
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/armaan-k019/go-eval-runner/gen/evalrunnerv1"
	"github.com/armaan-k019/go-eval-runner/internal/store"
)

type Setup struct {
	Name         string          `json:"name"`
	Description  string          `json:"description"`
	Workers      int             `json:"workers"`
	Concurrency  int             `json:"concurrency_per_worker"`
	Cases        int             `json:"cases"`
	Unique       int             `json:"unique_payloads"`
	JobType      string          `json:"job_type"`
	Config       json.RawMessage `json:"executor_config"`
	MaxAttempts  int             `json:"max_attempts"`
	Lease        string          `json:"lease"`
	KillWorkerAt string          `json:"kill_worker_after,omitempty"`
	Passes       []string        `json:"passes"`
	CasesFile    string          `json:"cases_file,omitempty"`
	BatchReports bool            `json:"batch_reports"`
	Host         Host            `json:"host"`
	GitCommit    string          `json:"git_commit"`
	GitDirty     bool            `json:"git_dirty"`
	StartedAt    time.Time       `json:"started_at"`
}

type Host struct {
	CPUModel  string `json:"cpu_model"`
	CPUs      int    `json:"logical_cpus"`
	MemGB     int    `json:"mem_gb"`
	OS        string `json:"os"`
	GoVersion string `json:"go_version"`
	Postgres  string `json:"postgres"`
	Topology  string `json:"topology"`
}

type Pass struct {
	Label     string          `json:"label"`
	Submit    json.RawMessage `json:"submit"`
	Stats     store.RunStats  `json:"stats"`
	KilledPID int             `json:"killed_worker_pid,omitempty"`
}

type Record struct {
	Setup  Setup  `json:"setup"`
	Passes []Pass `json:"passes"`
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "summarize" {
		summarize(os.Args[2:])
		return
	}
	var s Setup
	flag.StringVar(&s.Name, "name", "", "scenario name (required)")
	flag.StringVar(&s.Description, "desc", "", "one-line description")
	flag.IntVar(&s.Workers, "workers", 2, "worker processes")
	flag.IntVar(&s.Concurrency, "concurrency", 16, "concurrent jobs per worker")
	flag.IntVar(&s.Cases, "n", 2000, "cases per pass (ignored with -cases)")
	flag.IntVar(&s.Unique, "unique", 0, "distinct payloads (0 = all distinct)")
	flag.StringVar(&s.JobType, "type", "mock", "job type")
	cfg := flag.String("config", `{}`, "executor config JSON")
	flag.IntVar(&s.MaxAttempts, "max-attempts", 3, "attempts per job")
	lease := flag.Duration("lease", 15*time.Second, "coordinator lease duration")
	kill := flag.Duration("kill-worker-after", 0, "SIGKILL one worker this long into pass 1")
	warm := flag.Bool("warm", false, "add a second pass with identical cases (cache hits)")
	changed := flag.Float64("changed", 0, "add a pass where this fraction of cases has a new payload")
	flag.BoolVar(&s.BatchReports, "batch-reports", true, "workers batch result reports")
	flag.StringVar(&s.CasesFile, "cases", "", "JSONL cases file instead of generated mock cases")
	dsn := flag.String("dsn", "postgres://eval:eval@localhost:5432/eval?sslmode=disable", "Postgres DSN")
	out := flag.String("out", "bench/runs", "output directory")
	bin := flag.String("bin", "bin", "directory with coordinator and worker binaries")
	flag.Parse()
	if s.Name == "" {
		fail(fmt.Errorf("-name is required"))
	}
	s.Config = json.RawMessage(*cfg)
	s.Lease = lease.String()
	if *kill > 0 {
		s.KillWorkerAt = kill.String()
	}
	s.StartedAt = time.Now().UTC()
	s.Host = hostInfo(*dsn)
	s.GitCommit, s.GitDirty = gitInfo()

	ctx := context.Background()
	st, err := store.Open(ctx, *dsn, 4)
	if err != nil {
		fail(err)
	}
	if err := st.Migrate(ctx); err != nil {
		fail(err)
	}
	if err := st.Truncate(ctx, false); err != nil {
		fail(err)
	}

	coord := start(filepath.Join(*bin, "coordinator"), "-addr", "127.0.0.1:17070", "-metrics-addr", "127.0.0.1:19090", "-dsn", *dsn, "-lease", lease.String())
	defer stop(coord)
	conn, err := grpc.NewClient("127.0.0.1:17070", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(256<<20)))
	if err != nil {
		fail(err)
	}
	client := pb.NewCoordinatorClient(conn)
	waitReady(client)

	var workers []*exec.Cmd
	for i := 0; i < s.Workers; i++ {
		w := start(filepath.Join(*bin, "worker"), "-coordinator", "127.0.0.1:17070", "-id", fmt.Sprintf("bench-w%d", i),
			"-concurrency", fmt.Sprint(s.Concurrency), "-metrics-addr", "", fmt.Sprintf("-batch-reports=%v", s.BatchReports))
		workers = append(workers, w)
	}
	defer func() {
		for _, w := range workers {
			stop(w)
		}
	}()
	time.Sleep(300 * time.Millisecond) // let workers connect and start long-polling

	base := buildCases(s)
	s.Cases = len(base)
	rec := Record{Setup: s}
	passes := []struct {
		label string
		cases []*pb.Case
	}{{"cold", base}}
	if *warm {
		passes = append(passes, struct {
			label string
			cases []*pb.Case
		}{"warm (identical rerun)", base})
	}
	if *changed > 0 {
		passes = append(passes, struct {
			label string
			cases []*pb.Case
		}{fmt.Sprintf("rerun with %.0f%% of cases changed", *changed*100), mutate(base, *changed)})
	}
	for i, p := range passes {
		rec.Setup.Passes = append(rec.Setup.Passes, p.label)
		var killed int
		var killTimer *time.Timer
		if i == 0 && *kill > 0 && len(workers) > 0 {
			victim := workers[0]
			killTimer = time.AfterFunc(*kill, func() {
				victim.Process.Signal(syscall.SIGKILL)
			})
			killed = victim.Process.Pid
		}
		resp, err := client.SubmitRun(ctx, &pb.SubmitRunRequest{
			Suite: s.Name + "/" + p.label, JobType: s.JobType, ConfigJson: s.Config, Cases: p.cases,
			MaxAttempts: int32(s.MaxAttempts), TimeoutMs: 120000,
		})
		if err != nil {
			fail(err)
		}
		fmt.Fprintf(os.Stderr, "[%s] pass %q: run %s, %d jobs, %d cache hits, %d deduped\n", s.Name, p.label, resp.RunId, resp.Jobs, resp.CacheHits, resp.Deduped)
		for {
			rs, err := st.RunStats(ctx, resp.RunId)
			if err != nil {
				fail(err)
			}
			if rs.Done() {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if killTimer != nil {
			killTimer.Stop()
		}
		// Re-read once finished so every attempt row is visible.
		rs, err := st.RunStats(ctx, resp.RunId)
		if err != nil {
			fail(err)
		}
		sub, _ := json.Marshal(resp)
		rec.Passes = append(rec.Passes, Pass{Label: p.label, Submit: sub, Stats: rs, KilledPID: killed})
		fmt.Fprintf(os.Stderr, "[%s]   %.1fs wall, %.0f jobs/min, attempt p95 %.1fms, overhead p95 %.2fms, cache saved %.1f%%, executor calls %d, failed %d, retries %d, lease expirations %d\n",
			s.Name, rs.WallSeconds, rs.JobsPerMinute, rs.AttemptP95MS, rs.OverheadP95MS, rs.CacheSaved*100, rs.ExecutorCalls, rs.Failed, rs.Retries, rs.LeaseExpirations)
	}

	if err := os.MkdirAll(*out, 0o755); err != nil {
		fail(err)
	}
	path := filepath.Join(*out, fmt.Sprintf("%s_%s.json", s.StartedAt.Format("20060102T150405Z"), s.Name))
	b, _ := json.MarshalIndent(rec, "", "  ")
	if err := os.WriteFile(path, append(b, '\n'), 0o644); err != nil {
		fail(err)
	}
	fmt.Fprintln(os.Stderr, "wrote", path)
}

func buildCases(s Setup) []*pb.Case {
	if s.CasesFile != "" {
		b, err := os.ReadFile(s.CasesFile)
		if err != nil {
			fail(err)
		}
		var out []*pb.Case
		for i, line := range strings.Split(string(b), "\n") {
			if strings.TrimSpace(line) == "" {
				continue
			}
			var c struct {
				CaseID  string          `json:"case_id"`
				Payload json.RawMessage `json:"payload"`
			}
			if err := json.Unmarshal([]byte(line), &c); err != nil {
				fail(fmt.Errorf("%s line %d: %w", s.CasesFile, i+1, err))
			}
			out = append(out, &pb.Case{CaseId: c.CaseID, PayloadJson: c.Payload})
		}
		return out
	}
	u := s.Unique
	if u <= 0 || u > s.Cases {
		u = s.Cases
	}
	// A per-scenario salt keeps runs from sharing cache entries by accident.
	salt := fmt.Sprintf("%s-%d", s.Name, s.StartedAt.UnixNano())
	out := make([]*pb.Case, s.Cases)
	for i := range out {
		p, _ := json.Marshal(map[string]any{"task": i % u, "salt": salt})
		out[i] = &pb.Case{CaseId: fmt.Sprintf("case-%06d", i), PayloadJson: p}
	}
	return out
}

// mutate returns a copy of cases where a random fraction has a changed payload.
func mutate(cases []*pb.Case, frac float64) []*pb.Case {
	out := make([]*pb.Case, len(cases))
	r := rand.New(rand.NewPCG(1, 2))
	for i, c := range cases {
		out[i] = c
		if r.Float64() < frac {
			var p map[string]any
			json.Unmarshal(c.PayloadJson, &p)
			p["revision"] = 2
			b, _ := json.Marshal(p)
			out[i] = &pb.Case{CaseId: c.CaseId, PayloadJson: b}
		}
	}
	return out
}

// started tracks child processes so fail() can clean them up.
var started []*exec.Cmd

func start(path string, args ...string) *exec.Cmd {
	cmd := exec.Command(path, args...)
	started = append(started, cmd)
	logf, err := os.Create(filepath.Join(os.TempDir(), fmt.Sprintf("evalbench-%s-%d.log", filepath.Base(path), time.Now().UnixNano())))
	if err == nil {
		cmd.Stdout, cmd.Stderr = logf, logf
	}
	if err := cmd.Start(); err != nil {
		fail(fmt.Errorf("start %s: %w", path, err))
	}
	return cmd
}

func stop(cmd *exec.Cmd) {
	if cmd.Process == nil {
		return
	}
	cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan struct{})
	go func() { cmd.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		cmd.Process.Kill()
	}
}

func waitReady(c pb.CoordinatorClient) {
	for i := 0; i < 100; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := c.GetRun(ctx, &pb.GetRunRequest{RunId: "00000000-0000-0000-0000-000000000000"})
		cancel()
		if err != nil && strings.Contains(err.Error(), "NotFound") || err == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	fail(fmt.Errorf("coordinator did not become ready"))
}

func hostInfo(dsn string) Host {
	h := Host{CPUs: runtime.NumCPU(), OS: runtime.GOOS + "/" + runtime.GOARCH, GoVersion: runtime.Version(),
		Topology: "single host: 1 coordinator process + N worker processes over loopback gRPC; Postgres 16 in a Docker container on the same host"}
	if b, err := os.ReadFile("/proc/cpuinfo"); err == nil {
		for _, l := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(l, "model name") {
				h.CPUModel = strings.TrimSpace(strings.SplitN(l, ":", 2)[1])
				break
			}
		}
	}
	if b, err := os.ReadFile("/proc/meminfo"); err == nil {
		var kb int
		fmt.Sscanf(strings.SplitN(string(b), "\n", 2)[0], "MemTotal: %d kB", &kb)
		h.MemGB = kb / 1024 / 1024
	}
	if out, err := exec.Command("psql", dsn, "-Atc", "show server_version").Output(); err == nil {
		h.Postgres = strings.TrimSpace(string(out))
	}
	return h
}

func gitInfo() (string, bool) {
	out, err := exec.Command("git", "rev-parse", "--short", "HEAD").Output()
	if err != nil {
		return "", false
	}
	st, _ := exec.Command("git", "status", "--porcelain", "--untracked-files=no").Output()
	return strings.TrimSpace(string(out)), len(strings.TrimSpace(string(st))) > 0
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "evalbench:", err)
	for _, c := range started {
		if c.Process != nil {
			c.Process.Kill()
		}
	}
	os.Exit(1)
}
