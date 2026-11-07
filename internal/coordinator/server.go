// Package coordinator implements the gRPC Coordinator service on top of the
// Postgres store. It holds no scheduling state of its own: restart it at any
// time and workers carry on once it is back.
package coordinator

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/armaan-k019/go-eval-runner/gen/evalrunnerv1"
	"github.com/armaan-k019/go-eval-runner/internal/metrics"
	"github.com/armaan-k019/go-eval-runner/internal/store"
)

type Config struct {
	LeaseDuration  time.Duration
	BackoffBase    time.Duration
	BackoffMax     time.Duration
	ReapInterval   time.Duration
	PollInterval   time.Duration // long-poll recheck when not woken
	MaxLeaseWait   time.Duration
	DefaultTimeout time.Duration
	DefaultTries   int
}

func DefaultConfig() Config {
	return Config{
		LeaseDuration:  15 * time.Second,
		BackoffBase:    200 * time.Millisecond,
		BackoffMax:     10 * time.Second,
		ReapInterval:   time.Second,
		PollInterval:   50 * time.Millisecond,
		MaxLeaseWait:   5 * time.Second,
		DefaultTimeout: 5 * time.Minute,
		DefaultTries:   3,
	}
}

type Server struct {
	pb.UnimplementedCoordinatorServer
	st  *store.Store
	cfg Config
	m   *metrics.Coordinator
	log *slog.Logger

	mu   sync.Mutex
	wake chan struct{} // closed and replaced whenever new work may be runnable
}

func New(st *store.Store, cfg Config, m *metrics.Coordinator, log *slog.Logger) *Server {
	return &Server{st: st, cfg: cfg, m: m, log: log, wake: make(chan struct{})}
}

func (s *Server) notify() {
	s.mu.Lock()
	close(s.wake)
	s.wake = make(chan struct{})
	s.mu.Unlock()
}

func (s *Server) waitCh() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.wake
}

func (s *Server) SubmitRun(ctx context.Context, req *pb.SubmitRunRequest) (*pb.SubmitRunResponse, error) {
	if req.JobType == "" {
		return nil, status.Error(codes.InvalidArgument, "job_type is required")
	}
	if len(req.Cases) == 0 {
		return nil, status.Error(codes.InvalidArgument, "no cases")
	}
	spec := store.RunSpec{
		Suite:       req.Suite,
		JobType:     req.JobType,
		Config:      req.ConfigJson,
		MaxAttempts: int(req.MaxAttempts),
		TimeoutMS:   int(req.TimeoutMs),
		NoCache:     req.NoCache,
	}
	if spec.MaxAttempts <= 0 {
		spec.MaxAttempts = s.cfg.DefaultTries
	}
	if spec.TimeoutMS <= 0 {
		spec.TimeoutMS = int(s.cfg.DefaultTimeout.Milliseconds())
	}
	cases := make([]store.CaseIn, len(req.Cases))
	for i, c := range req.Cases {
		cases[i] = store.CaseIn{CaseID: c.CaseId, Payload: c.PayloadJson}
	}
	res, err := s.st.CreateRun(ctx, spec, cases)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create run: %v", err)
	}
	s.m.Submitted.Add(float64(res.Jobs))
	s.m.CacheHits.WithLabelValues("submit").Add(float64(res.CacheHits))
	s.m.Deduped.Add(float64(res.Deduped))
	s.log.Info("run submitted", "run", res.RunID, "suite", req.Suite, "jobs", res.Jobs, "cache_hits", res.CacheHits, "deduped", res.Deduped)
	s.notify()
	return &pb.SubmitRunResponse{RunId: res.RunID, Jobs: int32(res.Jobs), CacheHits: int32(res.CacheHits), Deduped: int32(res.Deduped)}, nil
}

func (s *Server) GetRun(ctx context.Context, req *pb.GetRunRequest) (*pb.RunStatus, error) {
	st, err := s.st.RunStats(ctx, req.RunId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "run %s: %v", req.RunId, err)
	}
	return ToProto(st), nil
}

func ToProto(st store.RunStats) *pb.RunStatus {
	return &pb.RunStatus{
		RunId: st.RunID, Suite: st.Suite, Done: st.Done(), Total: st.Total,
		Queued: st.Queued, Leased: st.Leased, Waiting: st.Waiting,
		Succeeded: st.Succeeded, Cached: st.Cached, Failed: st.Failed,
		Retries: st.Retries, LeaseExpirations: st.LeaseExpirations,
		WallSeconds: st.WallSeconds, JobsPerMinute: st.JobsPerMinute,
		AttemptP50Ms: st.AttemptP50MS, AttemptP95Ms: st.AttemptP95MS, AttemptP99Ms: st.AttemptP99MS,
		ExecP50Ms: st.ExecP50MS, ExecP95Ms: st.ExecP95MS,
		OverheadP50Ms: st.OverheadP50MS, OverheadP95Ms: st.OverheadP95MS,
		MeanScore: st.MeanScore, PassRate: st.PassRate,
		CacheSavedFraction: st.CacheSaved, ExecutorCalls: st.ExecutorCalls,
	}
}

func (s *Server) LeaseJobs(ctx context.Context, req *pb.LeaseJobsRequest) (*pb.LeaseJobsResponse, error) {
	start := time.Now()
	defer func() { s.m.LeaseRPCLat.Observe(time.Since(start).Seconds()) }()
	if req.WorkerId == "" || req.MaxJobs <= 0 {
		return nil, status.Error(codes.InvalidArgument, "worker_id and max_jobs > 0 required")
	}
	wait := time.Duration(req.WaitMs) * time.Millisecond
	if wait > s.cfg.MaxLeaseWait {
		wait = s.cfg.MaxLeaseWait
	}
	deadline := time.Now().Add(wait)
	for {
		woke := s.waitCh() // grab before querying so a notify in between is not lost
		jobs, err := s.st.Lease(ctx, req.WorkerId, int(req.MaxJobs), req.JobTypes, s.cfg.LeaseDuration)
		if err != nil {
			if errors.Is(ctx.Err(), context.Canceled) {
				return nil, status.FromContextError(ctx.Err()).Err()
			}
			return nil, status.Errorf(codes.Internal, "lease: %v", err)
		}
		if len(jobs) > 0 {
			s.m.LeaseGranted.Add(float64(len(jobs)))
			out := &pb.LeaseJobsResponse{LeaseMs: int32(s.cfg.LeaseDuration.Milliseconds())}
			for _, j := range jobs {
				out.Jobs = append(out.Jobs, &pb.Job{
					JobId: j.JobID, RunId: j.RunID, CaseId: j.CaseID, JobType: j.JobType,
					ConfigJson: j.Config, PayloadJson: j.Payload, Attempt: int32(j.Attempt), TimeoutMs: int32(j.TimeoutMS),
				})
			}
			return out, nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return &pb.LeaseJobsResponse{LeaseMs: int32(s.cfg.LeaseDuration.Milliseconds())}, nil
		}
		t := time.NewTimer(min(remaining, s.cfg.PollInterval))
		select {
		case <-ctx.Done():
			t.Stop()
			return nil, status.FromContextError(ctx.Err()).Err()
		case <-woke:
			t.Stop()
		case <-t.C:
		}
	}
}

func (s *Server) RenewLeases(ctx context.Context, req *pb.RenewLeasesRequest) (*pb.RenewLeasesResponse, error) {
	lost, err := s.st.Renew(ctx, req.WorkerId, req.JobIds, s.cfg.LeaseDuration)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "renew: %v", err)
	}
	return &pb.RenewLeasesResponse{LostJobIds: lost}, nil
}

func toStoreReport(req *pb.ReportResultRequest) store.Report {
	return store.Report{
		WorkerID: req.WorkerId, JobID: req.JobId, Attempt: int(req.Attempt),
		Success: req.Outcome == pb.Outcome_OUTCOME_SUCCESS, Result: req.ResultJson,
		Error: req.Error, Retryable: req.Retryable, ExecUS: req.ExecUs,
	}
}

func (s *Server) ReportResult(ctx context.Context, req *pb.ReportResultRequest) (*pb.ReportResultResponse, error) {
	out, err := s.st.Report(ctx, toStoreReport(req), s.cfg.BackoffBase, s.cfg.BackoffMax)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "report: %v", err)
	}
	s.afterReport(req, out)
	return &pb.ReportResultResponse{Accepted: out.Accepted}, nil
}

func (s *Server) ReportResults(ctx context.Context, req *pb.ReportResultsRequest) (*pb.ReportResultsResponse, error) {
	rs := make([]store.Report, len(req.Results))
	for i, r := range req.Results {
		rs[i] = toStoreReport(r)
	}
	outs, err := s.st.ReportBatch(ctx, rs, s.cfg.BackoffBase, s.cfg.BackoffMax)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "report batch: %v", err)
	}
	resp := &pb.ReportResultsResponse{Accepted: make([]bool, len(outs))}
	for i, out := range outs {
		s.afterReport(req.Results[i], out)
		resp.Accepted[i] = out.Accepted
	}
	return resp, nil
}

// afterReport updates metrics and wakes long-polling lessees when a report
// made new work runnable.
func (s *Server) afterReport(req *pb.ReportResultRequest, out store.ReportOutcome) {
	if !out.Accepted {
		s.m.StaleReports.Inc()
		return
	}
	s.m.AttemptLat.Observe(float64(req.ExecUs) / 1e6)
	switch out.Status {
	case "queued":
		s.m.Retries.Inc()
		s.notify()
	default:
		s.m.Finished.WithLabelValues(out.Status).Inc()
	}
	if out.Resolved > 0 {
		s.m.Finished.WithLabelValues("cached").Add(float64(out.Resolved))
		s.m.CacheHits.WithLabelValues("inflight_dedup").Add(float64(out.Resolved))
	}
	if out.Status == "failed" {
		s.notify() // a waiter may have been promoted
	}
}

// RunReaper reclaims expired leases and refreshes the queue-depth gauge until
// ctx is cancelled.
func (s *Server) RunReaper(ctx context.Context) {
	t := time.NewTicker(s.cfg.ReapInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		res, err := s.st.Reap(ctx)
		if err != nil {
			if ctx.Err() == nil {
				s.log.Error("reap failed", "err", err)
			}
			continue
		}
		if res.Expired > 0 || res.Promoted > 0 || res.Resolved > 0 {
			s.log.Warn("reaper", "expired", res.Expired, "failed", res.Failed, "promoted", res.Promoted, "resolved", res.Resolved)
			s.m.LeaseExpired.Add(float64(res.Expired))
			s.m.CacheHits.WithLabelValues("inflight_dedup").Add(float64(res.Resolved))
			s.notify()
		}
		if d, err := s.st.Depth(ctx); err == nil {
			for k, v := range d {
				s.m.QueueDepth.WithLabelValues(k).Set(float64(v))
			}
		}
	}
}
