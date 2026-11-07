// Package worker pulls jobs from the coordinator and runs them on a bounded
// pool of goroutines, renewing leases while they execute and reporting
// results with retry.
package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	pb "github.com/armaan-k019/go-eval-runner/gen/evalrunnerv1"
	"github.com/armaan-k019/go-eval-runner/internal/executor"
	"github.com/armaan-k019/go-eval-runner/internal/metrics"
)

type Config struct {
	ID          string
	Concurrency int
	LeaseWait   time.Duration // long-poll duration per LeaseJobs call
	JobTypes    []string
	// BatchReports sends results through one reporter goroutine that packs
	// whatever has accumulated into a single ReportResults RPC.
	BatchReports bool
	MaxBatch     int
}

type Worker struct {
	cfg    Config
	client pb.CoordinatorClient
	reg    executor.Registry
	m      *metrics.Worker
	log    *slog.Logger

	mu       sync.Mutex
	inflight map[int64]context.CancelFunc
	leaseMS  int32
	reports  chan *pb.ReportResultRequest
}

func New(cfg Config, client pb.CoordinatorClient, reg executor.Registry, m *metrics.Worker, log *slog.Logger) *Worker {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if cfg.LeaseWait <= 0 {
		cfg.LeaseWait = 2 * time.Second
	}
	if len(cfg.JobTypes) == 0 {
		cfg.JobTypes = reg.Names()
	}
	if cfg.MaxBatch <= 0 {
		cfg.MaxBatch = 256
	}
	return &Worker{cfg: cfg, client: client, reg: reg, m: m, log: log, inflight: map[int64]context.CancelFunc{}, leaseMS: 15000,
		reports: make(chan *pb.ReportResultRequest, cfg.Concurrency)}
}

// Run leases and executes jobs until ctx is cancelled, then waits for
// in-flight jobs to finish and report (graceful drain).
func (w *Worker) Run(ctx context.Context) error {
	slots := make(chan struct{}, w.cfg.Concurrency)
	for i := 0; i < w.cfg.Concurrency; i++ {
		slots <- struct{}{}
	}
	var wg sync.WaitGroup
	renewCtx, stopRenew := context.WithCancel(context.Background())
	defer stopRenew()
	go w.renewLoop(renewCtx)
	reporterDone := make(chan struct{})
	if w.cfg.BatchReports {
		go func() { defer close(reporterDone); w.reportLoop() }()
	} else {
		close(reporterDone)
	}

	backoff := 100 * time.Millisecond
	for ctx.Err() == nil {
		// Block for at least one free slot, then grab any others that are free.
		select {
		case <-ctx.Done():
			continue
		case <-slots:
		}
		free := 1
	grab:
		for free < w.cfg.Concurrency {
			select {
			case <-slots:
				free++
			default:
				break grab
			}
		}
		resp, err := w.client.LeaseJobs(ctx, &pb.LeaseJobsRequest{
			WorkerId: w.cfg.ID, MaxJobs: int32(free), WaitMs: int32(w.cfg.LeaseWait.Milliseconds()), JobTypes: w.cfg.JobTypes,
		})
		if err != nil {
			for i := 0; i < free; i++ {
				slots <- struct{}{}
			}
			if ctx.Err() != nil {
				break
			}
			w.log.Warn("lease failed, backing off", "err", err, "backoff", backoff)
			sleepCtx(ctx, backoff)
			backoff = min(backoff*2, 5*time.Second)
			continue
		}
		backoff = 100 * time.Millisecond
		if resp.LeaseMs > 0 {
			w.mu.Lock()
			w.leaseMS = resp.LeaseMs
			w.mu.Unlock()
		}
		for i := len(resp.Jobs); i < free; i++ {
			slots <- struct{}{}
		}
		for _, job := range resp.Jobs {
			jctx, cancel := context.WithCancel(context.Background())
			w.mu.Lock()
			w.inflight[job.JobId] = cancel
			w.mu.Unlock()
			w.m.InFlight.Inc()
			wg.Add(1)
			go func(job *pb.Job) {
				defer wg.Done()
				defer func() { slots <- struct{}{} }()
				defer w.m.InFlight.Dec()
				defer func() {
					w.mu.Lock()
					delete(w.inflight, job.JobId)
					w.mu.Unlock()
					cancel()
				}()
				w.execute(jctx, job)
			}(job)
		}
	}
	w.log.Info("draining in-flight jobs")
	wg.Wait()
	close(w.reports)
	<-reporterDone
	return nil
}

func (w *Worker) execute(ctx context.Context, job *pb.Job) {
	req := &pb.ReportResultRequest{WorkerId: w.cfg.ID, JobId: job.JobId, Attempt: job.Attempt}
	ex, err := w.reg.Get(job.JobType)
	start := time.Now()
	if err != nil {
		req.Outcome, req.Error, req.Retryable = pb.Outcome_OUTCOME_ERROR, err.Error(), false
	} else {
		tctx := ctx
		if job.TimeoutMs > 0 {
			var cancel context.CancelFunc
			tctx, cancel = context.WithTimeout(ctx, time.Duration(job.TimeoutMs)*time.Millisecond)
			defer cancel()
		}
		res, runErr := ex.Run(tctx, executor.Job{
			JobID: job.JobId, CaseID: job.CaseId, Attempt: int(job.Attempt),
			Config: job.ConfigJson, Payload: job.PayloadJson,
		})
		if ctx.Err() != nil {
			// Lease was lost and the job cancelled; someone else owns it now.
			w.m.LostLeases.Inc()
			return
		}
		if runErr != nil {
			req.Outcome, req.Error, req.Retryable = pb.Outcome_OUTCOME_ERROR, runErr.Error(), executor.Retryable(runErr)
		} else {
			req.Outcome, req.ResultJson = pb.Outcome_OUTCOME_SUCCESS, res.Raw
		}
	}
	elapsed := time.Since(start)
	req.ExecUs = elapsed.Microseconds()
	outcome := "success"
	if req.Outcome != pb.Outcome_OUTCOME_SUCCESS {
		outcome = "error"
	}
	w.m.ExecSeconds.WithLabelValues(job.JobType, outcome).Observe(elapsed.Seconds())
	if w.cfg.BatchReports {
		w.reports <- req
		return
	}
	w.report(req)
}

// reportLoop drains the report channel. While one ReportResults RPC is in
// flight, finished jobs queue up and go out together in the next one, so
// batch size adapts to load with no added latency when idle.
func (w *Worker) reportLoop() {
	for first := range w.reports {
		batch := []*pb.ReportResultRequest{first}
	drain:
		for len(batch) < w.cfg.MaxBatch {
			select {
			case r, ok := <-w.reports:
				if !ok {
					break drain
				}
				batch = append(batch, r)
			default:
				break drain
			}
		}
		w.sendBatch(batch)
	}
}

func (w *Worker) sendBatch(batch []*pb.ReportResultRequest) {
	delay := 50 * time.Millisecond
	for i := 0; i < 8; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, err := w.client.ReportResults(ctx, &pb.ReportResultsRequest{Results: batch})
		cancel()
		if err == nil {
			return
		}
		w.log.Warn("batch report failed, retrying", "size", len(batch), "err", err)
		time.Sleep(delay)
		delay = min(delay*2, 3*time.Second)
	}
	w.m.ReportErrs.Add(float64(len(batch)))
	w.log.Error("giving up on batch report; leases will expire and the jobs will be retried", "size", len(batch))
}

// report retries transient RPC failures. Reports are idempotent on the
// coordinator side: a duplicate is rejected by the (worker, attempt) fence.
func (w *Worker) report(req *pb.ReportResultRequest) {
	delay := 50 * time.Millisecond
	for i := 0; i < 8; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		resp, err := w.client.ReportResult(ctx, req)
		cancel()
		if err == nil {
			if !resp.Accepted {
				w.log.Debug("report not accepted (stale lease)", "job", req.JobId, "attempt", req.Attempt)
			}
			return
		}
		w.log.Warn("report failed, retrying", "job", req.JobId, "err", err)
		time.Sleep(delay)
		delay = min(delay*2, 3*time.Second)
	}
	w.m.ReportErrs.Inc()
	w.log.Error("giving up on report; lease will expire and the job will be retried", "job", req.JobId)
}

func (w *Worker) renewLoop(ctx context.Context) {
	for {
		w.mu.Lock()
		every := time.Duration(w.leaseMS) * time.Millisecond / 3
		ids := make([]int64, 0, len(w.inflight))
		for id := range w.inflight {
			ids = append(ids, id)
		}
		w.mu.Unlock()
		if !sleepCtx(ctx, every) {
			return
		}
		if len(ids) == 0 {
			continue
		}
		rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		resp, err := w.client.RenewLeases(rctx, &pb.RenewLeasesRequest{WorkerId: w.cfg.ID, JobIds: ids})
		cancel()
		if err != nil {
			if !errors.Is(ctx.Err(), context.Canceled) {
				w.log.Warn("renew failed", "err", err)
			}
			continue
		}
		if len(resp.LostJobIds) > 0 {
			w.mu.Lock()
			for _, id := range resp.LostJobIds {
				if c, ok := w.inflight[id]; ok {
					c()
				}
			}
			w.mu.Unlock()
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
