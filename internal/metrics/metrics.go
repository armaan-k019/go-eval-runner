// Package metrics holds the Prometheus collectors for the coordinator and
// workers. Each process registers only the set it uses.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var latencyBuckets = prometheus.ExponentialBuckets(0.001, 2, 18) // 1ms .. ~131s

type Coordinator struct {
	Submitted    prometheus.Counter
	CacheHits    *prometheus.CounterVec // source: submit | inflight_dedup
	Deduped      prometheus.Counter
	Finished     *prometheus.CounterVec // status: succeeded | failed | cached
	Retries      prometheus.Counter
	LeaseExpired prometheus.Counter
	StaleReports prometheus.Counter
	LeaseGranted prometheus.Counter
	AttemptLat   prometheus.Histogram
	QueueDepth   *prometheus.GaugeVec
	LeaseRPCLat  prometheus.Histogram
}

func NewCoordinator(reg prometheus.Registerer) *Coordinator {
	m := &Coordinator{
		Submitted:    prometheus.NewCounter(prometheus.CounterOpts{Name: "evalrunner_jobs_submitted_total", Help: "Jobs submitted."}),
		CacheHits:    prometheus.NewCounterVec(prometheus.CounterOpts{Name: "evalrunner_cache_hits_total", Help: "Jobs answered without calling an executor."}, []string{"source"}),
		Deduped:      prometheus.NewCounter(prometheus.CounterOpts{Name: "evalrunner_jobs_deduped_total", Help: "Jobs parked behind an identical in-flight job at submit."}),
		Finished:     prometheus.NewCounterVec(prometheus.CounterOpts{Name: "evalrunner_jobs_finished_total", Help: "Jobs reaching a terminal state via a worker report."}, []string{"status"}),
		Retries:      prometheus.NewCounter(prometheus.CounterOpts{Name: "evalrunner_retries_scheduled_total", Help: "Failed attempts requeued with backoff."}),
		LeaseExpired: prometheus.NewCounter(prometheus.CounterOpts{Name: "evalrunner_lease_expired_total", Help: "Leases that timed out and were reclaimed."}),
		StaleReports: prometheus.NewCounter(prometheus.CounterOpts{Name: "evalrunner_stale_reports_total", Help: "Reports dropped because the lease had moved on."}),
		LeaseGranted: prometheus.NewCounter(prometheus.CounterOpts{Name: "evalrunner_leases_granted_total", Help: "Jobs handed to workers."}),
		AttemptLat:   prometheus.NewHistogram(prometheus.HistogramOpts{Name: "evalrunner_attempt_reported_exec_seconds", Help: "Executor time reported by workers.", Buckets: latencyBuckets}),
		QueueDepth:   prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "evalrunner_queue_depth", Help: "Non-terminal jobs by status."}, []string{"status"}),
		LeaseRPCLat:  prometheus.NewHistogram(prometheus.HistogramOpts{Name: "evalrunner_lease_rpc_seconds", Help: "LeaseJobs handler time, including long-poll wait.", Buckets: latencyBuckets}),
	}
	reg.MustRegister(m.Submitted, m.CacheHits, m.Deduped, m.Finished, m.Retries, m.LeaseExpired,
		m.StaleReports, m.LeaseGranted, m.AttemptLat, m.QueueDepth, m.LeaseRPCLat)
	return m
}

type Worker struct {
	ExecSeconds *prometheus.HistogramVec // job_type, outcome
	InFlight    prometheus.Gauge
	LostLeases  prometheus.Counter
	ReportErrs  prometheus.Counter
}

func NewWorker(reg prometheus.Registerer) *Worker {
	m := &Worker{
		ExecSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "evalrunner_worker_exec_seconds", Help: "Executor wall time per attempt.", Buckets: latencyBuckets}, []string{"job_type", "outcome"}),
		InFlight:    prometheus.NewGauge(prometheus.GaugeOpts{Name: "evalrunner_worker_inflight", Help: "Jobs currently executing."}),
		LostLeases:  prometheus.NewCounter(prometheus.CounterOpts{Name: "evalrunner_worker_lost_leases_total", Help: "Jobs whose lease was reclaimed mid-execution."}),
		ReportErrs:  prometheus.NewCounter(prometheus.CounterOpts{Name: "evalrunner_worker_report_errors_total", Help: "ReportResult RPCs that failed after retries."}),
	}
	reg.MustRegister(m.ExecSeconds, m.InFlight, m.LostLeases, m.ReportErrs)
	return m
}

// NewRegistry returns a registry with Go runtime and process collectors.
func NewRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return reg
}

// Serve exposes /metrics on addr. Returns the server so callers can shut it down.
func Serve(addr string, reg *prometheus.Registry) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	srv := &http.Server{Addr: addr, Handler: mux}
	go srv.ListenAndServe()
	return srv
}
