// Package api is the HTTP side of the runner: a REST API over runs and a
// WebSocket stream of live run stats, served by the stateless apiserver.
// Submissions are forwarded to the coordinator over gRPC; reads go to
// Postgres, optionally through a shared Redis cache.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"

	pb "github.com/armaan-k019/go-eval-runner/gen/evalrunnerv1"
	"github.com/armaan-k019/go-eval-runner/internal/store"
)

// Lister lists recent runs.
type Lister interface {
	ListRuns(ctx context.Context, limit int) ([]store.RunSummary, error)
}

type Config struct {
	PushInterval time.Duration // WebSocket push period while a run is live
	AllowOrigins []string      // CORS and WebSocket origin patterns, e.g. "localhost:3000"
}

type Server struct {
	stats StatsSource
	runs  Lister
	coord pb.CoordinatorClient
	cfg   Config
	log   *slog.Logger
	m     *Metrics
	mux   *http.ServeMux
}

type Metrics struct {
	Requests  *prometheus.CounterVec // route, code
	Latency   *prometheus.HistogramVec
	DBStats   prometheus.Counter
	WSClients prometheus.Gauge
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Requests:  prometheus.NewCounterVec(prometheus.CounterOpts{Name: "evalrunner_api_requests_total", Help: "HTTP requests."}, []string{"route", "code"}),
		Latency:   prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "evalrunner_api_request_seconds", Help: "HTTP handler time.", Buckets: prometheus.ExponentialBuckets(0.0005, 2, 16)}, []string{"route"}),
		DBStats:   prometheus.NewCounter(prometheus.CounterOpts{Name: "evalrunner_api_stats_db_queries_total", Help: "Run stats computed in Postgres (cache misses)."}),
		WSClients: prometheus.NewGauge(prometheus.GaugeOpts{Name: "evalrunner_api_ws_clients", Help: "Open WebSocket streams."}),
	}
	reg.MustRegister(m.Requests, m.Latency, m.DBStats, m.WSClients)
	return m
}

// Store is the read side the API needs; *store.Store implements it.
type Store interface {
	StatsSource
	Lister
}

// New builds the server. If cache is nil, every stats read hits Postgres.
func New(st Store, cache *RedisStats, coord pb.CoordinatorClient, cfg Config, m *Metrics, log *slog.Logger) *Server {
	var src StatsSource = countingSource{src: st, onCall: m.DBStats.Inc}
	if cache != nil {
		cache.Src = st
		cache.OnMiss = m.DBStats.Inc
		src = cache
	}
	if cfg.PushInterval <= 0 {
		cfg.PushInterval = 500 * time.Millisecond
	}
	s := &Server{stats: src, runs: st, coord: coord, cfg: cfg, log: log, m: m, mux: http.NewServeMux()}
	s.route("GET /api/runs", "list", s.listRuns)
	s.route("POST /api/runs", "submit", s.submitRun)
	s.route("GET /api/runs/{id}", "get", s.getRun)
	s.mux.HandleFunc("GET /api/runs/{id}/ws", s.streamRun)
	s.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	return s
}

func (s *Server) Handler() http.Handler { return s.cors(s.mux) }

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (w *statusWriter) WriteHeader(c int) { w.code = c; w.ResponseWriter.WriteHeader(c) }

func (s *Server) route(pattern, name string, h http.HandlerFunc) {
	s.mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
		t := time.Now()
		sw := &statusWriter{ResponseWriter: w, code: 200}
		h(sw, r)
		s.m.Latency.WithLabelValues(name).Observe(time.Since(t).Seconds())
		s.m.Requests.WithLabelValues(name, strconv.Itoa(sw.code)).Inc()
	})
}

func (s *Server) cors(next http.Handler) http.Handler {
	allowed := map[string]bool{}
	for _, o := range s.cfg.AllowOrigins {
		allowed["http://"+o] = true
		allowed["https://"+o] = true
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if o := r.Header.Get("Origin"); allowed[o] {
			w.Header().Set("Access-Control-Allow-Origin", o)
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
			w.Header().Set("Vary", "Origin")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

// RunView is the JSON shape of a run's stats.
type RunView struct {
	store.RunStats
	Done bool `json:"done"`
}

func view(st store.RunStats) RunView { return RunView{RunStats: st, Done: st.Done()} }

func (s *Server) listRuns(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	runs, err := s.runs.ListRuns(r.Context(), limit)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": runs})
}

func (s *Server) lookup(ctx context.Context, id string) (store.RunStats, int, error) {
	st, err := s.stats.RunStats(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return st, http.StatusNotFound, errors.New("run not found")
	}
	if err != nil {
		// Malformed UUIDs fail in Postgres; treat them as not found too.
		return st, http.StatusNotFound, err
	}
	return st, http.StatusOK, nil
}

func (s *Server) getRun(w http.ResponseWriter, r *http.Request) {
	st, code, err := s.lookup(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, code, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, view(st))
}

// SubmitRequest is the JSON body of POST /api/runs.
type SubmitRequest struct {
	Suite       string          `json:"suite"`
	JobType     string          `json:"job_type"`
	Config      json.RawMessage `json:"config"`
	Cases       []SubmitCase    `json:"cases"`
	MaxAttempts int32           `json:"max_attempts"`
	TimeoutMS   int32           `json:"timeout_ms"`
	NoCache     bool            `json:"no_cache"`
}

type SubmitCase struct {
	CaseID  string          `json:"case_id"`
	Payload json.RawMessage `json:"payload"`
}

const maxSubmitBytes = 64 << 20

func (s *Server) submitRun(w http.ResponseWriter, r *http.Request) {
	var req SubmitRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxSubmitBytes))
	if err := dec.Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if req.JobType == "" || len(req.Cases) == 0 {
		writeErr(w, http.StatusBadRequest, "job_type and at least one case are required")
		return
	}
	if len(req.Config) == 0 {
		req.Config = json.RawMessage("{}")
	}
	pr := &pb.SubmitRunRequest{
		Suite: req.Suite, JobType: req.JobType, ConfigJson: req.Config,
		MaxAttempts: req.MaxAttempts, TimeoutMs: req.TimeoutMS, NoCache: req.NoCache,
		Cases: make([]*pb.Case, len(req.Cases)),
	}
	for i, c := range req.Cases {
		if len(c.Payload) == 0 {
			c.Payload = json.RawMessage("{}")
		}
		pr.Cases[i] = &pb.Case{CaseId: c.CaseID, PayloadJson: c.Payload}
	}
	resp, err := s.coord.SubmitRun(r.Context(), pr)
	if err != nil {
		writeErr(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"run_id": resp.RunId, "jobs": resp.Jobs, "cache_hits": resp.CacheHits, "deduped": resp.Deduped,
	})
}

// streamRun pushes the run's stats over a WebSocket every PushInterval while
// the run is live, sends one final message when it finishes, then closes.
func (s *Server) streamRun(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, code, err := s.lookup(r.Context(), id); err != nil {
		writeErr(w, code, err.Error())
		return
	}
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: s.cfg.AllowOrigins})
	if err != nil {
		return
	}
	defer c.CloseNow()
	s.m.WSClients.Inc()
	defer s.m.WSClients.Dec()

	ctx := c.CloseRead(r.Context()) // client messages are ignored; detects disconnects
	t := time.NewTicker(s.cfg.PushInterval)
	defer t.Stop()
	for {
		st, err := s.stats.RunStats(ctx, id)
		if err != nil {
			c.Close(websocket.StatusInternalError, "stats unavailable")
			return
		}
		wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err = wsjson.Write(wctx, c, view(st))
		cancel()
		if err != nil {
			return
		}
		if st.Done() {
			c.Close(websocket.StatusNormalClosure, "run finished")
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
