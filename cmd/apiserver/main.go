// Command apiserver serves the REST API and WebSocket run streams. It is
// stateless, so several replicas can run behind a load balancer; with -redis
// set they share one stats cache.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/armaan-k019/go-eval-runner/gen/evalrunnerv1"
	"github.com/armaan-k019/go-eval-runner/internal/api"
	"github.com/armaan-k019/go-eval-runner/internal/metrics"
	"github.com/armaan-k019/go-eval-runner/internal/store"
)

func main() {
	addr := flag.String("addr", envOr("EVALRUNNER_HTTP_ADDR", ":8080"), "HTTP listen address")
	metricsAddr := flag.String("metrics-addr", envOr("EVALRUNNER_METRICS_ADDR", ":9092"), "Prometheus /metrics address")
	dsn := flag.String("dsn", envOr("EVALRUNNER_DSN", "postgres://eval:eval@localhost:5432/eval?sslmode=disable"), "Postgres DSN")
	coordAddr := flag.String("coordinator", envOr("EVALRUNNER_COORDINATOR", "localhost:7070"), "coordinator gRPC address")
	redisAddr := flag.String("redis", envOr("EVALRUNNER_REDIS", ""), "Redis address for the shared stats cache (empty disables it)")
	ttl := flag.Duration("stats-ttl", 250*time.Millisecond, "cache lifetime of a live run's stats")
	push := flag.Duration("push", 500*time.Millisecond, "WebSocket push interval")
	origins := flag.String("origins", envOr("EVALRUNNER_ORIGINS", "localhost:3000"), "comma-separated browser origins allowed for CORS and WebSockets")
	dbConns := flag.Int("db-conns", 16, "Postgres pool size")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var st *store.Store
	var err error
	for i := 0; i < 30; i++ {
		if st, err = store.Open(ctx, *dsn, int32(*dbConns)); err == nil {
			break
		}
		log.Warn("postgres not ready", "err", err)
		time.Sleep(time.Second)
	}
	if err != nil {
		log.Error("open store", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	conn, err := grpc.NewClient(*coordAddr, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(256<<20)))
	if err != nil {
		log.Error("dial coordinator", "err", err)
		os.Exit(1)
	}
	defer conn.Close()

	var cache *api.RedisStats
	if *redisAddr != "" {
		rdb := redis.NewClient(&redis.Options{Addr: *redisAddr})
		defer rdb.Close()
		cache = &api.RedisStats{RDB: rdb, TTL: *ttl, DoneTTL: time.Hour}
	}

	reg := metrics.NewRegistry()
	m := api.NewMetrics(reg)
	msrv := metrics.Serve(*metricsAddr, reg)
	defer msrv.Close()

	s := api.New(st, cache, pb.NewCoordinatorClient(conn), api.Config{
		PushInterval: *push,
		AllowOrigins: strings.Split(*origins, ","),
	}, m, log)
	srv := &http.Server{Addr: *addr, Handler: s.Handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(sctx)
	}()
	log.Info("apiserver listening", "addr", *addr, "redis", *redisAddr != "", "coordinator", *coordAddr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("serve", "err", err)
		os.Exit(1)
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
