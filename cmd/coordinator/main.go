// Command coordinator serves the gRPC job queue backed by Postgres.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	pb "github.com/armaan-k019/go-eval-runner/gen/evalrunnerv1"
	"github.com/armaan-k019/go-eval-runner/internal/coordinator"
	"github.com/armaan-k019/go-eval-runner/internal/metrics"
	"github.com/armaan-k019/go-eval-runner/internal/store"
)

func main() {
	cfg := coordinator.DefaultConfig()
	addr := flag.String("addr", envOr("EVALRUNNER_ADDR", ":7070"), "gRPC listen address")
	metricsAddr := flag.String("metrics-addr", envOr("EVALRUNNER_METRICS_ADDR", ":9090"), "Prometheus /metrics address")
	dsn := flag.String("dsn", envOr("EVALRUNNER_DSN", "postgres://eval:eval@localhost:5432/eval?sslmode=disable"), "Postgres DSN")
	maxConns := flag.Int("db-conns", 32, "Postgres pool size")
	flag.DurationVar(&cfg.LeaseDuration, "lease", cfg.LeaseDuration, "lease duration before an unrenewed job is reclaimed")
	flag.DurationVar(&cfg.BackoffBase, "backoff-base", cfg.BackoffBase, "retry backoff base")
	flag.DurationVar(&cfg.BackoffMax, "backoff-max", cfg.BackoffMax, "retry backoff cap")
	flag.DurationVar(&cfg.ReapInterval, "reap-interval", cfg.ReapInterval, "how often to reclaim expired leases")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var st *store.Store
	var err error
	for i := 0; i < 30; i++ { // wait for Postgres in docker compose
		if st, err = store.Open(ctx, *dsn, int32(*maxConns)); err == nil {
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
	if err := st.Migrate(ctx); err != nil {
		log.Error("migrate", "err", err)
		os.Exit(1)
	}

	reg := metrics.NewRegistry()
	m := metrics.NewCoordinator(reg)
	msrv := metrics.Serve(*metricsAddr, reg)
	defer msrv.Close()

	srv := coordinator.New(st, cfg, m, log)
	go srv.RunReaper(ctx)

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Error("listen", "err", err)
		os.Exit(1)
	}
	g := grpc.NewServer()
	pb.RegisterCoordinatorServer(g, srv)
	go func() {
		<-ctx.Done()
		g.GracefulStop()
	}()
	log.Info("coordinator listening", "addr", lis.Addr().String(), "metrics", *metricsAddr)
	if err := g.Serve(lis); err != nil {
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
