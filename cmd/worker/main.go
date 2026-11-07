// Command worker leases eval jobs from the coordinator and runs them.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/armaan-k019/go-eval-runner/gen/evalrunnerv1"
	"github.com/armaan-k019/go-eval-runner/internal/executor"
	"github.com/armaan-k019/go-eval-runner/internal/metrics"
	"github.com/armaan-k019/go-eval-runner/internal/worker"
)

func main() {
	host, _ := os.Hostname()
	coord := flag.String("coordinator", envOr("EVALRUNNER_COORDINATOR", "localhost:7070"), "coordinator gRPC address")
	id := flag.String("id", envOr("EVALRUNNER_WORKER_ID", fmt.Sprintf("%s-%d", host, os.Getpid())), "worker id")
	conc := flag.Int("concurrency", 8, "jobs executed in parallel")
	metricsAddr := flag.String("metrics-addr", envOr("EVALRUNNER_METRICS_ADDR", ":9091"), "Prometheus /metrics address (empty to disable)")
	types := flag.String("types", "", "comma-separated job types to accept (default: all registered)")
	allow := flag.String("exec-allow", os.Getenv("EVALRUNNER_EXEC_ALLOW"), "comma-separated command prefixes the exec executor may run (empty: any)")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})).With("worker", *id)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	conn, err := grpc.NewClient(*coord, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Error("dial", "err", err)
		os.Exit(1)
	}
	defer conn.Close()

	reg := executor.Registry{}
	reg.Register(executor.Mock{})
	reg.Register(executor.Exec{AllowedPrefixes: splitNonEmpty(*allow)})

	promReg := metrics.NewRegistry()
	m := metrics.NewWorker(promReg)
	if *metricsAddr != "" {
		srv := metrics.Serve(*metricsAddr, promReg)
		defer srv.Close()
	}

	w := worker.New(worker.Config{ID: *id, Concurrency: *conc, LeaseWait: 2 * time.Second, JobTypes: splitNonEmpty(*types)},
		pb.NewCoordinatorClient(conn), reg, m, log)
	log.Info("worker started", "coordinator", *coord, "concurrency", *conc)
	if err := w.Run(ctx); err != nil {
		log.Error("worker", "err", err)
		os.Exit(1)
	}
	log.Info("worker stopped")
}

func splitNonEmpty(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
