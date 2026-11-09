// Command apiload measures the apiserver: HTTP GET load on one run's stats,
// or many WebSocket viewers of a live run. Requests are spread round-robin
// over every -api URL (one per replica), and the Postgres stats queries each
// replica made are read from its /metrics before and after.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

func main() {
	apis := flag.String("api", "http://localhost:8080", "comma-separated apiserver base URLs")
	metricsURLs := flag.String("metrics", "http://localhost:9092/metrics", "comma-separated apiserver /metrics URLs")
	run := flag.String("run", "", "run id")
	mode := flag.String("mode", "get", "get | ws")
	conc := flag.Int("c", 64, "concurrent HTTP clients (get) or WebSocket viewers (ws)")
	dur := flag.Duration("d", 15*time.Second, "duration")
	label := flag.String("label", "", "label written into the JSON record")
	flag.Parse()
	if *run == "" {
		fmt.Fprintln(os.Stderr, "-run is required")
		os.Exit(2)
	}
	bases := strings.Split(*apis, ",")
	mets := strings.Split(*metricsURLs, ",")

	before := sumMetric(mets, "evalrunner_api_stats_db_queries_total")
	ctx, cancel := context.WithTimeout(context.Background(), *dur)
	defer cancel()
	start := time.Now()
	var rec map[string]any
	switch *mode {
	case "get":
		rec = loadGet(ctx, bases, *run, *conc)
	case "ws":
		rec = loadWS(ctx, bases, *run, *conc)
	default:
		fmt.Fprintln(os.Stderr, "unknown -mode")
		os.Exit(2)
	}
	elapsed := time.Since(start).Seconds()
	after := sumMetric(mets, "evalrunner_api_stats_db_queries_total")
	rec["label"] = *label
	rec["mode"] = *mode
	rec["replicas"] = len(bases)
	rec["concurrency"] = *conc
	rec["seconds"] = elapsed
	rec["db_stats_queries"] = after - before
	rec["db_stats_queries_per_sec"] = (after - before) / elapsed
	json.NewEncoder(os.Stdout).Encode(rec)
}

func loadGet(ctx context.Context, bases []string, run string, conc int) map[string]any {
	tr := &http.Transport{MaxIdleConnsPerHost: conc, MaxConnsPerHost: conc}
	client := &http.Client{Transport: tr, Timeout: 10 * time.Second}
	var mu sync.Mutex
	var lats []float64
	var errs atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < conc; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			local := make([]float64, 0, 4096)
			for n := i; ctx.Err() == nil; n++ {
				url := bases[n%len(bases)] + "/api/runs/" + run
				t := time.Now()
				res, err := client.Get(url)
				if err == nil {
					io.Copy(io.Discard, res.Body)
					res.Body.Close()
				}
				if err != nil || res.StatusCode != 200 {
					if ctx.Err() == nil {
						errs.Add(1)
					}
					continue
				}
				local = append(local, float64(time.Since(t).Microseconds())/1000)
			}
			mu.Lock()
			lats = append(lats, local...)
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	sort.Float64s(lats)
	return map[string]any{
		"requests": len(lats), "errors": errs.Load(),
		"p50_ms": pct(lats, 0.50), "p95_ms": pct(lats, 0.95), "p99_ms": pct(lats, 0.99),
		"rps_note": "requests / seconds",
	}
}

func loadWS(ctx context.Context, bases []string, run string, viewers int) map[string]any {
	var msgs, errs, opened atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < viewers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			url := strings.Replace(bases[i%len(bases)], "http", "ws", 1) + "/api/runs/" + run + "/ws"
			c, _, err := websocket.Dial(ctx, url, nil)
			if err != nil {
				if ctx.Err() == nil {
					errs.Add(1)
				}
				return
			}
			opened.Add(1)
			defer c.CloseNow()
			for {
				if _, _, err := c.Read(ctx); err != nil {
					return
				}
				msgs.Add(1)
			}
		}(i)
	}
	wg.Wait()
	return map[string]any{"viewers_opened": opened.Load(), "messages": msgs.Load(), "errors": errs.Load()}
}

func pct(s []float64, p float64) float64 {
	if len(s) == 0 {
		return 0
	}
	i := int(p * float64(len(s)-1))
	return s[i]
}

func sumMetric(urls []string, name string) float64 {
	total := 0.0
	for _, u := range urls {
		res, err := http.Get(u)
		if err != nil {
			continue
		}
		sc := bufio.NewScanner(res.Body)
		for sc.Scan() {
			line := sc.Text()
			if strings.HasPrefix(line, name+" ") {
				v, _ := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, name)), 64)
				total += v
			}
		}
		res.Body.Close()
	}
	return total
}
