package api

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"

	pb "github.com/armaan-k019/go-eval-runner/gen/evalrunnerv1"
	"github.com/armaan-k019/go-eval-runner/internal/store"
)

// fakeStore finishes a run after a fixed number of stats reads.
type fakeStore struct {
	mu        sync.Mutex
	calls     int
	doneAfter int
}

func (f *fakeStore) RunStats(_ context.Context, id string) (store.RunStats, error) {
	if id != "run-1" {
		return store.RunStats{}, pgx.ErrNoRows
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	st := store.RunStats{RunID: id, Suite: "s", Total: 10}
	if f.calls >= f.doneAfter {
		st.Succeeded = 10
	} else {
		st.Queued = 10 - int64(f.calls)
		st.Succeeded = int64(f.calls)
	}
	return st, nil
}

func (f *fakeStore) ListRuns(context.Context, int) ([]store.RunSummary, error) {
	return []store.RunSummary{{RunID: "run-1", Suite: "s", Total: 10}}, nil
}

type fakeCoord struct {
	pb.CoordinatorClient
	got *pb.SubmitRunRequest
}

func (f *fakeCoord) SubmitRun(_ context.Context, r *pb.SubmitRunRequest, _ ...grpc.CallOption) (*pb.SubmitRunResponse, error) {
	f.got = r
	return &pb.SubmitRunResponse{RunId: "run-1", Jobs: int32(len(r.Cases))}, nil
}

func newTestServer(t *testing.T, st Store, cache *RedisStats, coord pb.CoordinatorClient) (*httptest.Server, *Metrics) {
	t.Helper()
	m := NewMetrics(prometheus.NewRegistry())
	s := New(st, cache, coord, Config{PushInterval: 5 * time.Millisecond}, m, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	return ts, m
}

func TestGetAndList(t *testing.T) {
	ts, _ := newTestServer(t, &fakeStore{doneAfter: 1}, nil, &fakeCoord{})
	res, err := http.Get(ts.URL + "/api/runs/run-1")
	if err != nil {
		t.Fatal(err)
	}
	var v RunView
	json.NewDecoder(res.Body).Decode(&v)
	if res.StatusCode != 200 || !v.Done || v.Succeeded != 10 {
		t.Fatalf("got %d %+v", res.StatusCode, v)
	}
	if res, _ := http.Get(ts.URL + "/api/runs/missing"); res.StatusCode != 404 {
		t.Fatalf("missing run: %d", res.StatusCode)
	}
	res, _ = http.Get(ts.URL + "/api/runs")
	var l struct{ Runs []store.RunSummary }
	json.NewDecoder(res.Body).Decode(&l)
	if len(l.Runs) != 1 {
		t.Fatalf("list: %+v", l)
	}
}

func TestSubmitValidatesAndForwards(t *testing.T) {
	coord := &fakeCoord{}
	ts, _ := newTestServer(t, &fakeStore{doneAfter: 1}, nil, coord)
	res, _ := http.Post(ts.URL+"/api/runs", "application/json", strings.NewReader(`{"job_type":"mock","cases":[]}`))
	if res.StatusCode != 400 {
		t.Fatalf("empty cases: %d", res.StatusCode)
	}
	body := `{"suite":"x","job_type":"mock","config":{"latency_median_ms":1},"cases":[{"case_id":"a","payload":{"k":1}},{"case_id":"b"}]}`
	res, _ = http.Post(ts.URL+"/api/runs", "application/json", bytes.NewBufferString(body))
	if res.StatusCode != 201 {
		t.Fatalf("submit: %d", res.StatusCode)
	}
	if len(coord.got.Cases) != 2 || string(coord.got.Cases[1].PayloadJson) != "{}" || string(coord.got.ConfigJson) != `{"latency_median_ms":1}` {
		t.Fatalf("forwarded %+v", coord.got)
	}
}

func TestWebSocketStreamsUntilDone(t *testing.T) {
	ts, _ := newTestServer(t, &fakeStore{doneAfter: 5}, nil, &fakeCoord{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// lookup consumes one read before the stream starts.
	c, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(ts.URL, "http")+"/api/runs/run-1/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.CloseNow()
	var msgs []RunView
	for {
		var v RunView
		if err := wsjson.Read(ctx, c, &v); err != nil {
			if websocket.CloseStatus(err) != websocket.StatusNormalClosure {
				t.Fatalf("read: %v", err)
			}
			break
		}
		msgs = append(msgs, v)
	}
	if len(msgs) != 4 || !msgs[3].Done || msgs[0].Done {
		t.Fatalf("got %d messages, last %+v", len(msgs), msgs[len(msgs)-1])
	}
}

// TestRedisCacheSharesStats needs a Redis server: EVALRUNNER_TEST_REDIS=localhost:6379.
func TestRedisCacheSharesStats(t *testing.T) {
	addr := os.Getenv("EVALRUNNER_TEST_REDIS")
	if addr == "" {
		t.Skip("EVALRUNNER_TEST_REDIS not set")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()
	rdb.Del(context.Background(), statsKey("run-1"))
	src := &fakeStore{doneAfter: 1000}
	var misses atomic.Int64
	// Two "replicas" sharing one Redis.
	a := &RedisStats{Src: src, RDB: rdb, TTL: time.Minute, DoneTTL: time.Hour, OnMiss: func() { misses.Add(1) }}
	b := &RedisStats{Src: src, RDB: rdb, TTL: time.Minute, DoneTTL: time.Hour, OnMiss: func() { misses.Add(1) }}
	for i := 0; i < 50; i++ {
		if _, err := a.RunStats(context.Background(), "run-1"); err != nil {
			t.Fatal(err)
		}
		if _, err := b.RunStats(context.Background(), "run-1"); err != nil {
			t.Fatal(err)
		}
	}
	if misses.Load() != 1 {
		t.Fatalf("100 reads across 2 replicas cost %d Postgres queries, want 1", misses.Load())
	}
	ttl := rdb.PTTL(context.Background(), statsKey("run-1")).Val()
	if ttl <= 0 || ttl > time.Minute {
		t.Fatalf("live run ttl %v", ttl)
	}
}
