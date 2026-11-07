package executor

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeScript(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "case.sh")
	if err := os.WriteFile(p, []byte("#!/bin/sh\n"+body), 0o755); err != nil {
		t.Fatal(err)
	}
	return p
}

func execJob(t *testing.T, script string) Job {
	cfg, _ := json.Marshal(ExecConfig{Command: []string{script}})
	return Job{CaseID: "c1", Attempt: 2, Config: cfg, Payload: json.RawMessage(`{"prompt":"cube"}`)}
}

func TestExecContract(t *testing.T) {
	// The case reads stdin and echoes fields back, proving the input shape.
	s := writeScript(t, `in=$(cat)
echo "log line the runner should skip"
case "$in" in *'"case_id":"c1"'*'"attempt":2'*) echo '{"score":0.75,"passed":true,"metrics":{"iou":0.9}}';; *) echo "bad input: $in" >&2; exit 1;; esac`)
	res, err := Exec{}.Run(context.Background(), execJob(t, s))
	if err != nil {
		t.Fatal(err)
	}
	var out struct {
		Score   float64            `json:"score"`
		Metrics map[string]float64 `json:"metrics"`
	}
	if err := json.Unmarshal(res.Raw, &out); err != nil || out.Score != 0.75 || out.Metrics["iou"] != 0.9 {
		t.Fatalf("result %s err %v", res.Raw, err)
	}
}

func TestExecErrorClassification(t *testing.T) {
	cases := map[string]struct {
		body      string
		retryable bool
	}{
		"nonzero exit":        {`exit 1`, true},
		"permanent exit code": {`exit 3`, false},
		"reported retryable":  {`echo '{"error":"rate limited","retryable":true}'`, true},
		"reported error":      {`echo '{"error":"bad geometry"}'`, false},
		"garbage stdout":      {`echo not json`, false},
	}
	for name, c := range cases {
		_, err := Exec{}.Run(context.Background(), execJob(t, writeScript(t, c.body)))
		if err == nil {
			t.Errorf("%s: expected error", name)
			continue
		}
		if Retryable(err) != c.retryable {
			t.Errorf("%s: retryable=%v want %v (%v)", name, Retryable(err), c.retryable, err)
		}
	}
}

func TestExecTimeoutKillsProcess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := Exec{}.Run(ctx, execJob(t, writeScript(t, `exec sleep 5`)))
	if err == nil || !Retryable(err) {
		t.Fatalf("want retryable timeout error, got %v", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("process was not killed on timeout")
	}
}

func TestExecAllowlist(t *testing.T) {
	_, err := Exec{AllowedPrefixes: []string{"/opt/evals/"}}.Run(context.Background(), execJob(t, writeScript(t, `echo '{}'`)))
	if err == nil || Retryable(err) || !strings.Contains(err.Error(), "allowlist") {
		t.Fatalf("want permanent allowlist error, got %v", err)
	}
}

func TestMockScoreDeterministic(t *testing.T) {
	j := Job{Attempt: 1, Payload: json.RawMessage(`{"task":5}`)}
	a, err := Mock{}.Run(context.Background(), j)
	if err != nil {
		t.Fatal(err)
	}
	j.Attempt = 2
	b, _ := Mock{}.Run(context.Background(), j)
	var sa, sb struct{ Score float64 }
	json.Unmarshal(a.Raw, &sa)
	json.Unmarshal(b.Raw, &sb)
	if sa.Score != sb.Score {
		t.Fatalf("score changed between attempts: %v vs %v", sa.Score, sb.Score)
	}
}

func TestMockFailRate(t *testing.T) {
	fails := 0
	const n = 4000
	for i := 0; i < n; i++ {
		_, err := Mock{}.Run(context.Background(), Job{Attempt: 1, Config: json.RawMessage(`{"fail_rate":0.2}`), Payload: json.RawMessage(`{"task":1}`)})
		if err != nil {
			fails++
		}
	}
	// 0.2 * 4000 = 800, sd about 25. Allow a wide band.
	if fails < 650 || fails > 950 {
		t.Fatalf("fail_rate 0.2 produced %d/%d failures", fails, n)
	}
}
