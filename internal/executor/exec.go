package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// ExecConfig is the run config for the "exec" job type.
type ExecConfig struct {
	// Command and arguments, for example ["python", "-m", "text_to_cad.eval_case"].
	Command []string `json:"command"`
	// Working directory for the process (optional).
	Dir string `json:"dir"`
	// Extra environment, KEY=VALUE.
	Env []string `json:"env"`
}

// execInput is written to the process's stdin.
type execInput struct {
	CaseID  string          `json:"case_id"`
	Payload json.RawMessage `json:"payload"`
	Attempt int             `json:"attempt"`
}

// execOutput is what the process must print on stdout.
type execOutput struct {
	Score     *float64           `json:"score"`
	Passed    *bool              `json:"passed"`
	Metrics   map[string]float64 `json:"metrics"`
	Error     string             `json:"error"`
	Retryable *bool              `json:"retryable"`
}

// ExitPermanent is the exit code a case process uses to say "do not retry".
const ExitPermanent = 3

// Exec runs one OS process per eval case. Protocol: one JSON object in on
// stdin, one JSON object out on stdout, exit 0. This keeps the runner
// language-agnostic: the eval can be Python, a shell script, anything.
type Exec struct {
	// AllowedPrefixes, when set, restricts which commands workers will run.
	AllowedPrefixes []string
}

func (Exec) Name() string { return "exec" }

func (e Exec) Run(ctx context.Context, job Job) (Result, error) {
	var cfg ExecConfig
	if err := json.Unmarshal(job.Config, &cfg); err != nil {
		return Result{}, Permanent(fmt.Errorf("exec config: %w", err))
	}
	if len(cfg.Command) == 0 {
		return Result{}, Permanent(errors.New("exec config: command is empty"))
	}
	if len(e.AllowedPrefixes) > 0 {
		ok := false
		for _, p := range e.AllowedPrefixes {
			if strings.HasPrefix(cfg.Command[0], p) {
				ok = true
			}
		}
		if !ok {
			return Result{}, Permanent(fmt.Errorf("exec: command %q not in worker allowlist", cfg.Command[0]))
		}
	}
	in, _ := json.Marshal(execInput{CaseID: job.CaseID, Payload: job.Payload, Attempt: job.Attempt})

	cmd := exec.CommandContext(ctx, cfg.Command[0], cfg.Command[1:]...)
	cmd.Dir = cfg.Dir
	cmd.Env = append(os.Environ(), cfg.Env...)
	cmd.Stdin = bytes.NewReader(in)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	runErr := cmd.Run()
	if ctx.Err() != nil {
		return Result{}, fmt.Errorf("exec: %w", ctx.Err())
	}
	if runErr != nil {
		msg := fmt.Sprintf("exec: %v: %s", runErr, tail(stderr.String(), 500))
		var ee *exec.ExitError
		if errors.As(runErr, &ee) && ee.ExitCode() == ExitPermanent {
			return Result{}, Permanent(errors.New(msg))
		}
		return Result{}, errors.New(msg)
	}

	out := bytes.TrimSpace(stdout.Bytes())
	// Tolerate log lines before the result: take the last line.
	if i := bytes.LastIndexByte(out, '\n'); i >= 0 {
		out = out[i+1:]
	}
	var parsed execOutput
	if err := json.Unmarshal(out, &parsed); err != nil {
		return Result{}, Permanent(fmt.Errorf("exec: stdout is not a JSON result: %v: %q", err, tail(string(out), 200)))
	}
	if parsed.Error != "" {
		err := fmt.Errorf("exec: case reported error: %s", parsed.Error)
		if parsed.Retryable != nil && *parsed.Retryable {
			return Result{}, err
		}
		return Result{}, Permanent(err)
	}
	return Result{Raw: json.RawMessage(append([]byte(nil), out...))}, nil
}

func tail(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) > n {
		return "..." + s[len(s)-n:]
	}
	return s
}
