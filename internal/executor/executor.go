// Package executor defines the generic job interface. An Executor turns one
// eval case (config plus payload, both JSON) into a JSON result. The runner
// never looks inside the payload, so any eval can plug in.
package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// Job is what a worker hands to an executor.
type Job struct {
	JobID   int64
	CaseID  string
	Attempt int
	Config  json.RawMessage
	Payload json.RawMessage
}

// Result is the executor's answer. Score and Passed are optional but, when
// present, are indexed by the store for reporting. Raw is stored verbatim.
type Result struct {
	Raw json.RawMessage
}

// Executor runs one job. Returning an error marks the attempt failed; wrap it
// with Permanent to stop retries.
type Executor interface {
	Name() string
	Run(ctx context.Context, job Job) (Result, error)
}

type permanentError struct{ err error }

func (p permanentError) Error() string { return p.err.Error() }
func (p permanentError) Unwrap() error { return p.err }

// Permanent marks an error as not worth retrying (bad input, missing file).
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err}
}

// Retryable reports whether an attempt that failed with err should be retried.
// Errors are retryable unless explicitly marked permanent.
func Retryable(err error) bool {
	var p permanentError
	return !errors.As(err, &p)
}

// Registry maps job type names to executors.
type Registry map[string]Executor

func (r Registry) Register(e Executor) { r[e.Name()] = e }

func (r Registry) Get(name string) (Executor, error) {
	e, ok := r[name]
	if !ok {
		return nil, fmt.Errorf("no executor registered for job type %q", name)
	}
	return e, nil
}

func (r Registry) Names() []string {
	out := make([]string, 0, len(r))
	for n := range r {
		out = append(out, n)
	}
	return out
}
