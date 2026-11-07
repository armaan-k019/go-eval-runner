// Package cachekey derives the result-cache key for an eval job.
//
// The key is sha256 over the job type, the run config, and the case payload,
// with both JSON documents canonicalized (object keys sorted, insignificant
// whitespace removed) so that semantically identical jobs share a key.
package cachekey

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// Canonical re-encodes a JSON document with sorted object keys. Numbers are
// kept as their original literals so 1.0 and 1 stay distinct.
func Canonical(doc []byte) ([]byte, error) {
	if len(bytes.TrimSpace(doc)) == 0 {
		return []byte("null"), nil
	}
	dec := json.NewDecoder(bytes.NewReader(doc))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("canonicalize: %w", err)
	}
	if dec.More() {
		return nil, fmt.Errorf("canonicalize: trailing data after JSON value")
	}
	return json.Marshal(v) // encoding/json sorts map keys
}

// Key returns the hex sha256 cache key for a job.
func Key(jobType string, config, payload []byte) (string, error) {
	c, err := Canonical(config)
	if err != nil {
		return "", fmt.Errorf("config: %w", err)
	}
	p, err := Canonical(payload)
	if err != nil {
		return "", fmt.Errorf("payload: %w", err)
	}
	h := sha256.New()
	h.Write([]byte("evalrunner/v1\x00"))
	h.Write([]byte(jobType))
	h.Write([]byte{0})
	h.Write(c)
	h.Write([]byte{0})
	h.Write(p)
	return hex.EncodeToString(h.Sum(nil)), nil
}
