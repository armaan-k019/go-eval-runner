package store

import (
	"testing"
	"time"
)

func TestBackoffBounds(t *testing.T) {
	base, max := 100*time.Millisecond, 2*time.Second
	for attempt := 1; attempt <= 40; attempt++ {
		want := base << (attempt - 1)
		if want <= 0 || want > max {
			want = max
		}
		for i := 0; i < 50; i++ {
			d := Backoff(attempt, base, max)
			if d < want/2 || d > want {
				t.Fatalf("attempt %d: %v outside [%v, %v]", attempt, d, want/2, want)
			}
		}
	}
}
