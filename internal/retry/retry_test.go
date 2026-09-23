package retry

import (
	"strconv"
	"testing"
	"time"
)

func TestBackoff(t *testing.T) {
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 0, want: 1 * time.Second},
		{attempt: 1, want: 2 * time.Second},
		{attempt: 2, want: 4 * time.Second},
		{attempt: 3, want: 8 * time.Second},
		{attempt: 4, want: 16 * time.Second},
		{attempt: 5, want: 32 * time.Second},
		{attempt: 6, want: 64 * time.Second},
		{attempt: 20, want: 15 * time.Minute},
	}

	for _, tt := range tests {
		t.Run("attempt_"+strconv.Itoa(tt.attempt), func(t *testing.T) {
			got := Backoff(tt.attempt)

			if got != tt.want {
				t.Fatalf(
					"Backoff(%d) = %v, want %v",
					tt.attempt,
					got,
					tt.want,
				)
			}
		})
	}
}
