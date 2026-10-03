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

func TestBackoffWithJitter(t *testing.T) {
	if got := BackoffWithJitter(1); got != BaseDelay {
		t.Fatalf("BackoffWithJitter(1) = %v, want %v", got, BaseDelay)
	}
	if got := BackoffWithJitter(0); got != BaseDelay {
		t.Fatalf("BackoffWithJitter(0) = %v, want %v", got, BaseDelay)
	}
	// attempt 2 derives from Backoff(1)=2s: full-jitter range [1s, 2s].
	for i := 0; i < 50; i++ {
		got := BackoffWithJitter(2)
		if got < 1*time.Second || got > 2*time.Second {
			t.Fatalf("BackoffWithJitter(2) = %v, want in [1s,2s]", got)
		}
	}
	// attempt 4 derives from Backoff(3)=8s: range [4s,8s].
	for i := 0; i < 50; i++ {
		got := BackoffWithJitter(4)
		if got < 4*time.Second || got > 8*time.Second {
			t.Fatalf("BackoffWithJitter(4) = %v, want in [4s,8s]", got)
		}
	}
	// Cap: huge attempt never exceeds MaxDelay.
	for _, a := range []int{30, 100} {
		if got := BackoffWithJitter(a); got > MaxDelay {
			t.Fatalf("BackoffWithJitter(%d) = %v, want <= %v", a, got, MaxDelay)
		}
	}
}
