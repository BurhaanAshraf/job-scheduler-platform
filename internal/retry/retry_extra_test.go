package retry

import (
	"testing"
	"time"
)

func TestBackoffWithJitterFirstAttempt(t *testing.T) {
	if got := BackoffWithJitter(1); got != BaseDelay {
		t.Fatalf("attempt 1 = %v, want %v", got, BaseDelay)
	}
	if got := BackoffWithJitter(0); got != BaseDelay {
		t.Fatalf("attempt 0 = %v, want %v", got, BaseDelay)
	}
	if got := BackoffWithJitter(-5); got != BaseDelay {
		t.Fatalf("attempt -5 = %v, want %v", got, BaseDelay)
	}
}

func TestBackoffWithJitterBounds(t *testing.T) {
	for attempt := 2; attempt <= 12; attempt++ {
		got := BackoffWithJitter(attempt)
		// Full-jitter range for Backoff(attempt-1) is [base/2, base].
		base := Backoff(attempt - 1)
		if got < base/2 || got > base {
			t.Fatalf("attempt %d: jitter %v outside [%v, %v]", attempt, got, base/2, base)
		}
	}
}

func TestBackoffWithJitterCapsAtMax(t *testing.T) {
	for _, attempt := range []int{30, 100, 1000} {
		if got := BackoffWithJitter(attempt); got > MaxDelay {
			t.Fatalf("attempt %d: %v exceeds MaxDelay", attempt, got)
		}
	}
}

func TestNextRunAt(t *testing.T) {
	now := time.Now().UTC()
	got := NextRunAt(now, 3)
	if !got.After(now) {
		t.Fatalf("NextRunAt not in future: %v", got)
	}
	// Jittered around Backoff(attempt-1): within [base/2, base] of now.
	base := Backoff(2)
	lower, upper := now.Add(base/2), now.Add(base)
	if got.Before(lower) || got.After(upper.Add(time.Second)) {
		t.Fatalf("NextRunAt = %v, want in [%v, %v]", got, lower, upper)
	}
}
