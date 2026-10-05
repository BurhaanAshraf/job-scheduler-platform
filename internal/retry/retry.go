package retry

import (
	"math/rand"
	"time"
)

const (
	BaseDelay = 1 * time.Second
	MaxDelay  = 15 * time.Minute
)

func Backoff(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}

	delay := BaseDelay

	for i := 0; i < attempt; i++ {
		if delay >= MaxDelay/2 {
			return MaxDelay
		}

		delay *= 2
	}

	if delay > MaxDelay {
		return MaxDelay
	}

	return delay
}

// BackoffWithJitter is the production retry delay. attempt is 1-based
// (first execution == 1), so the first retry waits ~BaseDelay.
// Full-jitter spreads retries after mass failures.
func BackoffWithJitter(attempt int) time.Duration {
	if attempt <= 1 {
		return BaseDelay
	}
	base := Backoff(attempt - 1)
	half := int64(base) / 2
	if half <= 0 {
		return base
	}
	return time.Duration(half + rand.Int63n(half+1))
}

func NextRunAt(now time.Time, attempt int) time.Time {
	return now.Add(BackoffWithJitter(attempt))
}
