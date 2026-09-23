package retry

import "time"

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

func NextRunAt(now time.Time, attempt int) time.Time {
	return now.Add(Backoff(attempt))
}
