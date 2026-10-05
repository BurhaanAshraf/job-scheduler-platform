package api

import (
	"fmt"
	"net/http"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/ratelimit"
)

func RateLimit(limiter *ratelimit.Limiter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Bucket by key ID, not client name: names are not unique, so
			// name-bucketing lets one key eat another key's quota.
			keyID, ok := ClientKeyIDFromContext(r.Context())
			if !ok {
				WriteError(
					w,
					http.StatusUnauthorized,
					"UNAUTHORIZED",
					"client identity missing",
				)
				return
			}

			allowed, ttl, err := limiter.Allow(r.Context(), "key:"+keyID)
			if err != nil {
				WriteError(
					w,
					http.StatusInternalServerError,
					"INTERNAL_SERVER_ERROR",
					"rate limiter failure",
				)
				return
			}

			if !allowed {
				retryAfter := int64(ttl.Seconds())
				if retryAfter < 1 {
					retryAfter = 1
				}
				w.Header().Set("Retry-After", fmt.Sprintf("%d", retryAfter))
				WriteError(
					w,
					http.StatusTooManyRequests,
					"RATE_LIMIT_EXCEEDED",
					"rate limit exceeded",
				)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
