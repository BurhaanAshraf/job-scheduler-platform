package api

import (
	"net/http"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/ratelimit"
)

func RateLimit(limiter *ratelimit.Limiter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			clientName, ok := ClientNameFromContext(r.Context())
			if !ok {
				WriteError(
					w,
					http.StatusUnauthorized,
					"UNAUTHORIZED",
					"client identity missing",
				)
				return
			}

			allowed, err := limiter.Allow(r.Context(), clientName)
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
