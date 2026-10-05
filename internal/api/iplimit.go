package api

import (
	"fmt"
	"net"
	"net/http"
	"strings"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/ratelimit"
)

// IPRateLimit throttles by client IP outside authentication, so
// unauthenticated traffic (key guessing, scrape floods) is bounded. The
// budget is deliberately generous — per-key quotas still enforce fairness
// after auth. A nil limiter disables the check (tests, local dev).
func IPRateLimit(limiter *ratelimit.Limiter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if limiter == nil {
				next.ServeHTTP(w, r)
				return
			}

			allowed, ttl, err := limiter.Allow(r.Context(), "ip:"+clientIP(r))
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

// clientIP prefers the leftmost X-Forwarded-For entry (what ALBs and other
// proxies append the original client to) and falls back to the connection's
// remote address. The leftmost entry is client-controlled, so this is a
// throttle — not an identity — and must never gate authentication.
func clientIP(r *http.Request) string {
	if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
		if first, _, _ := strings.Cut(fwd, ","); strings.TrimSpace(first) != "" {
			return strings.TrimSpace(first)
		}
	}
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	if strings.TrimSpace(r.RemoteAddr) != "" {
		return strings.TrimSpace(r.RemoteAddr)
	}
	return "unknown"
}
