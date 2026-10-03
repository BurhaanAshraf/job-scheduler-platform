package api

import (
	"context"
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/google/uuid"
)

type requestIDKey string

const RequestIDKey requestIDKey = "request_id"

type StatusWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (w *StatusWriter) WriteHeader(status int) {
	if w.wroteHeader {
		return
	}
	w.status = status
	w.wroteHeader = true
	w.ResponseWriter.WriteHeader(status)

}

func (w *StatusWriter) Write(body []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(body)
}

func Recovery(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			// recover only works when called from a deferred function during a panic
			if err := recover(); err != nil {

				log.Error("panic recovered", "error", err, "stack", string(debug.Stack()))

				WriteError(w, http.StatusInternalServerError, "INTERNAL_SERVER_ERROR", "internal server error")

			}
		}()

		next.ServeHTTP(w, r)
	})
}

func Logging(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		start := time.Now()
		sw := &StatusWriter{
			ResponseWriter: w,
			status:         0,
		}
		requestID := uuid.New().String()
		w.Header().Set("X-Request-ID", requestID)
		ctx := context.WithValue(r.Context(), RequestIDKey, requestID)
		next.ServeHTTP(sw, r.WithContext(ctx))
		status := sw.status
		if !sw.wroteHeader {
			status = http.StatusOK
		}
		// calling log after next.ServeHTTP ensures the duration is accurate
		log.Info(
			"http request",
			"request_id", requestID,
			"method", r.Method,
			"path", r.URL.Path,
			"status", status,
			"latency", time.Since(start).String(),
			"latency_ms", time.Since(start).Milliseconds(),
		)
	})
}

// RequestIDFromContext returns the request ID injected by Logging.
func RequestIDFromContext(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(RequestIDKey).(string)
	return id, ok
}

// RequestIDFromContextOrEmpty is the log-field helper for 10.3: it returns
// the request id or "" when the handler is invoked without the Logging
// middleware (unit tests).
func RequestIDFromContextOrEmpty(ctx context.Context) string {
	id, _ := RequestIDFromContext(ctx)
	return id
}
