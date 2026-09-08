package api

import (
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"
	"uuid"
)

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
			status:         http.StatusOK,
		}
		requestID := uuid.New().String()
		next.ServeHTTP(sw, r)
		// calling log.Printf after next.ServeHTTP ensures the duration is accurate
		log.Info(
			"http request",
			"request_id", requestID,
			"method", r.Method,
			"path", r.URL.Path,
			"status", sw.status,
			"latency", time.Since(start),
		)
	})
}
