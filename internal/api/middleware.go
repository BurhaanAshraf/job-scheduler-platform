package api

import (
	"log"
	"net/http"
	"time"
)

func Recovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			// recover only works when called from a deferred function during a panic
			if err := recover(); err != nil {
				WriteError(w , http.StatusInternalServerError , "INTERNAL_SERVER_ERROR" , "internal server error")

			}
		}()

		next.ServeHTTP(w, r)
	})
}

func Logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		start := time.Now()
		next.ServeHTTP(w, r)
		// calling log.Printf after next.ServeHTTP ensures the duration is accurate
		log.Printf("method=%s path=%s duration=%s", r.Method, r.URL.Path, time.Since(start))
	})
}
