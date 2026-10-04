package main

import (
	_ "embed"
	"log/slog"
	"net/http"
	"text/template"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/api"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/ratelimit"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

//go:embed openapi.yaml
var openAPISpec string

//go:embed swagger-ui.html
var swaggerUIHTML string

//go:embed dashboard.html
var dashboardHTML string

func Server(log *slog.Logger, h *Handler, healthHandler *HealthHandler, limiter *ratelimit.Limiter) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /healthz", http.HandlerFunc(healthHandler.Healthz))
	mux.Handle("GET /metrics", promhttp.Handler())
	auth := api.APIKeyAuth(h.jobRepo)
	rateLimit := api.RateLimit(limiter)

	mux.Handle("POST /v1/jobs", auth(rateLimit(http.HandlerFunc(h.CreateJob))))
	mux.Handle("GET /v1/jobs/{id}", auth(rateLimit(http.HandlerFunc(h.GetJob))))
	mux.Handle("GET /v1/jobs", auth(rateLimit(http.HandlerFunc(h.ListJobs))))
	mux.Handle("DELETE /v1/jobs/{id}", auth(rateLimit(http.HandlerFunc(h.DeleteJob))))
	mux.Handle("GET /v1/dead-letters", auth(rateLimit(http.HandlerFunc(h.ListDeadLetters))))
	mux.Handle("POST /v1/jobs/{id}/retry", auth(rateLimit(http.HandlerFunc(h.RetryJob))))
	mux.Handle("POST /v1/cron-jobs", auth(rateLimit(http.HandlerFunc(h.CreateCronJob))))
	mux.Handle("PATCH /v1/cron-jobs/{id}", auth(rateLimit(http.HandlerFunc(h.UpdateCronJob))))

	// OpenAPI spec endpoint
	mux.HandleFunc("GET /openapi.yaml", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/yaml")
		w.Write([]byte(openAPISpec))
	})

	// Swagger UI
	mux.HandleFunc("GET /docs", func(w http.ResponseWriter, r *http.Request) {
		tmpl, err := template.New("swagger").Parse(swaggerUIHTML)
		if err != nil {
			http.Error(w, "Template error", http.StatusInternalServerError)
			return
		}
		data := struct {
			SpecURL string
		}{
			SpecURL: "/openapi.yaml",
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		tmpl.Execute(w, data)
	})

	// Dashboard
	mux.HandleFunc("GET /dashboard", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(dashboardHTML))
	})

	return api.Recovery(log, api.Logging(log, mux))
}
