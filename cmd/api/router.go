package main

import (
	"log/slog"
	"net/http"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/api"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/ratelimit"
)

func Server(log *slog.Logger, h *Handler, limiter *ratelimit.Limiter) http.Handler {
	mux := http.NewServeMux()
	auth := api.APIKeyAuth(h.jobRepo)
	rateLimit := api.RateLimit(limiter)
	mux.Handle("POST /v1/jobs", auth(rateLimit(http.HandlerFunc(h.CreateJob))))
	mux.Handle("GET /v1/jobs/{id}", auth(rateLimit(http.HandlerFunc(h.GetJob))))
	mux.Handle("GET /v1/jobs", auth(rateLimit(http.HandlerFunc(h.ListJobs))))
	mux.Handle("DELETE /v1/jobs/{id}", auth(rateLimit(http.HandlerFunc(h.DeleteJob))))
	mux.Handle("GET /v1/dead-letters", auth(rateLimit(http.HandlerFunc(h.ListDeadLetters))))
	mux.Handle("POST /v1/jobs/{id}/retry", auth(rateLimit(http.HandlerFunc(h.RetryJob))))
	return api.Recovery(log, api.Logging(log, mux))
}
