package main

import (
	"log/slog"
	"net/http"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/api"
)

func Server(log *slog.Logger, h *Handler) http.Handler {
	mux := http.NewServeMux()
	auth := api.APIKeyAuth(h.jobRepo)
	mux.Handle("POST /v1/jobs", auth(http.HandlerFunc(h.CreateJob)))
	mux.Handle("GET /v1/jobs/{id}", auth(http.HandlerFunc(h.GetJob)))
	mux.Handle("GET /v1/jobs", auth(http.HandlerFunc(h.ListJobs)))
	mux.Handle("DELETE /v1/jobs/{id}", auth(http.HandlerFunc(h.DeleteJob)))
	return api.Recovery(log, api.Logging(log, mux))
}
