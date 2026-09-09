package main

import (
	"log/slog"
	"net/http"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/api"
)

func Server(log *slog.Logger, h *Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/jobs", h.CreateJob)
	mux.HandleFunc("GET /v1/jobs/{id}", h.GetJob)
	mux.HandleFunc("GET /v1/jobs", h.ListJobs)
	return api.Recovery(log, api.Logging(log, mux))

}
