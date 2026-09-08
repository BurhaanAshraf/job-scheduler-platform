package main

import (
	"log/slog"
	"net/http"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/api"
)

func Server(log *slog.Logger, h *Handler) http.Handler {
	// creating a ServeMux
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/jobs", h.CreateJob)
	return api.Recovery(log, api.Logging(log, mux))

}
