package main

import (
	"context"
	"net/http"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/api"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/health"
	"github.com/redis/go-redis/v9"
)

type redisPinger struct {
	client *redis.Client
}

func (p redisPinger) Ping(ctx context.Context) error {
	return p.client.Ping(ctx).Err()
}

type HealthHandler struct {
	checker *health.Checker
}

func NewHealthHandler(checker *health.Checker) *HealthHandler {
	return &HealthHandler{
		checker: checker,
	}
}

func (h *HealthHandler) Healthz(w http.ResponseWriter, r *http.Request) {
	if err := h.checker.Check(r.Context()); err != nil {
		api.WriteError(
			w,
			http.StatusServiceUnavailable,
			"SERVICE_UNAVAILABLE",
			"service dependencies unavailable",
		)
		return
	}

	w.WriteHeader(http.StatusOK)
}
