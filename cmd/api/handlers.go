package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/api"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/repository"
)

type CreateJobRequest struct {
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	RunAt          time.Time       `json:"run_at"`
	MaxAttempts    int             `json:"max_attempts"`
	IdempotencyKey string          `json:"idempotency_key"`
	CallbackURL    *string         `json:"callback_url"`
}

type Handler struct {
	jobRepo *repository.JobRepository
}

func NewHandler(jobRepo *repository.JobRepository) *Handler {
	return &Handler{
		jobRepo: jobRepo,
	}
}

func (h *Handler) CreateJob(w http.ResponseWriter, r *http.Request) {
	var req CreateJobRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid request body")
		return
	}

	if err := validateCreateJobRequest(req); err != nil {
		api.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
		return
	}

	input := repository.CreateJobInput{
		Type:           req.Type,
		Payload:        req.Payload,
		RunAt:          req.RunAt,
		MaxAttempts:    req.MaxAttempts,
		IdempotencyKey: req.IdempotencyKey,
		CallbackURL:    req.CallbackURL,
	}

	jobID, err := h.jobRepo.Create(r.Context(), input)

	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, "INTERNAL_SERVER_ERROR", "failed to create job")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"id": jobID,
	})
}

func validateCreateJobRequest(req CreateJobRequest) error {
	if req.Type == "" {
		return fmt.Errorf("type is required")
	}

	if len(req.Payload) == 0 || !json.Valid(req.Payload) {
		return fmt.Errorf("payload must be valid JSON")
	}

	if req.MaxAttempts <= 0 {
		return fmt.Errorf("max_attempts must be greater than 0")
	}

	if req.IdempotencyKey == "" {
		return fmt.Errorf("idempotency_key is required")
	}
	if req.CallbackURL == nil || strings.TrimSpace(*req.CallbackURL) == "" {
		return fmt.Errorf("callback_url is required")
	}

	u, err := url.Parse(*req.CallbackURL)

	if err != nil || !u.IsAbs() || u.Host == "" {
		return fmt.Errorf("callback_url must be a valid absolute URL")
	}

	return nil
}
