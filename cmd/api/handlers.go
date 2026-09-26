package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/api"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/repository"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/scheduler"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/stream"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	defaultJobListLimit = 20
	maxJobListLimit     = 100
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
	jobRepo  *repository.JobRepository
	cronRepo *repository.CronJobRepository
	redis    *redis.Client
}

type CreateCronJobRequest struct {
	CronExpression string          `json:"cron_expression"`
	JobTemplate    json.RawMessage `json:"job_template"`
}

type UpdateCronJobRequest struct {
	Enabled *bool `json:"enabled"`
}

func NewHandler(
	jobRepo *repository.JobRepository,
	cronRepo *repository.CronJobRepository,
	redisClient *redis.Client,
) *Handler {
	return &Handler{
		jobRepo:  jobRepo,
		cronRepo: cronRepo,
		redis:    redisClient,
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

	if err == nil {
		if req.RunAt.After(time.Now().UTC()) {
			if err := stream.ScheduleJob(
				r.Context(),
				h.redis,
				jobID.String(),
				req.Payload,
				1,
				req.RunAt,
			); err != nil {
				api.WriteError(
					w,
					http.StatusInternalServerError,
					"INTERNAL_SERVER_ERROR",
					"failed to schedule job",
				)
				return
			}
		} else {
			if _, err := stream.EnqueueDue(
				r.Context(),
				h.redis,
				jobID.String(),
				req.Payload,
				1,
			); err != nil {
				api.WriteError(
					w,
					http.StatusInternalServerError,
					"INTERNAL_SERVER_ERROR",
					"failed to enqueue job",
				)
				return
			}
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)

		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": jobID,
		})
		return
	}

	if errors.Is(err, repository.ErrConflict) {
		job, lookupErr := h.jobRepo.GetByIdempotencyKey(
			r.Context(),
			req.IdempotencyKey,
		)
		if lookupErr != nil {
			api.WriteError(
				w,
				http.StatusInternalServerError,
				"INTERNAL_SERVER_ERROR",
				"failed to retrieve existing job",
			)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": job.ID,
		})
		return
	}

	api.WriteError(
		w,
		http.StatusInternalServerError,
		"INTERNAL_SERVER_ERROR",
		"failed to create job",
	)
}

func (h *Handler) GetJob(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))

	if err != nil {
		api.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid job id")

		return
	}

	job, err := h.jobRepo.GetByID(r.Context(), id)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			api.WriteError(w, http.StatusNotFound, "NOT_FOUND", "job not found")
			return
		}

		api.WriteError(w, http.StatusInternalServerError, "INTERNAL_SERVER_ERROR", "failed to retrieve job")

		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	_ = json.NewEncoder(w).Encode(job)
}

func (h *Handler) ListJobs(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()

	status := query.Get("status")

	limit := defaultJobListLimit
	if value := query.Get("limit"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed <= 0 {
			api.WriteError(
				w,
				http.StatusBadRequest,
				"INVALID_REQUEST",
				"limit must be a positive integer",
			)
			return
		}

		limit = parsed
	}

	if limit > maxJobListLimit {
		limit = maxJobListLimit
	}

	offset := 0
	if value := query.Get("offset"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 0 {
			api.WriteError(
				w,
				http.StatusBadRequest,
				"INVALID_REQUEST",
				"offset must be a non-negative integer",
			)
			return
		}

		offset = parsed
	}

	jobs, err := h.jobRepo.ListByStatus(
		r.Context(),
		status,
		limit,
		offset,
	)
	if err != nil {
		api.WriteError(
			w,
			http.StatusBadRequest,
			"INVALID_REQUEST",
			err.Error(),
		)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	_ = json.NewEncoder(w).Encode(jobs)
}

func (h *Handler) DeleteJob(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		api.WriteError(
			w, http.StatusBadRequest, "INVALID_REQUEST", "invalid job id",
		)
		return
	}

	err = h.jobRepo.Cancel(r.Context(), id)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			api.WriteError(w, http.StatusNotFound, "NOT_FOUND", "job not found")
			return
		}
		if errors.Is(err, repository.ErrNotCancellable) {
			api.WriteError(w, http.StatusConflict, "CONFLICT", err.Error())
			return
		}
		api.WriteError(w, http.StatusInternalServerError, "INTERNAL_SERVER_ERROR", "failed to cancel job")

		return
	}

	w.WriteHeader(http.StatusNoContent)

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

func (h *Handler) ListDeadLetters(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()

	limit := defaultJobListLimit
	if value := query.Get("limit"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed <= 0 {
			api.WriteError(
				w,
				http.StatusBadRequest,
				"INVALID_REQUEST",
				"limit must be a positive integer",
			)
			return
		}

		limit = parsed
	}

	if limit > maxJobListLimit {
		limit = maxJobListLimit
	}

	offset := 0
	if value := query.Get("offset"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 0 {
			api.WriteError(
				w,
				http.StatusBadRequest,
				"INVALID_REQUEST",
				"offset must be a non-negative integer",
			)
			return
		}

		offset = parsed
	}

	jobs, err := h.jobRepo.ListByStatus(
		r.Context(),
		repository.StatusDead,
		limit,
		offset,
	)
	if err != nil {
		api.WriteError(
			w,
			http.StatusInternalServerError,
			"INTERNAL_SERVER_ERROR",
			"failed to list dead-lettered jobs",
		)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	_ = json.NewEncoder(w).Encode(jobs)
}

func (h *Handler) RetryJob(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		api.WriteError(
			w,
			http.StatusBadRequest,
			"INVALID_REQUEST",
			"invalid job id",
		)
		return
	}

	job, err := h.jobRepo.GetByID(r.Context(), id)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			api.WriteError(
				w,
				http.StatusNotFound,
				"NOT_FOUND",
				"job not found",
			)
			return
		}

		api.WriteError(
			w,
			http.StatusInternalServerError,
			"INTERNAL_SERVER_ERROR",
			"failed to retrieve job",
		)
		return
	}

	if job.Status != repository.StatusDead {
		api.WriteError(
			w,
			http.StatusConflict,
			"CONFLICT",
			"only dead jobs can be retried",
		)
		return
	}

	if err := h.jobRepo.Retry(r.Context(), id); err != nil {
		api.WriteError(
			w,
			http.StatusInternalServerError,
			"INTERNAL_SERVER_ERROR",
			"failed to retry job",
		)
		return
	}

	job, err = h.jobRepo.GetByID(r.Context(), id)
	if err != nil {
		api.WriteError(
			w,
			http.StatusInternalServerError,
			"INTERNAL_SERVER_ERROR",
			"failed to retrieve retried job",
		)
		return
	}

	if _, err := stream.EnqueueDue(
		r.Context(),
		h.redis,
		job.ID.String(),
		job.Payload,
		job.QueueGeneration,
	); err != nil {
		api.WriteError(
			w,
			http.StatusInternalServerError,
			"INTERNAL_SERVER_ERROR",
			"failed to enqueue retried job",
		)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	_ = json.NewEncoder(w).Encode(job)
}

func (h *Handler) CreateCronJob(w http.ResponseWriter, r *http.Request) {
	var req CreateCronJobRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(
			w,
			http.StatusBadRequest,
			"invalid_json",
			"invalid JSON body",
		)
		return
	}

	if strings.TrimSpace(req.CronExpression) == "" {
		api.WriteError(
			w,
			http.StatusBadRequest,
			"invalid_cron_expression",
			"cron_expression is required",
		)
		return
	}

	if len(req.JobTemplate) == 0 || string(req.JobTemplate) == "null" {
		api.WriteError(
			w,
			http.StatusBadRequest,
			"invalid_job_template",
			"job_template is required",
		)
		return
	}

	if !json.Valid(req.JobTemplate) {
		api.WriteError(
			w,
			http.StatusBadRequest,
			"invalid_job_template",
			"job_template must be valid JSON",
		)
		return
	}

	now := time.Now().UTC()

	nextRunAt, err := scheduler.NextRunAt(req.CronExpression, now)
	if err != nil {
		api.WriteError(
			w,
			http.StatusBadRequest,
			"invalid_cron_expression",
			err.Error(),
		)
		return
	}

	cronJob, err := h.cronRepo.Create(
		r.Context(),
		repository.CreateCronJobInput{
			CronExpression: req.CronExpression,
			JobTemplate:    req.JobTemplate,
			NextRunAt:      nextRunAt,
			Enabled:        true,
		},
	)
	if err != nil {
		api.WriteError(
			w,
			http.StatusInternalServerError,
			"cron_job_creation_failed",
			"failed to create cron job",
		)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)

	_ = json.NewEncoder(w).Encode(map[string]int64{
		"id": cronJob.ID,
	})
}

func (h *Handler) UpdateCronJob(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		api.WriteError(
			w,
			http.StatusBadRequest,
			"invalid_cron_job_id",
			"invalid cron job id",
		)
		return
	}

	var req UpdateCronJobRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteError(
			w,
			http.StatusBadRequest,
			"invalid_json",
			"invalid JSON body",
		)
		return
	}

	if req.Enabled == nil {
		api.WriteError(
			w,
			http.StatusBadRequest,
			"invalid_enabled",
			"enabled is required",
		)
		return
	}

	if err := h.cronRepo.UpdateEnabled(
		r.Context(),
		id,
		*req.Enabled,
	); err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			api.WriteError(
				w,
				http.StatusNotFound,
				"cron_job_not_found",
				"cron job not found",
			)
			return
		}

		api.WriteError(
			w,
			http.StatusInternalServerError,
			"cron_job_update_failed",
			"failed to update cron job",
		)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
