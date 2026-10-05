package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/api"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/metrics"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/repository"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/scheduler"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/stream"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/validator"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	defaultJobListLimit = 20
	maxJobListLimit     = 100
	maxBodyBytes        = 1 << 20 // 1 MiB
	maxTypeLen          = 128
	maxIdempotencyLen   = 128
	maxAttemptsLimit    = 100
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
	logger   *slog.Logger
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
	logger *slog.Logger,
) *Handler {
	return &Handler{
		jobRepo:  jobRepo,
		cronRepo: cronRepo,
		redis:    redisClient,
		logger:   logger,
	}
}

func (h *Handler) CreateJob(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	defer func() { _ = r.Body.Close() }()

	var req CreateJobRequest

	if !decodeJSONBody(w, r, &req, "invalid request body") {
		return
	}

	if err := validateCreateJobRequest(r.Context(), req); err != nil {
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
		if err := metrics.Increment(
			r.Context(),
			h.redis,
			metrics.JobsSubmittedKey,
		); err != nil {
			h.logger.ErrorContext(
				r.Context(),
				"failed to update submitted jobs metric",
				"job_id", jobID,
				"error", err,
			)
		}

		h.logger.InfoContext(
			r.Context(),
			"job submitted",
			"job_id", jobID,
			"request_id", api.RequestIDFromContextOrEmpty(r.Context()),
		)
	}

	if err == nil {
		enqueueErr := error(nil)
		if req.RunAt.After(time.Now().UTC()) {
			enqueueErr = stream.ScheduleJob(
				r.Context(),
				h.redis,
				jobID.String(),
				req.Payload,
				1,
				req.RunAt,
			)
		} else {
			_, enqueueErr = stream.EnqueueDue(
				r.Context(),
				h.redis,
				jobID.String(),
				req.Payload,
				1,
			)
		}
		if enqueueErr != nil {
			// DB row exists but Redis enqueue failed. The outbox row
			// written in the same transaction as the job lets the
			// scheduler reconciler re-enqueue it; log for visibility.
			h.logger.ErrorContext(
				r.Context(),
				"job created in DB but Redis enqueue failed; outbox reconciler will retry",
				"job_id", jobID,
				"error", enqueueErr,
			)
			api.WriteError(
				w,
				http.StatusInternalServerError,
				"INTERNAL_SERVER_ERROR",
				"failed to schedule job",
			)
			return
		}

		// Best-effort: the reconciler reaps leftovers, so a cleanup
		// failure here is logged, never fatal.
		if err := h.jobRepo.DeleteOutboxEntries(r.Context(), jobID); err != nil {
			h.logger.ErrorContext(r.Context(), "delete outbox entry failed", "job_id", jobID, "error", err)
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
		// Idempotency must be safe: same key + different payload is a
		// client bug, not a replay. Return 409 on mismatch.
		// NOTE: payloads are compared semantically, not byte-wise:
		// Postgres jsonb normalizes whitespace (e.g. {"a":1} is stored
		// as {"a": 1}), so byte comparison would 409 true replays.
		if job.Type != req.Type || !jsonPayloadEqual(job.Payload, req.Payload) ||
			job.MaxAttempts != req.MaxAttempts ||
			(job.CallbackURL == nil && req.CallbackURL != nil) ||
			(job.CallbackURL != nil && req.CallbackURL == nil) ||
			(job.CallbackURL != nil && req.CallbackURL != nil && *job.CallbackURL != *req.CallbackURL) {
			api.WriteError(
				w,
				http.StatusConflict,
				"IDEMPOTENCY_KEY_CONFLICT",
				"idempotency key already used with different parameters",
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

	var jobs []repository.Job
	var err error
	if status == "" {
		jobs, err = h.jobRepo.List(r.Context(), limit, offset)
	} else {
		jobs, err = h.jobRepo.ListByStatus(
			r.Context(),
			status,
			limit,
			offset,
		)
	}
	if err != nil {
		// Invalid status is a 400; DB failures are 500. Distinguish by message.
		if strings.Contains(err.Error(), "invalid job status") {
			api.WriteError(
				w,
				http.StatusBadRequest,
				"INVALID_REQUEST",
				err.Error(),
			)
			return
		}
		api.WriteError(
			w,
			http.StatusInternalServerError,
			"INTERNAL_SERVER_ERROR",
			"failed to list jobs",
		)
		return
	}

	if jobs == nil {
		jobs = []repository.Job{}
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
			api.WriteError(w, http.StatusConflict, "CONFLICT", "job cannot be cancelled in its current state") //nolint:misspell // 'cancelled' is the contractual spelling (DB CHECK constraint)
			return
		}
		api.WriteError(w, http.StatusInternalServerError, "INTERNAL_SERVER_ERROR", "failed to cancel job")

		return
	}

	// Best-effort: remove from delayed queue so a canceled job is never
	// promoted after the DB status flips. Failures are logged, not fatal,
	// because the worker's generation guard provides a second line of defense.
	if h.redis != nil {
		if err := stream.RemoveScheduled(r.Context(), h.redis, id.String()); err != nil {
			h.logger.ErrorContext(r.Context(), "failed to remove cancelled job from schedule", "job_id", id, "error", err) //nolint:misspell // 'cancelled' is the contractual spelling (DB CHECK constraint)
		}
	}

	w.WriteHeader(http.StatusNoContent)

}

// decodeJSONBody decodes a single JSON object with unknown fields rejected.
// Trailing garbage after the first value is a 400 (it is never a valid
// request), and bodies over maxBodyBytes are a 413, not a 400, so clients
// can distinguish "too large" from "malformed".
func decodeJSONBody(w http.ResponseWriter, r *http.Request, dst any, malformedMsg string) bool {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			api.WriteError(w, http.StatusRequestEntityTooLarge, "REQUEST_TOO_LARGE", "request body exceeds 1 MiB")
			return false
		}
		api.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST", malformedMsg)
		return false
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		api.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST", malformedMsg)
		return false
	}
	return true
}

// jsonPayloadEqual compares two JSON payloads semantically. Byte comparison
// is wrong here because Postgres jsonb re-serializes documents (whitespace,
// key order), so a byte-identical replay from the client would otherwise
// 409. Falls back to byte comparison if either side is not valid JSON.
func jsonPayloadEqual(a, b json.RawMessage) bool {
	var va, vb any
	if err := json.Unmarshal(a, &va); err != nil {
		return string(a) == string(b)
	}
	if err := json.Unmarshal(b, &vb); err != nil {
		return string(a) == string(b)
	}
	return reflect.DeepEqual(va, vb)
}

func validateCreateJobRequest(ctx context.Context, req CreateJobRequest) error {
	if strings.TrimSpace(req.Type) == "" {
		return fmt.Errorf("type is required")
	}
	if len(req.Type) > maxTypeLen {
		return fmt.Errorf("type must be at most %d characters", maxTypeLen)
	}

	if len(req.Payload) == 0 || len(req.Payload) > maxBodyBytes || !json.Valid(req.Payload) {
		return fmt.Errorf("payload must be valid JSON")
	}

	if req.MaxAttempts <= 0 || req.MaxAttempts > maxAttemptsLimit {
		return fmt.Errorf("max_attempts must be between 1 and %d", maxAttemptsLimit)
	}

	if strings.TrimSpace(req.IdempotencyKey) == "" {
		return fmt.Errorf("idempotency_key is required")
	}
	if len(req.IdempotencyKey) > maxIdempotencyLen {
		return fmt.Errorf("idempotency_key must be at most %d characters", maxIdempotencyLen)
	}
	if req.CallbackURL == nil || strings.TrimSpace(*req.CallbackURL) == "" {
		return fmt.Errorf("callback_url is required")
	}

	if err := validateCallbackURL(ctx, *req.CallbackURL); err != nil {
		return err
	}

	return nil
}

// allowPrivateCallbackIPs mirrors the worker's JOB_SCHEDULER_ALLOW_PRIVATE_IPS
// flag so submit-time validation enforces exactly what delivery enforces.
// Dev-only: set "true" in compose to reach the in-network demo sink.
// Production task definitions omit it, keeping the guard strict.
var allowPrivateCallbackIPs = os.Getenv("JOB_SCHEDULER_ALLOW_PRIVATE_IPS") == "true"

func validateCallbackURL(ctx context.Context, raw string) error {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || !u.IsAbs() || u.Host == "" {
		return fmt.Errorf("callback_url must be a valid absolute URL")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("callback_url must use http or https")
	}
	if isPrivateHost(u.Hostname()) {
		return fmt.Errorf("callback_url must not target internal hosts")
	}
	// Fail fast on DNS names that resolve to private/link-local addresses
	// (DNS-rebinding style bypass of the string check above). This applies
	// the same rule the worker enforces at delivery, so a submit that
	// passes here cannot fail validation later. An unresolvable host is
	// accepted: delivery-time validation is authoritative and fails the
	// job with a clear error, so transient DNS blips never bounce valid
	// submissions.
	if _, err := validator.ValidateCallbackURL(
		ctx,
		raw,
		validator.Config{AllowPrivateIPs: allowPrivateCallbackIPs},
	); err != nil {
		if errors.Is(err, validator.ErrPrivateAddress) ||
			errors.Is(err, validator.ErrBlockedHost) {
			return fmt.Errorf("callback_url must not target internal hosts")
		}
		if errors.Is(err, validator.ErrInvalidURL) {
			return fmt.Errorf("callback_url must be a valid absolute URL")
		}
	}
	return nil
}

func isPrivateHost(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	if host == "localhost" || host == "127.0.0.1" || host == "::1" {
		return true
	}
	if strings.HasSuffix(host, ".local") || strings.HasSuffix(host, ".internal") {
		return true
	}
	if strings.HasPrefix(host, "10.") || strings.HasPrefix(host, "192.168.") {
		return true
	}
	if strings.HasPrefix(host, "172.") {
		// 172.16.0.0/12
		parts := strings.Split(host, ".")
		if len(parts) >= 2 {
			if n, err := strconv.Atoi(parts[1]); err == nil && n >= 16 && n <= 31 {
				return true
			}
		}
	}
	if host == "169.254.169.254" || host == "metadata.google.internal" {
		return true
	}
	return false
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
		if errors.Is(err, repository.ErrNotFound) {
			api.WriteError(w, http.StatusNotFound, "NOT_FOUND", "job not found")
			return
		}
		if errors.Is(err, repository.ErrNotCancellable) {
			api.WriteError(w, http.StatusConflict, "CONFLICT", "only dead jobs can be retried")
			return
		}
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
		// The outbox row written by Retry() lets the reconciler finish
		// this handoff; report 500 now and log for visibility.
		h.logger.ErrorContext(
			r.Context(),
			"job retried in DB but Redis enqueue failed; outbox reconciler will retry",
			"job_id", job.ID,
			"error", err,
		)
		api.WriteError(
			w,
			http.StatusInternalServerError,
			"INTERNAL_SERVER_ERROR",
			"failed to enqueue retried job",
		)
		return
	}

	if err := h.jobRepo.DeleteOutboxEntries(r.Context(), job.ID); err != nil {
		h.logger.ErrorContext(r.Context(), "delete outbox entry failed", "job_id", job.ID, "error", err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	_ = json.NewEncoder(w).Encode(job)
}

func (h *Handler) CreateCronJob(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	defer func() { _ = r.Body.Close() }()

	var req CreateCronJobRequest

	if !decodeJSONBody(w, r, &req, "invalid JSON body") {
		return
	}

	if strings.TrimSpace(req.CronExpression) == "" {
		api.WriteError(
			w,
			http.StatusBadRequest,
			"INVALID_REQUEST",
			"cron_expression is required",
		)
		return
	}

	if len(req.JobTemplate) == 0 || string(req.JobTemplate) == "null" {
		api.WriteError(
			w,
			http.StatusBadRequest,
			"INVALID_REQUEST",
			"job_template is required",
		)
		return
	}

	if !json.Valid(req.JobTemplate) {
		api.WriteError(
			w,
			http.StatusBadRequest,
			"INVALID_REQUEST",
			"job_template must be valid JSON",
		)
		return
	}

	// Validate template shape early so tick never aborts on a bad template.
	var tpl struct {
		Type        string          `json:"type"`
		Payload     json.RawMessage `json:"payload"`
		MaxAttempts int             `json:"max_attempts"`
		CallbackURL *string         `json:"callback_url"`
	}
	if err := json.Unmarshal(req.JobTemplate, &tpl); err != nil {
		api.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST", "job_template must be a valid object")
		return
	}
	if strings.TrimSpace(tpl.Type) == "" || len(tpl.Type) > maxTypeLen || len(tpl.Payload) == 0 || tpl.MaxAttempts <= 0 || tpl.MaxAttempts > maxAttemptsLimit {
		api.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST", "job_template must include type (max 128 chars), payload, and max_attempts (1-100)")
		return
	}
	// callback_url is required: instances spawned from this template have
	// nowhere to deliver without it, and the tick path would fail them.
	if tpl.CallbackURL == nil || strings.TrimSpace(*tpl.CallbackURL) == "" {
		api.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST", "job_template.callback_url is required")
		return
	}
	if err := validateCallbackURL(r.Context(), *tpl.CallbackURL); err != nil {
		api.WriteError(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
		return
	}

	now := time.Now().UTC()

	nextRunAt, err := scheduler.NextRunAt(req.CronExpression, now)
	if err != nil {
		api.WriteError(
			w,
			http.StatusBadRequest,
			"INVALID_REQUEST",
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
			"INTERNAL_SERVER_ERROR",
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
			"INVALID_REQUEST",
			"invalid cron job id",
		)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	defer func() { _ = r.Body.Close() }()

	var req UpdateCronJobRequest

	if !decodeJSONBody(w, r, &req, "invalid JSON body") {
		return
	}

	if req.Enabled == nil {
		api.WriteError(
			w,
			http.StatusBadRequest,
			"INVALID_REQUEST",
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
				"NOT_FOUND",
				"cron job not found",
			)
			return
		}

		api.WriteError(
			w,
			http.StatusInternalServerError,
			"INTERNAL_SERVER_ERROR",
			"failed to update cron job",
		)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
