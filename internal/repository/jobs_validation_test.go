package repository

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestJobRepository_CreateValidation(t *testing.T) {
	ctx := context.Background()
	pool := testDBPool(t)
	defer pool.Close()
	repo := NewJobRepository(pool)
	cb := "https://example.com/callback"

	if _, err := repo.Create(ctx, CreateJobInput{
		Type:           "  ",
		Payload:        json.RawMessage(`{"a":1}`),
		RunAt:          time.Now().UTC(),
		MaxAttempts:    3,
		IdempotencyKey: uuid.NewString(),
		CallbackURL:    &cb,
	}); err == nil {
		t.Fatal("empty type: want validation error, got nil")
	}

	if _, err := repo.Create(ctx, CreateJobInput{
		Type:           "email",
		Payload:        json.RawMessage(`{"a":1}`),
		RunAt:          time.Now().UTC(),
		MaxAttempts:    3,
		IdempotencyKey: uuid.NewString(),
		CallbackURL:    nil,
	}); err != nil {
		t.Fatalf("nil callback_url should be allowed at repo layer, got: %v", err)
	}

	bad := "not-a-url"
	if _, err := repo.Create(ctx, CreateJobInput{
		Type:           "email",
		Payload:        json.RawMessage(`{"a":1}`),
		RunAt:          time.Now().UTC(),
		MaxAttempts:    3,
		IdempotencyKey: uuid.NewString(),
		CallbackURL:    &bad,
	}); err == nil {
		t.Fatal("malformed callback_url: want validation error, got nil")
	}
}
