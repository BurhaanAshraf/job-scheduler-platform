package repository

import (
	"context"
	"testing"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/apikey"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
)

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("JOB_SCHEDULER_DB_DSN")
	if dsn == "" {
		t.Skip("JOB_SCHEDULER_DB_DSN is required")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	return pool
}

func TestGetAPIKeyByHash(t *testing.T) {
	ctx := context.Background()
	pool := testPool(t)
	repo := &JobRepository{pool: pool}

	raw, hashed, err := apikey.Generate()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if raw == "" || hashed == "" {
		t.Fatal("empty key material")
	}

	var id string
	err = pool.QueryRow(ctx, `INSERT INTO api_keys (id, client_name, hashed_key, created_at) VALUES (gen_random_uuid(), $1, $2, now()) RETURNING id`,
		"coverage-test", hashed).Scan(&id)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	got, err := repo.GetAPIKeyByHash(ctx, hashed)
	if err != nil {
		t.Fatalf("GetAPIKeyByHash: %v", err)
	}
	if got.ClientName != "coverage-test" || got.HashedKey != hashed {
		t.Fatalf("unexpected key: %+v", got)
	}

	if _, err := repo.GetAPIKeyByHash(ctx, "no-such-hash"); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	_, _ = pool.Exec(ctx, `DELETE FROM api_keys WHERE id = $1`, id)
}
