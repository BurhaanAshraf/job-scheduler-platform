package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type APIKey struct {
	ID         uuid.UUID  `json:"id"`
	ClientName string     `json:"client_name"`
	HashedKey  string     `json:"-"`
	CreatedAt  time.Time  `json:"created_at"`
	RevokedAt  *time.Time `json:"revoked_at,omitempty"`
}

func (r *JobRepository) GetAPIKeyByHash(ctx context.Context, hashedKey string) (*APIKey, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	var key APIKey

	err := r.pool.QueryRow(ctx, `SELECT id, client_name, hashed_key, created_at, revoked_at
			 FROM api_keys
			 WHERE hashed_key = $1`, hashedKey).Scan(&key.ID,
		&key.ClientName,
		&key.HashedKey,
		&key.CreatedAt,
		&key.RevokedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("failed to retrieve API key: %w", err)
	}

	return &key, nil
}
