package db

import (
	"context"
	"fmt"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/config"
	"github.com/jackc/pgx/v5/pgxpool"
)

func NewPool(ctx context.Context, cfg config.Config) (*pgxpool.Pool, error) {

	poolConfig, err := pgxpool.ParseConfig(cfg.DBDSN)
	if err != nil {
		return nil, fmt.Errorf("failed to parse database: %w", err)
	}
	poolConfig.MaxConns = int32(cfg.DBMaxConns)

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to establish database pool: %w", err)
	}

	err = Ping(pool, ctx)
	if err != nil {
		return nil, err
	}

	return pool, nil
}

func Ping(pool *pgxpool.Pool, ctx context.Context) error {

	err := pool.Ping(ctx)
	if err != nil {
		pool.Close()
		return fmt.Errorf("failed to reach database during ping: %w", err)
	}
	return nil
}
