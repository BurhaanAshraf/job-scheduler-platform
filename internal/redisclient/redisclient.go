package redisclient

import (
	"context"
	"time"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/config"
	"github.com/redis/go-redis/v9"
)

func New(ctx context.Context, cfg config.Config) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         cfg.RedisAddr,
		PoolSize:     10,
		MinIdleConns: 2,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
		PoolTimeout:  3 * time.Second,
	})

	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, err
	}

	return client, nil
}
