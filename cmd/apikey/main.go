package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/apikey"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/config"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/db"
	"github.com/google/uuid"
)

func main() {
	clientName := flag.String("client-name", "", "name of the API key client")
	flag.Parse()

	if *clientName == "" {
		fmt.Fprintln(os.Stderr, "error: --client-name is required")
		os.Exit(1)
	}

	rawKey, hashedKey, err := apikey.Generate()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error generating API key: %v\n", err)
		os.Exit(1)
	}

	cfg, err := config.Load()
	cfg.DBMaxConns = 20
	if err != nil {
		fmt.Fprintf(os.Stderr, "error loading configuration: %v\n", err)
		os.Exit(1)
	}

	ctx := context.Background()

	pool, err := db.NewPool(ctx, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error connecting to database: %v\n", err)
		os.Exit(1)
	}
	defer pool.Close()

	_, err = pool.Exec(
		ctx,
		`INSERT INTO api_keys (
			id,
			client_name,
			hashed_key,
			created_at
		) VALUES ($1, $2, $3, $4)`,
		uuid.New(),
		*clientName,
		hashedKey,
		time.Now().UTC(),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error storing API key: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("API key: %s\n", rawKey)
}
