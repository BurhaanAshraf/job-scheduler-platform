.PHONY: build test clean lint migrate-up migrate-down vet run-api run-scheduler run-worker

build:
	go build ./...

# -p 1 avoids DB/Redis contention; -count=1 is the standard no-cache run.
test:
	go test ./... -p 1 -count=1

clean:
	go clean -testcache

vet:
	go vet ./...

lint:
	golangci-lint run

# Requires github.com/golang-migrate/migrate installed, and
# JOB_SCHEDULER_DB_DSN exported (see .env.example).
migrate-up:
	migrate -path=./migrations -database="$(JOB_SCHEDULER_DB_DSN)" up

migrate-down:
	migrate -path=./migrations -database="$(JOB_SCHEDULER_DB_DSN)" down 1

# Local dev entrypoints (require env from .env / environment).
run-api:
	go run ./cmd/api

run-scheduler:
	go run ./cmd/scheduler

run-worker:
	go run ./cmd/worker
