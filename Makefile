.PHONY: build test lint migrate-up migrate-down vet

build:
	go build ./...

test:
	go test ./...

vet:
	go vet ./...

lint:
	golangci-lint run

migrate-up:
	migrate -path=./migrations -database="$(JOB_SCHEDULER_DB_DSN)" up
	
migrate-down:
	migrate -path=./migrations -database="$(JOB_SCHEDULER_DB_DSN)" down 1

	