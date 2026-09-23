.PHONY: build test clean lint migrate-up migrate-down vet

build:
	go build ./...

test:
	go test ./... -p 1 -count=3

clean:
	go clean -testcache

vet:

	go vet ./...

lint:
	golangci-lint run

migrate-up:
	migrate -path=./migrations -database="$(JOB_SCHEDULER_DB_DSN)" up

migrate-down:
	migrate -path=./migrations -database="$(JOB_SCHEDULER_DB_DSN)" down 1
