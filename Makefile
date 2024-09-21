build:
	go build -o bin/redis2nats cmd/redis2nats/main.go

run:
	go run cmd/redis2nats/main.go

test:
	TESTCONTAINERS_RYUK_CONNECTION_TIMEOUT=10m TESTCONTAINERS_RYUK_RECONNECTION_TIMEOUT=10m go test -v -coverpkg=./... -coverprofile=cover.out ./tests/...

clean:
	rm -rf bin

all: build