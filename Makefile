build:
	go build -o bin/redis2nats cmd/redis2nats/main.go

run:
	go run cmd/redis2nats/main.go

clean:
	rm -rf bin

all: build