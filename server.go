package redisnats

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/henomis/redis2nats/nats"
)

const (
	appName = "Redis2NATS"
)

// Config represents the configuration for the fake Redis server.
type Config struct {
	NATSURL          string
	NATSTimeout      time.Duration
	NATSBucketPrefix string
	RedisAddress     string
	RedisMaxDB       int
}

// RedisServer represents the fake Redis server that uses a storage backend.
type RedisServer struct {
	log    *slog.Logger
	config *Config
}

// NewRedisServer creates a new RedisServer instance with the provided storage.
func NewRedisServer(c *Config) *RedisServer {
	return &RedisServer{
		config: c,
		log:    slog.Default().With("module", "redis-server"),
	}
}

// Start starts the fake Redis server on the given address.
func (s *RedisServer) Start(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.config.RedisAddress)
	if err != nil {
		return err
	}
	s.log.Info(fmt.Sprintf("%s server is running", appName), "address", s.config.RedisAddress)

	// Create a storage pool with the number of databases specified in the configuration.
	storagePool := make([]*nats.KV, s.config.RedisMaxDB)
	for i := 0; i < s.config.RedisMaxDB; i++ {
		bucket := fmt.Sprintf("%s-%d", s.config.NATSBucketPrefix, i)
		storage := nats.New(s.config.NATSURL, bucket, s.config.NATSTimeout)
		err := storage.Connect(ctx)
		if err != nil {
			return err
		}
		storagePool[i] = storage
		defer storage.Close()
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			s.log.Error("Error accepting connection", "error", err)
			continue
		}

		go NewConnection(conn, storagePool).handle()
	}
}
