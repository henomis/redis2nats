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
	NATSPersist      bool
	RedisAddress     string
	RedisNumDB       int
}

// RedisServer represents the fake Redis server that uses a storage backend.
type RedisServer struct {
	log    *slog.Logger
	config *Config
	stop   chan struct{}
}

// NewRedisServer creates a new RedisServer instance with the provided storage.
func NewRedisServer(c *Config) *RedisServer {
	return &RedisServer{
		config: c,
		log:    slog.Default().With("module", "redis-server"),
		stop:   make(chan struct{}),
	}
}

// Start starts the fake Redis server on the given address.
func (s *RedisServer) Start(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.config.RedisAddress)
	if err != nil {
		return err
	}
	defer ln.Close()

	s.log.Info(fmt.Sprintf("%s server is running", appName), "address", s.config.RedisAddress)

	// Create a storage pool with the number of databases specified in the configuration.
	storagePool := make([]*nats.KV, s.config.RedisNumDB)
	for i := 0; i < s.config.RedisNumDB; i++ {
		bucket := fmt.Sprintf("%s-%d", s.config.NATSBucketPrefix, i)
		storage := nats.New(s.config.NATSURL, bucket, s.config.NATSPersist)
		errConnect := storage.Connect(ctx)
		if errConnect != nil {
			return errConnect
		}
		storagePool[i] = storage
		defer storage.Close()
	}

	for {
		conn, errAccept := ln.Accept()
		if errAccept != nil {
			s.log.Error("Error accepting connection", "error", err)
			continue
		}

		select {
		case <-s.stop:
			return nil
		case <-ctx.Done():
			return nil
		default:
		}

		// nolint:contextcheck
		go NewConnection(conn, storagePool, s.config.NATSTimeout).handle()
	}
}

func (s *RedisServer) Stop() {
	close(s.stop)
	// open a connection to unlock the listener
	ln, err := net.Dial("tcp", s.config.RedisAddress)
	if err != nil {
		s.log.Error("Error unlocking listener", "error", err)
	}
	defer ln.Close()
}
