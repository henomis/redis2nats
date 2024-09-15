package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"strings"
	"time"

	redisnats "github.com/henomis/redis2nats"
	"github.com/spf13/viper"
)

func main() {
	// Logging setup
	log := slog.Default().With("module", "redis-nats")

	// Define command-line flags
	configFile := flag.String("config", "", "Path to the configuration file (optional)")
	showHelp := flag.Bool("help", false, "Show help message")

	// Parse command-line flags
	flag.Parse()

	// If --help is passed, show usage and exit
	if *showHelp {
		flag.Usage()
		os.Exit(0)
	}

	// Initialize Viper
	viper.SetEnvPrefix("REDIS2NATS") // Set prefix to avoid environment variable conflicts
	viper.AutomaticEnv()             // Automatically read environment variables
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// If a configuration file is provided, load it
	if *configFile != "" {
		viper.SetConfigFile(*configFile)
		err := viper.ReadInConfig()
		if err != nil {
			log.Error("Error reading config file", "error", err)
			os.Exit(1)
		}
	}

	// Setting default values in case environment variables are not set
	viper.SetDefault("nats.url", "nats://localhost:4222")
	viper.SetDefault("nats.bucketPrefix", "redisnats")
	viper.SetDefault("nats.timeout", 10*time.Second)
	viper.SetDefault("redis.address", ":6379")
	viper.SetDefault("redis.numDB", 16)

	// Retrieve configuration from environment variables
	natsURL := viper.GetString("nats.url")
	natsBucketPrefix := viper.GetString("nats.bucketPrefix")
	natsTimeout := viper.GetDuration("nats.timeout")
	redisURL := viper.GetString("redis.address")
	redisNumDB := viper.GetInt("redis.numDB")

	// Create and start the fake Redis server using environment variable for Redis URL
	fakeRedis := redisnats.NewRedisServer(
		&redisnats.Config{
			NATSURL:          natsURL,
			NATSTimeout:      natsTimeout,
			NATSBucketPrefix: natsBucketPrefix,
			RedisAddress:     redisURL,
			RedisNumDB:       redisNumDB,
		},
	)

	err := fakeRedis.Start(context.Background())
	if err != nil {
		log.Error("Error starting Redis server", "error", err)
		os.Exit(1)
	}
}
