package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/go-redis/redis/v8"
)

// Global context to be used for Redis commands
var ctx = context.Background()

func main() {
	// Create a new Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379", // Redis server address (localhost and default Redis port)
		Password: "",               // No password set
		DB:       1,                // Use default DB
	})

	// Test connection with PING
	pong, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("Could not connect to Redis: %v", err)
	}
	fmt.Println("Connected to Redis:", pong)

	_, err = rdb.Set(ctx, "mykey", "0", 0).Result()
	if err != nil {
		log.Fatalf("Could not set key in Redis: %v", err)
	}
	fmt.Println("Key 'mykey' set successfully.")

	wg := sync.WaitGroup{}

	// j := 0

	for i := 0; i < 2000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Create a new Redis client
			rc := redis.NewClient(&redis.Options{
				Addr:     "localhost:6379", // Redis server address (localhost and default Redis port)
				Password: "",               // No password set
				DB:       1,                // Use default DB
			})
			defer rc.Close()

			i, err := rc.Incr(ctx, "mykey").Result()
			if err != nil {
				log.Fatalf("Could not set key in Redis: %v", err)
			}
			fmt.Println("Key 'mykey' INCR successfully.", i)
		}()
	}

	wg.Wait()

	// Get the value for the key
	val, err := rdb.Get(ctx, "mykey").Result()
	if err != nil {
		log.Fatalf("Could not get value from Redis: %v", err)
	}
	fmt.Printf("Value for 'mykey': %s\n", val)

	// Close the Redis connection
	err = rdb.Close()
	if err != nil {
		log.Fatalf("Could not close Redis client: %v", err)
	}
	fmt.Println("Redis client closed.")

}
