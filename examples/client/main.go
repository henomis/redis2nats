package main

import (
	"context"
	"fmt"
	"log"

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

	// Set a key-value pair in Redis
	err = rdb.Set(ctx, "mykey", "Hello, Redis!", 0).Err() // '0' means no expiration
	if err != nil {
		log.Fatalf("Could not set key in Redis: %v", err)
	}
	fmt.Println("Key 'mykey' set successfully.")

	// Set a key-value pair in Redis
	err = rdb.Set(ctx, "myotherkey", "Hello, Redis again!", 0).Err() // '0' means no expiration
	if err != nil {
		log.Fatalf("Could not set key in Redis: %v", err)
	}
	fmt.Println("Key 'myotherkey' set successfully.")

	// Get the value for the key
	val, err := rdb.Get(ctx, "mykey").Result()
	if err != nil {
		log.Fatalf("Could not get value from Redis: %v", err)
	}
	fmt.Printf("Value for 'mykey': %s\n", val)

	// MSet multiple key-value pairs
	err = rdb.MSet(ctx, "foo", 1, "bar", 2).Err()
	if err != nil {
		log.Fatalf("Could not set multiple keys in Redis: %v", err)
	}
	fmt.Println("Keys 'foo' and 'bar' set successfully.")

	// Incr foo
	foo, err := rdb.Incr(ctx, "foo").Result()
	if err != nil {
		log.Fatalf("Could not increment key in Redis: %v", err)
	}
	fmt.Printf("Key 'foo' incremented: %d\n", foo)

	// Decr bar
	bar, err := rdb.Decr(ctx, "bar").Result()
	if err != nil {
		log.Fatalf("Could not decrement key in Redis: %v", err)
	}
	fmt.Printf("Key 'bar' decremented: %d\n", bar)

	// MGet the value for the keys
	vals, err := rdb.MGet(ctx, "foo", "bar", "notexists").Result()
	if err != nil {
		log.Fatalf("Could not get values from Redis: %v", err)
	}
	fmt.Printf("Values for 'foo' and 'bar': %v\n", vals)

	// call exists
	exists, err := rdb.Exists(ctx, "mykey").Result()
	if err != nil {
		log.Fatalf("Could not check if key exists in Redis: %v", err)
	}
	fmt.Printf("Key 'mykey' exists: %t\n", exists == 1)

	// Delete the key
	deleted, err := rdb.Del(ctx, "mykey").Result()
	if err != nil {
		log.Fatalf("Could not delete key from Redis: %v", err)
	}
	fmt.Printf("Key 'mykey' deleted: %d\n", deleted)

	// Check if the key exists after deletion
	exists, err = rdb.Exists(ctx, "mykey").Result()
	if err != nil {
		log.Fatalf("Could not check if key exists in Redis: %v", err)
	}
	fmt.Printf("Key 'mykey' exists: %t\n", exists == 1)

	err = rdb.Del(ctx, "myhash").Err()
	if err != nil {
		log.Fatalf("Could not delete hash from Redis: %v", err)
	}
	fmt.Println("Hash 'myhash' deleted.")

	//HSet
	err = rdb.HSet(ctx, "myhash", "field1", "value1").Err()
	if err != nil {
		log.Fatalf("Could not set hash field in Redis: %v", err)
	}
	fmt.Println("Hash field 'field1' set successfully.")

	//HGet
	val, err = rdb.HGet(ctx, "myhash", "field1").Result()
	if err != nil {
		log.Fatalf("Could not get hash field from Redis: %v", err)
	}
	fmt.Printf("Hash field 'field1': %s\n", val)

	//HGETALL
	fields, err := rdb.HGetAll(ctx, "myhash2").Result()
	if err != nil {
		log.Fatalf("Could not get all hash fields from Redis: %v", err)
	}
	fmt.Printf("All hash fields: %v\n", fields)

	//HKEYS
	keys, err := rdb.HKeys(ctx, "myhash").Result()
	if err != nil {
		log.Fatalf("Could not get all hash fields from Redis: %v", err)
	}
	fmt.Printf("All hash fields: %v\n", keys)

	//HLEN
	length, err := rdb.HLen(ctx, "myhash").Result()
	if err != nil {
		log.Fatalf("Could not get hash length from Redis: %v", err)
	}
	fmt.Printf("Hash length: %d\n", length)

	//HEXISTS
	hexists, err := rdb.HExists(ctx, "myhash", "field1").Result()
	if err != nil {
		log.Fatalf("Could not check if hash field exists in Redis: %v", err)
	}
	fmt.Printf("Hash field 'field1' exists: %t\n", hexists)

	// HDel
	deleted, err = rdb.HDel(ctx, "myhash", "field1").Result()
	if err != nil {
		log.Fatalf("Could not delete hash field from Redis: %v", err)
	}
	fmt.Printf("Hash field 'field1' deleted: %d\n", deleted)

	err = rdb.Del(ctx, "mylist").Err()
	if err != nil {
		log.Fatalf("Could not delete list from Redis: %v", err)
	}
	fmt.Println("List 'mylist' deleted.")

	// lpush
	err = rdb.LPush(ctx, "mylist", "value1", "value2", "value3").Err()
	if err != nil {
		log.Fatalf("Could not push to list in Redis: %v", err)
	}
	fmt.Println("Values pushed to list successfully.")

	// lpop
	val, err = rdb.LPop(ctx, "mylist").Result()
	if err != nil {
		log.Fatalf("Could not pop from list in Redis: %v", err)
	}
	fmt.Printf("Value popped from list: %s\n", val)

	// lrange
	values, err := rdb.LRange(ctx, "mylist", 0, -1).Result()
	if err != nil {
		log.Fatalf("Could not get range from list in Redis: %v", err)
	}
	fmt.Printf("Values in list: %v\n", values)

	// EXPIRE
	expireRes, err := rdb.Expire(ctx, "myotherkey", 10).Result()
	if err != nil {
		log.Fatalf("Could not set expiration for key in Redis: %v", err)
	}
	fmt.Printf("Key 'myotherkey' expiration set: %t\n", expireRes)

	// TTL
	ttl, err := rdb.TTL(ctx, "myotherkey").Result()
	if err != nil {
		log.Fatalf("Could not get TTL for key in Redis: %v", err)
	}
	fmt.Printf("Key 'myotherkey' TTL: %v\n", ttl)

	// Close the Redis connection
	err = rdb.Close()
	if err != nil {
		log.Fatalf("Could not close Redis client: %v", err)
	}
	fmt.Println("Redis client closed.")
}
