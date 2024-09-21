package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	nc "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// KV is a simple key-value store backed by NATS JetStream
type KV struct {
	m                sync.Mutex
	url              string
	bucket           string
	expirationBucket string
	conn             *nc.Conn
	jetstream        jetstream.JetStream
	store            jetstream.KeyValue
	expirationStore  jetstream.KeyValue
	persist          bool
	log              *slog.Logger
}

// New creates a new NATS JetStream key-value store
func New(url string, bucket string, persist bool) *KV {
	return &KV{
		url:              url,
		bucket:           bucket,
		expirationBucket: "EXP-" + bucket,
		persist:          persist,
		log:              slog.Default().With("module", "nats-kv"),
	}
}

// Connect connects to the NATS server
func (n *KV) Connect(ctx context.Context) error {
	nc, err := nc.Connect(n.url)
	if err != nil {
		return err
	}

	n.conn = nc
	n.log.Info("Connected to NATS server", "url", n.url)

	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}

	n.log.Info("Connected to NATS JetStream")

	n.jetstream = js

	err = n.storage(ctx)
	if err != nil {
		return err
	}

	return n.expirationStorage(ctx)
}

// Close closes the connection to the NATS server
func (n *KV) Close() {
	if n.conn != nil {
		n.conn.Close()

		n.log.Info("Disconnected from NATS server", "url", n.url)
	}
}

func (n *KV) storage(ctx context.Context) error {
	if !n.persist {
		errDelete := n.jetstream.DeleteKeyValue(ctx, n.bucket)
		if errDelete != nil && !errors.Is(errDelete, jetstream.ErrBucketNotFound) {
			return errDelete
		}
	}

	store, err := n.jetstream.CreateKeyValue(
		ctx,
		jetstream.KeyValueConfig{
			Bucket: n.bucket,
		},
	)
	if err != nil {
		return err
	}

	n.log.Info("Starting NATS JetStream Key-Value store", "bucket", n.bucket)

	n.store = store

	return nil
}

func (n *KV) expirationStorage(ctx context.Context) error {
	if !n.persist {
		errDelete := n.jetstream.DeleteKeyValue(ctx, n.expirationBucket)
		if errDelete != nil && !errors.Is(errDelete, jetstream.ErrBucketNotFound) {
			return errDelete
		}
	}

	store, err := n.jetstream.CreateKeyValue(
		ctx,
		jetstream.KeyValueConfig{
			Bucket: n.expirationBucket,
		},
	)
	if err != nil {
		return err
	}

	n.log.Info("Starting NATS JetStream Key-Value store", "bucket", n.expirationBucket)

	n.expirationStore = store

	go func(ctx context.Context) {
		n.log.Info("Starting expiration check", "bucket", n.expirationBucket)
		for {
			errExpiration := n.checkExpiration(ctx)
			if errExpiration != nil {
				break
			}
			<-time.After(1 * time.Second)
		}
	}(ctx)

	return nil
}

func (n *KV) Lock() {
	n.m.Lock()
}

func (n *KV) Unlock() {
	n.m.Unlock()
}

// Set sets a key-value pair in the key-value store
func (n *KV) Set(ctx context.Context, key, value string) error {
	_, err := n.store.Put(ctx, key, []byte(value))
	return err
}

// MSet sets multiple key-value pairs in the key-value store
func (n *KV) MSet(ctx context.Context, args ...string) error {
	for i := 0; i < len(args); i += 2 {
		err := n.Set(ctx, args[i], args[i+1])
		if err != nil {
			return err
		}
	}

	return nil
}

// Get gets the value for a key in the key-value store
func (n *KV) Get(ctx context.Context, key string) (string, error) {
	entry, err := n.store.Get(ctx, key)
	if err != nil && errors.Is(err, jetstream.ErrKeyNotFound) {
		return "", ErrKeyNotFound
	} else if err != nil {
		return "", err
	}
	return string(entry.Value()), nil
}

// MGet gets the values for multiple keys in the key-value store
func (n *KV) MGet(ctx context.Context, keys ...string) ([]string, error) {
	var natsKeys []string
	for _, key := range keys {
		entry, err := n.store.Get(ctx, key)
		if err != nil && errors.Is(err, jetstream.ErrKeyNotFound) {
			natsKeys = append(natsKeys, "")
			continue
		} else if err != nil {
			return keys, err
		}

		natsKeys = append(natsKeys, string(entry.Value()))
	}

	return natsKeys, nil
}

// Del deletes a key from the key-value store
func (n *KV) Del(ctx context.Context, keys ...string) (int, error) {
	deletedKeys := 0

	for _, key := range keys {
		_, err := n.store.Get(ctx, key)
		if err != nil && errors.Is(err, jetstream.ErrKeyNotFound) {
			continue
		}

		err = n.store.Purge(ctx, key)
		if err != nil {
			return deletedKeys, err
		}
		deletedKeys++
	}

	return deletedKeys, nil
}

// Exists checks if a key exists in the key-value store
func (n *KV) Exists(ctx context.Context, keys ...string) (int, error) {
	exists := 0
	for _, key := range keys {
		_, err := n.store.Get(ctx, key)
		if err == nil {
			exists++
		} else if !errors.Is(err, jetstream.ErrKeyNotFound) {
			return exists, err
		}
	}

	return exists, nil
}

// Keys gets all keys in the key-value store
func (n *KV) Keys(ctx context.Context, pattern string) ([]string, error) {
	keys := make([]string, 0)

	keyListener, err := n.store.ListKeys(ctx)
	if err != nil {
		return keys, err
	}

	for k := range keyListener.Keys() {
		if matchPattern(k, pattern) {
			keys = append(keys, k)
		}
	}

	return keys, nil
}

// Incr increments a key in the key-value store
func (n *KV) Incr(ctx context.Context, key string) (int, error) {
	value, err := n.Get(ctx, key)
	if err != nil && errors.Is(err, ErrKeyNotFound) {
		value = "0"
	} else if err != nil {
		return 0, err
	}

	valueAsInt, err := strconv.Atoi(value)
	if err != nil {
		return 0, err
	}

	valueAsInt++

	err = n.Set(ctx, key, fmt.Sprintf("%d", valueAsInt))
	if err != nil {
		return 0, err
	}

	return valueAsInt, nil
}

// Decr decrements a key in the key-value store
func (n *KV) Decr(ctx context.Context, key string) (int, error) {
	value, err := n.Get(ctx, key)
	if err != nil && errors.Is(err, ErrKeyNotFound) {
		value = "0"
	} else if err != nil {
		return 0, err
	}

	valueAsInt, err := strconv.Atoi(value)
	if err != nil {
		return 0, err
	}

	valueAsInt--

	err = n.Set(ctx, key, fmt.Sprintf("%d", valueAsInt))
	if err != nil {
		return 0, err
	}

	return valueAsInt, nil
}

// HSet sets a field in a hash in the key-value store
func (n *KV) HSet(ctx context.Context, key string, fieldsValues ...string) (int, error) {
	hash := make(map[string]string)

	entry, err := n.store.Get(ctx, key)
	if err != nil && errors.Is(err, jetstream.ErrKeyNotFound) {
		// do nothing
		_ = err
	} else if err != nil {
		return 0, err
	} else {
		err = json.Unmarshal(entry.Value(), &hash)
		if err != nil {
			return 0, err
		}
	}

	added := 0
	for i := 0; i < len(fieldsValues); i += 2 {
		if _, ok := hash[fieldsValues[i]]; !ok {
			added++
		}

		hash[fieldsValues[i]] = fieldsValues[i+1]
	}

	data, err := json.Marshal(hash)
	if err != nil {
		return 0, err
	}

	_, err = n.store.Put(ctx, key, data)
	if err != nil {
		return 0, err
	}

	return added, nil
}

// HGet gets the value for a field in a hash in the key-value store
func (n *KV) HGet(ctx context.Context, key, field string) (string, error) {
	hash := make(map[string]string)

	entry, err := n.store.Get(ctx, key)
	if err != nil && errors.Is(err, jetstream.ErrKeyNotFound) {
		return "", ErrKeyNotFound
	} else if err != nil {
		return "", err
	}

	err = json.Unmarshal(entry.Value(), &hash)
	if err != nil {
		return "", err
	}

	value, ok := hash[field]
	if !ok {
		return "", ErrFieldNotFound
	}

	return value, nil
}

// HDel deletes a field from a hash in the key-value store
func (n *KV) HDel(ctx context.Context, key string, fields ...string) (int, error) {
	hash := make(map[string]string)

	entry, err := n.store.Get(ctx, key)
	if err != nil && errors.Is(err, jetstream.ErrKeyNotFound) {
		return 0, ErrKeyNotFound
	} else if err != nil {
		return 0, err
	}

	err = json.Unmarshal(entry.Value(), &hash)
	if err != nil {
		return 0, err
	}

	deleted := 0
	for _, field := range fields {
		if _, ok := hash[field]; ok {
			deleted++
		}

		delete(hash, field)
	}

	data, err := json.Marshal(hash)
	if err != nil {
		return 0, err
	}

	_, err = n.store.Put(ctx, key, data)
	if err != nil {
		return 0, err
	}

	return deleted, nil
}

// HGetAll gets all fields and values in a hash in the key-value store
func (n *KV) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	hash := make(map[string]string)

	entry, err := n.store.Get(ctx, key)
	if err != nil && errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, ErrKeyNotFound
	} else if err != nil {
		return nil, err
	}

	err = json.Unmarshal(entry.Value(), &hash)
	if err != nil {
		return nil, err
	}

	return hash, nil
}

// HKeys gets all fields in a hash in the key-value store
func (n *KV) HKeys(ctx context.Context, key string) ([]string, error) {
	hash := make(map[string]string)

	entry, err := n.store.Get(ctx, key)
	if err != nil && errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, ErrKeyNotFound
	} else if err != nil {
		return nil, err
	}

	err = json.Unmarshal(entry.Value(), &hash)
	if err != nil {
		return nil, err
	}

	var keys []string
	for key := range hash {
		keys = append(keys, key)
	}

	return keys, nil
}

// HLen gets the number of fields in a hash in the key-value store
func (n *KV) HLen(ctx context.Context, key string) (int, error) {
	hash := make(map[string]string)

	entry, err := n.store.Get(ctx, key)
	if err != nil && errors.Is(err, jetstream.ErrKeyNotFound) {
		return 0, ErrKeyNotFound
	} else if err != nil {
		return 0, err
	}

	err = json.Unmarshal(entry.Value(), &hash)
	if err != nil {
		return 0, err
	}

	return len(hash), nil
}

// HExists checks if a field exists in a hash in the key-value store
func (n *KV) HExists(ctx context.Context, key, field string) (bool, error) {
	hash := make(map[string]string)

	entry, err := n.store.Get(ctx, key)
	if err != nil && errors.Is(err, jetstream.ErrKeyNotFound) {
		return false, ErrKeyNotFound
	} else if err != nil {
		return false, err
	}

	err = json.Unmarshal(entry.Value(), &hash)
	if err != nil {
		return false, err
	}

	_, ok := hash[field]
	if !ok {
		return false, ErrFieldNotFound
	}

	return true, nil
}

// LPush pushes values to a list in the key-value store
func (n *KV) LPush(ctx context.Context, key string, values ...string) (int, error) {
	list := make([]string, 0)

	entry, err := n.store.Get(ctx, key)
	if err != nil && errors.Is(err, jetstream.ErrKeyNotFound) {
		// do nothing
		_ = err
	} else if err != nil {
		return 0, err
	} else {
		err = json.Unmarshal(entry.Value(), &list)
		if err != nil {
			return 0, err
		}
	}

	for _, value := range values {
		list = append([]string{value}, list...)
	}

	data, err := json.Marshal(list)
	if err != nil {
		return 0, err
	}

	_, err = n.store.Put(ctx, key, data)
	if err != nil {
		return 0, err
	}

	return len(list), nil
}

// LPop pops a value from a list in the key-value store
func (n *KV) LPop(ctx context.Context, key string, count int) ([]string, error) {
	list := make([]string, 0)

	entry, err := n.Get(ctx, key)
	if err != nil && errors.Is(err, ErrKeyNotFound) {
		return nil, ErrKeyNotFound
	}

	err = json.Unmarshal([]byte(entry), &list)
	if err != nil {
		return nil, err
	}

	if count > len(list) {
		count = len(list)
	}

	popped := list[:count]
	list = list[count:]

	data, err := json.Marshal(list)
	if err != nil {
		return nil, err
	}

	_, err = n.store.Put(ctx, key, data)
	if err != nil {
		return nil, err
	}

	return popped, nil
}

// LRange gets a range of values from a list in the key-value store
func (n *KV) LRange(ctx context.Context, key string, start, stop int) ([]string, error) {
	list := make([]string, 0)

	entry, err := n.store.Get(ctx, key)
	if err != nil && errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, ErrKeyNotFound
	} else if err != nil {
		return nil, err
	}

	err = json.Unmarshal(entry.Value(), &list)
	if err != nil {
		return nil, err
	}

	listLen := len(list)

	// Handle negative indexing for start
	if start < 0 {
		start = listLen + start
	}
	if start < 0 {
		start = 0 // Clamp to 0 if start is too negative
	}
	if start >= listLen {
		return []string{}, nil // Start is beyond the list size, return empty
	}

	// Handle negative indexing for stop
	if stop < 0 {
		stop = listLen + stop
	}
	if stop >= listLen {
		stop = listLen - 1 // Clamp stop to the end of the slice
	}
	if stop < 0 {
		return []string{}, nil // Stop is before the start, return empty
	}

	// Return the sliced portion (start to stop inclusive)
	return list[start : stop+1], nil
}

func (n *KV) Expire(ctx context.Context, key string, ttl time.Duration) error {
	_, err := n.Get(ctx, key)
	if err != nil && errors.Is(err, ErrKeyNotFound) {
		return ErrKeyNotFound
	} else if err != nil {
		return err
	}

	expirationTime := time.Now().Add(ttl).Unix()

	n.log.Info("Setting expiration", "key", key, "expiration", expirationTime)
	_, err = n.expirationStore.Put(ctx, key, []byte(fmt.Sprintf("%d", expirationTime)))
	return err
}

func (n *KV) TTL(ctx context.Context, key string) (int64, error) {
	exists, err := n.Exists(ctx, key)
	if err != nil {
		return 0, err
	}

	if exists == 0 {
		return 0, ErrKeyNotFound
	}

	entry, err := n.expirationStore.Get(ctx, key)
	if err != nil && errors.Is(err, jetstream.ErrKeyNotFound) {
		return 0, ErrExpKeyNotFound
	} else if err != nil {
		return 0, err
	}

	expirationTime, err := strconv.ParseInt(string(entry.Value()), 10, 64)
	if err != nil {
		return 0, err
	}

	return (expirationTime - time.Now().Unix()), nil
}

// nolint:gocognit
func (n *KV) checkExpiration(ctx context.Context) error {
	watcher, err := n.expirationStore.WatchAll(ctx)
	if err != nil {
		return err
	}
	// nolint:errcheck
	defer watcher.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case event := <-watcher.Updates():
			if event == nil {
				return nil
			}

			key := event.Key()

			expirationTime, errParse := strconv.ParseInt(string(event.Value()), 10, 64)
			if errParse != nil {
				continue
			}

			if time.Now().Unix() >= expirationTime {
				n.Lock()

				n.log.Info("Key expired", "key", key)
				errPurge := n.store.Purge(ctx, key)
				if errPurge != nil {
					n.Unlock()
					continue
				}

				errPurge = n.expirationStore.Purge(ctx, key)
				if errPurge != nil {
					n.Unlock()
					continue
				}

				n.Unlock()
			}
		}
	}
}
