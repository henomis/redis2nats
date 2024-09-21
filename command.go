package redisnats

import (
	"bufio"
	"context"
	"errors"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/henomis/redis2nats/nats"
)

type redisCommandcmdr func(context.Context, ...string) (string, error)

type Command struct {
	redisCommands map[string]redisCommandcmdr
	storage       *nats.KV
	storagePool   []*nats.KV
	natsTimeout   time.Duration
	log           *slog.Logger
}

func NewCommandExecutor(storagePool []*nats.KV, natsTimeout time.Duration) *Command {
	c := &Command{
		storage:     storagePool[0],
		storagePool: storagePool,
		natsTimeout: natsTimeout,
		log:         slog.Default().With("module", "redis-command"),
	}

	c.redisCommands = map[string]redisCommandcmdr{
		"PING":    c.cmdPing,
		"SET":     c.cmdSet,
		"SETNX":   c.cmdSetNX,
		"GET":     c.cmdGet,
		"MGET":    c.cmdMGet,
		"MSET":    c.cmdMSet,
		"DEL":     c.cmdDel,
		"EXISTS":  c.cmdExists,
		"KEYS":    c.cmdKeys,
		"SELECT":  c.cmdSelect,
		"INCR":    c.cmdIncr,
		"DECR":    c.cmdDecr,
		"HSET":    c.cmdHSet,
		"HGET":    c.cmdHGet,
		"HDEL":    c.cmdHDel,
		"HGETALL": c.cmdHGetAll,
		"HKEYS":   c.cmdHKeys,
		"HLEN":    c.cmdHLen,
		"HEXISTS": c.cmdHExists,
		"LPUSH":   c.cmdLPush,
		"LPOP":    c.cmdLPop,
		"LRANGE":  c.cmdLRange,
	}

	return c
}

func (c *Command) Execute(reader *bufio.Reader) (string, error) {
	input, err := reader.ReadString(redisLF)

	if err != nil {
		return "", err
	}

	input = strings.TrimSpace(input)

	// Check if it's a RESP Array
	if !strings.HasPrefix(input, redisArrayPrefix) {
		return redisNOP, ErrInvalidCommand
	}

	currentStorage := c.storage

	currentStorage.Lock()
	defer currentStorage.Unlock()

	response, err := c.cmdRESPCommand(reader, input)
	if err != nil {
		return redisNOP, err
	}

	return response, nil
}

// cmdRESPCommand processes RESP commands like SET, GET, PING, etc.
func (c *Command) cmdRESPCommand(reader *bufio.Reader, input string) (string, error) {
	// Parse the array length
	arrayLength, err := strconv.Atoi(input[1:])
	if err != nil || arrayLength <= 0 {
		return redisNOP, ErrInvalidCommand
	}

	// Read each part of the array (command and arguments)
	var commandParts []string
	for i := 0; i < arrayLength; i++ {
		// Read the bulk string header (e.g., $3 for "SET")
		line, errRead := reader.ReadString(redisLF)
		if errRead != nil {
			return redisNOP, ErrInvalidBulkData
		}
		line = strings.TrimSpace(line)

		// Make sure it starts with '$'
		if !strings.HasPrefix(line, redisbulkStringPrefix) {
			return redisNOP, ErrInvalidBulkData
		}

		// Read the bulk string length
		length, errAtoi := strconv.Atoi(line[1:])
		if errAtoi != nil || length <= 0 {
			return redisNOP, ErrInvalidBulkData
		}

		// Read the actual bulk string data
		bulkString, errRead := reader.ReadString(redisLF)
		if errRead != nil {
			return redisNOP, ErrInvalidBulkData
		}
		bulkString = strings.TrimSpace(bulkString)

		// Append the bulk string to the command parts
		commandParts = append(commandParts, bulkString)
	}

	c.log.Debug("Received command", "command", commandParts)

	cmd, ok := c.redisCommands[strings.ToUpper(commandParts[0])]
	if !ok {
		return redisNOP, ErrCommandNotSupported
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.natsTimeout)
	defer cancel()

	return cmd(ctx, commandParts[1:]...)
}

// cmdPing responds with a PONG message.
func (c *Command) cmdPing(_ context.Context, _ ...string) (string, error) {
	return redisPong, nil
}

// cmdSet stores the key-value pair using the provided storage.
// supported options: XX
func (c *Command) cmdSet(ctx context.Context, args ...string) (string, error) {
	if len(args) < 2 {
		return redisNOP, ErrWrongNumArgs
	}

	key, value := args[0], args[1]

	options := []nats.Option{}
	for i := 2; i < len(args); i++ {
		option := strings.ToUpper(args[i])
		natsOption, ok := redisOptionToNatsOption[option]
		if !ok {
			return redisNOP, ErrCommandNotSupported
		}

		options = append(options, natsOption)
	}

	found := true
	if len(options) >= 0 {
		exists, errExists := c.storage.Exists(ctx, key)
		if errExists != nil {
			return redisNOP, ErrCmdFailed
		}
		found = exists == 1
	}

	if slices.Index(options, nats.OptionSetXX) != -1 && !found {
		return redisNil, nil
	}

	err := c.storage.Set(ctx, key, value)
	if err != nil {
		return redisNOP, ErrCmdFailed
	}

	return redisOK, nil
}

// cmdSetNX stores the key-value pair using the provided storage only if the key does not exist.
func (c *Command) cmdSetNX(ctx context.Context, args ...string) (string, error) {
	if len(args) != 2 {
		return redisNOP, ErrWrongNumArgs
	}

	key, value := args[0], args[1]

	exists, errExists := c.storage.Exists(ctx, key)
	if errExists != nil {
		return redisNOP, ErrCmdFailed
	}

	if exists == 1 {
		return redisNil, nil
	}

	err := c.storage.Set(ctx, key, value)
	if err != nil {
		return redisNOP, ErrCmdFailed
	}

	return redisOK, nil
}

// cmdMSet stores the key-value pairs using the provided storage.
func (c *Command) cmdMSet(ctx context.Context, args ...string) (string, error) {
	if len(args)%2 != 0 {
		return redisNOP, ErrWrongNumArgs
	}

	err := c.storage.MSet(ctx, args...)
	if err != nil {
		return redisNOP, ErrCmdFailed
	}

	return redisOK, nil
}

// cmdGet retrieves the value for the given key using the provided storage.
func (c *Command) cmdGet(ctx context.Context, args ...string) (string, error) {
	if len(args) != 1 {
		return redisNOP, ErrWrongNumArgs
	}

	key := args[0]
	value, err := c.storage.Get(ctx, key)
	if err != nil && errors.Is(err, nats.ErrKeyNotFound) {
		return redisNotFound, nil
	} else if err != nil {
		return redisNOP, ErrCmdFailed
	}

	return fmtSimpleString(value), nil
}

// cmdMGet retrieves the values for the given keys using the provided storage.
func (c *Command) cmdMGet(ctx context.Context, args ...string) (string, error) {
	if len(args) == 0 {
		return redisNOP, ErrWrongNumArgs
	}

	values, err := c.storage.MGet(ctx, args...)
	if err != nil {
		return redisNOP, ErrCmdFailed
	}

	return fmtArrayOfString(values...), nil
}

// cmdDel removes the key-value pair for the given key using the provided storage.
func (c *Command) cmdDel(ctx context.Context, args ...string) (string, error) {
	deletedKeys, err := c.storage.Del(ctx, args...)
	if err != nil {
		return redisNOP, ErrCmdFailed
	}

	return fmtInt(deletedKeys), nil
}

// cmdExists checks if the given key exists in the storage.
func (c *Command) cmdExists(ctx context.Context, args ...string) (string, error) {
	exists, err := c.storage.Exists(ctx, args...)
	if err != nil {
		return redisNOP, ErrCmdFailed
	}

	return fmtInt(exists), nil
}

// cmdKeys retrieves all keys in the storage.
func (c *Command) cmdKeys(ctx context.Context, args ...string) (string, error) {
	pattern := defaultKeysPattern
	if len(args) > 0 {
		pattern = args[0]
	}

	keys, err := c.storage.Keys(ctx, pattern)
	if err != nil {
		return redisNOP, ErrCmdFailed
	}

	return fmtArrayOfString(keys...), nil
}

// cmdIncr increments the value for the given key.
func (c *Command) cmdIncr(ctx context.Context, args ...string) (string, error) {
	if len(args) != 1 {
		return redisNOP, ErrWrongNumArgs
	}

	key := args[0]
	value, err := c.storage.Incr(ctx, key)
	if err != nil {
		return redisNOP, ErrCmdFailed
	}

	return fmtInt(value), nil
}

// cmdDecr decrements the value for the given key.
func (c *Command) cmdDecr(ctx context.Context, args ...string) (string, error) {
	if len(args) != 1 {
		return redisNOP, ErrWrongNumArgs
	}

	key := args[0]
	value, err := c.storage.Decr(ctx, key)
	if err != nil {
		return redisNOP, ErrCmdFailed
	}

	return fmtInt(value), nil
}

// cmdHSet stores the key-value pair in a hash using the provided storage.
func (c *Command) cmdHSet(ctx context.Context, args ...string) (string, error) {
	if (len(args)-1)%2 != 0 {
		return redisNOP, ErrWrongNumArgs
	}

	key := args[0]
	fieldsValues := args[1:]

	added, err := c.storage.HSet(ctx, key, fieldsValues...)
	if err != nil {
		return redisNOP, ErrCmdFailed
	}

	return fmtInt(added), nil
}

// cmdHGet retrieves the value for the given field in a hash using the provided storage.
func (c *Command) cmdHGet(ctx context.Context, args ...string) (string, error) {
	if len(args) != 2 {
		return redisNOP, ErrWrongNumArgs
	}

	key, field := args[0], args[1]
	value, err := c.storage.HGet(ctx, key, field)
	if err != nil && errors.Is(err, nats.ErrKeyNotFound) {
		// return redisNotFound, ErrCmdFailed
		return redisNotFound, ErrCmdFailed
	} else if err != nil && errors.Is(err, nats.ErrFieldNotFound) {
		// return redisNotFound, ErrCmdFailed
		return redisNotFound, ErrCmdFailed
	} else if err != nil {
		return redisNOP, ErrCmdFailed
	}

	return fmtBulkString(value), nil
}

// cmdHDel removes the field from a hash using the provided storage.
func (c *Command) cmdHDel(ctx context.Context, args ...string) (string, error) {
	if len(args) < 2 {
		return redisNOP, ErrWrongNumArgs
	}

	key := args[0]
	fields := args[1:]

	deleted, err := c.storage.HDel(ctx, key, fields...)
	if err != nil && errors.Is(err, nats.ErrKeyNotFound) {
		deleted = 0
	} else if err != nil {
		return redisNOP, ErrCmdFailed
	}

	return fmtInt(deleted), nil
}

// cmdHGetAll retrieves all fields and values from a hash using the provided storage.
func (c *Command) cmdHGetAll(ctx context.Context, args ...string) (string, error) {
	if len(args) != 1 {
		return redisNOP, ErrWrongNumArgs
	}

	fieldsValues := []string{}
	key := args[0]
	hash, err := c.storage.HGetAll(ctx, key)
	if err != nil && errors.Is(err, nats.ErrKeyNotFound) {
		fieldsValues = []string{}
	} else if err != nil {
		return redisNOP, ErrCmdFailed
	}

	for field, value := range hash {
		fieldsValues = append(fieldsValues, field, value)
	}

	return fmtArrayOfString(fieldsValues...), nil
}

// cmdHKeys retrieves all fields from a hash using the provided storage.
func (c *Command) cmdHKeys(ctx context.Context, args ...string) (string, error) {
	if len(args) != 1 {
		return redisNOP, ErrWrongNumArgs
	}

	key := args[0]
	fields, err := c.storage.HKeys(ctx, key)
	if err != nil && errors.Is(err, nats.ErrKeyNotFound) {
		fields = []string{}
	} else if err != nil {
		fields = []string{}
	}

	return fmtArrayOfString(fields...), nil
}

// cmdHLen retrieves the number of fields in a hash using the provided storage.
func (c *Command) cmdHLen(ctx context.Context, args ...string) (string, error) {
	if len(args) != 1 {
		return redisNOP, ErrWrongNumArgs
	}

	key := args[0]
	length, err := c.storage.HLen(ctx, key)
	if err != nil && errors.Is(err, nats.ErrKeyNotFound) {
		length = 0
	} else if err != nil {
		return redisNOP, ErrCmdFailed
	}

	return fmtInt(length), nil
}

// cmdHExists checks if the field exists in a hash using the provided storage.
func (c *Command) cmdHExists(ctx context.Context, args ...string) (string, error) {
	if len(args) != 2 {
		return redisNOP, ErrWrongNumArgs
	}

	key, field := args[0], args[1]
	exists, err := c.storage.HExists(ctx, key, field)
	if err != nil && errors.Is(err, nats.ErrKeyNotFound) {
		exists = false
	} else if err != nil && errors.Is(err, nats.ErrFieldNotFound) {
		exists = false
	} else if err != nil {
		return redisNOP, ErrCmdFailed
	}

	if !exists {
		return fmtInt(0), nil
	}

	return fmtInt(1), nil
}

// cmdLPush prepends the value to the list stored at the key using the provided storage.
func (c *Command) cmdLPush(ctx context.Context, args ...string) (string, error) {
	if len(args) < 2 {
		return redisNOP, ErrWrongNumArgs
	}

	key := args[0]
	values := args[1:]

	length, err := c.storage.LPush(ctx, key, values...)
	if err != nil {
		return redisNOP, ErrCmdFailed
	}

	return fmtInt(length), nil
}

// cmdLPop removes and returns the first element of the list stored at the key using the provided storage.
func (c *Command) cmdLPop(ctx context.Context, args ...string) (string, error) {
	if len(args) > 2 {
		return redisNOP, ErrWrongNumArgs
	}

	var err error
	key := args[0]
	count := 1
	if len(args) == 2 {
		count, err = strconv.Atoi(args[1])
		if err != nil {
			return redisNOP, ErrCmdFailed
		}
	}

	values, err := c.storage.LPop(ctx, key, count)
	if err != nil && errors.Is(err, nats.ErrKeyNotFound) {
		return redisNotFound, nil
	} else if err != nil {
		return redisNOP, ErrCmdFailed
	}

	if len(args) == 1 {
		return fmtBulkString(values[0]), nil
	}

	return fmtArrayOfString(values...), nil
}

// cmdLRange retrieves the elements of the list stored at the key using the provided storage.
func (c *Command) cmdLRange(ctx context.Context, args ...string) (string, error) {
	if len(args) != 3 {
		return redisNOP, ErrWrongNumArgs
	}

	key := args[0]
	start, err := strconv.Atoi(args[1])
	if err != nil {
		return redisNOP, ErrCmdFailed
	}

	stop, err := strconv.Atoi(args[2])
	if err != nil {
		return redisNOP, ErrCmdFailed
	}

	values, err := c.storage.LRange(ctx, key, start, stop)
	if err != nil && errors.Is(err, nats.ErrKeyNotFound) {
		values = []string{}
	} else if err != nil {
		return redisNOP, ErrCmdFailed
	}

	return fmtArrayOfString(values...), nil
}

// cmdSelect switches the active database to the given ID.
func (c *Command) cmdSelect(_ context.Context, args ...string) (string, error) {
	if len(args) != 1 {
		return redisNOP, ErrWrongNumArgs
	}

	dbID := args[0]

	dbIDAsInt, err := strconv.Atoi(dbID)
	if err != nil || dbIDAsInt < 0 || dbIDAsInt >= len(c.storagePool) {
		return redisNOP, ErrInvalidDB
	}

	c.storage = c.storagePool[dbIDAsInt]

	return redisOK, nil
}
