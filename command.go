package redisnats

import (
	"bufio"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/henomis/redis2nats/nats"
)

var (
	redisString = func(value string) string {
		return fmt.Sprintf("$%d\r\n%s", len(value), value)
	}
	redisInt = func(value int) string {
		return fmt.Sprintf(":%d", value)
	}
	redisStringArray = func(values ...string) string {
		var response strings.Builder
		response.WriteString(redisRESPCommandPrefix)
		response.WriteString(strconv.Itoa(len(values)))
		response.WriteString(redisCommandSeparator)

		for i, value := range values {
			if value == "" {
				response.WriteString(redisCommandNil)

			} else {
				response.WriteString(redisString(value))
			}

			if i < len(values)-1 {
				response.WriteString(redisCommandSeparator)
			}
		}

		return response.String()
	}
)

type redisCommandHandler func(...string) (string, error)

type command struct {
	redisCommands map[string]redisCommandHandler
	storage       *nats.KV
	storagePool   []*nats.KV
	log           *slog.Logger
}

func NewCommandExecutor(storagePool []*nats.KV) *command {
	c := &command{
		storage:     storagePool[0],
		storagePool: storagePool,
		log:         slog.Default().With("module", "redis-command"),
	}

	c.redisCommands = map[string]redisCommandHandler{
		"PING":    c.handlePing,
		"SET":     c.handleSet,
		"GET":     c.handleGet,
		"MGET":    c.handleMGet,
		"MSET":    c.handleMSet,
		"DEL":     c.handleDel,
		"EXISTS":  c.handleExists,
		"KEYS":    c.handleKeys,
		"SELECT":  c.handleSelect,
		"INCR":    c.handleIncr,
		"DECR":    c.handleDecr,
		"HSET":    c.handleHSet,
		"HGET":    c.handleHGet,
		"HDEL":    c.handleHDel,
		"HGETALL": c.handleHGetAll,
		"HKEYS":   c.handleHKeys,
		"HLEN":    c.handleHLen,
		"HEXISTS": c.handleHExists,
		"LPUSH":   c.handleLPush,
		"LPOP":    c.handleLPop,
		"LRANGE":  c.handleLRange,
	}

	return c
}

func (c *command) Execute(reader *bufio.Reader) (string, error) {
	input, err := reader.ReadString(redisCommandDelimiter)

	if err != nil {
		return "", err
	}

	input = strings.TrimSpace(input)

	// Check if it's a RESP Array
	if !strings.HasPrefix(input, redisRESPCommandPrefix) {
		return redisCommandNop, ErrInvalidCommand
	}

	currentStorage := c.storage

	currentStorage.Lock()
	defer currentStorage.Unlock()

	response, err := c.handleRESPCommand(reader, input)
	if err != nil {
		return redisCommandNop, err
	}

	return response, nil
}

// handleRESPCommand processes RESP commands like SET, GET, PING, etc.
func (c *command) handleRESPCommand(reader *bufio.Reader, input string) (string, error) {
	// Parse the array length
	arrayLength, err := strconv.Atoi(input[1:])
	if err != nil || arrayLength <= 0 {
		return redisCommandNop, ErrInvalidCommand
	}

	// Read each part of the array (command and arguments)
	var commandParts []string
	for i := 0; i < arrayLength; i++ {
		// Read the bulk string header (e.g., $3 for "SET")
		line, err := reader.ReadString(redisCommandDelimiter)
		if err != nil {
			return redisCommandNop, ErrInvalidBulkData
		}
		line = strings.TrimSpace(line)

		// Make sure it starts with '$'
		if !strings.HasPrefix(line, redisCommandPrefix) {
			return redisCommandNop, ErrInvalidBulkData
		}

		// Read the bulk string length
		length, err := strconv.Atoi(line[1:])
		if err != nil || length <= 0 {
			return redisCommandNop, ErrInvalidBulkData
		}

		// Read the actual bulk string data
		bulkString, err := reader.ReadString(redisCommandDelimiter)
		if err != nil {
			return redisCommandNop, ErrInvalidBulkData
		}
		bulkString = strings.TrimSpace(bulkString)

		// Append the bulk string to the command parts
		commandParts = append(commandParts, bulkString)
	}

	c.log.Debug("Received command", "command", commandParts)

	cmd, ok := c.redisCommands[strings.ToUpper(commandParts[0])]
	if !ok {
		return redisCommandNop, ErrCommandNotSupported
	}

	return cmd(commandParts[1:]...)
}

// handlePing responds with a PONG message.
func (c *command) handlePing(args ...string) (string, error) {
	return redisCommandPong, nil
}

// handleSet stores the key-value pair using the provided storage.
// supported options: NX, XX
func (c *command) handleSet(args ...string) (string, error) {
	if len(args) < 2 {
		return redisCommandNop, ErrWrongNumArgs
	}

	key, value := args[0], args[1]

	options := []nats.Option{}
	for i := 2; i < len(args); i += 1 {
		option := strings.ToUpper(args[i])
		if natsOption, ok := redisOptionToNatsOption[option]; ok {
			options = append(options, natsOption)
		}
	}

	err := c.storage.Set(key, value, options...)
	if err != nil && errors.Is(err, nats.ErrKeyNotFound) {
		return redisCommandNil, nil
	} else if err != nil {
		return redisCommandNop, ErrCmdFailed
	}

	return redisCommandOk, nil
}

// handleMSet stores the key-value pairs using the provided storage.
func (c *command) handleMSet(args ...string) (string, error) {
	if len(args)%2 != 0 {
		return redisCommandNop, ErrWrongNumArgs
	}

	err := c.storage.MSet(args...)
	if err != nil {
		return redisCommandNop, ErrCmdFailed
	}

	return redisCommandOk, nil
}

// handleGet retrieves the value for the given key using the provided storage.
func (c *command) handleGet(args ...string) (string, error) {
	if len(args) != 1 {
		return redisCommandNop, ErrWrongNumArgs
	}

	key := args[0]
	value, err := c.storage.Get(key)
	if err != nil && errors.Is(err, nats.ErrKeyNotFound) {
		return redisCommandNil, nil
	} else if err != nil {
		return redisCommandNop, ErrCmdFailed
	}

	return redisString(value), nil
}

// handleMGet retrieves the values for the given keys using the provided storage.
func (c *command) handleMGet(args ...string) (string, error) {
	if len(args) == 0 {
		return redisCommandNop, ErrWrongNumArgs
	}

	values, err := c.storage.MGet(args...)
	if err != nil {
		return redisCommandNop, ErrCmdFailed
	}

	return redisStringArray(values...), nil
}

// handleDel removes the key-value pair for the given key using the provided storage.
func (c *command) handleDel(args ...string) (string, error) {
	deletedKeys, err := c.storage.Del(args...)
	if err != nil {
		return redisCommandNop, ErrCmdFailed
	}

	return redisInt(deletedKeys), nil
}

// handleExists checks if the given key exists in the storage.
func (c *command) handleExists(args ...string) (string, error) {
	exists, err := c.storage.Exists(args...)
	if err != nil {
		return redisCommandNop, ErrCmdFailed
	}

	return redisInt(exists), nil
}

// handleKeys retrieves all keys in the storage.
func (c *command) handleKeys(args ...string) (string, error) {
	pattern := "*"
	if len(args) > 0 {
		pattern = args[0]
	}

	keys, err := c.storage.Keys(pattern)
	if err != nil {
		return redisCommandNop, ErrCmdFailed
	}

	if len(keys) == 0 {
		return redisCommandNil, nil
	}

	return redisStringArray(keys...), nil
}

// handleIncr increments the value for the given key.
func (c *command) handleIncr(args ...string) (string, error) {
	if len(args) != 1 {
		return redisCommandNop, ErrWrongNumArgs
	}

	key := args[0]
	value, err := c.storage.Incr(key)
	if err != nil {
		return redisCommandNop, ErrCmdFailed
	}

	return redisInt(value), nil
}

// handleDecr decrements the value for the given key.
func (c *command) handleDecr(args ...string) (string, error) {
	if len(args) != 1 {
		return redisCommandNop, ErrWrongNumArgs
	}

	key := args[0]
	value, err := c.storage.Decr(key)
	if err != nil {
		return redisCommandNop, ErrCmdFailed
	}

	return redisInt(value), nil
}

// handleHSet stores the key-value pair in a hash using the provided storage.
func (c *command) handleHSet(args ...string) (string, error) {
	if (len(args)-1)%2 != 0 {
		return redisCommandNop, ErrWrongNumArgs
	}

	key := args[0]
	fieldsValues := args[1:]

	added, err := c.storage.HSet(key, fieldsValues...)
	if err != nil {
		return redisCommandNop, ErrCmdFailed
	}

	return redisInt(added), nil
}

// handleHGet retrieves the value for the given field in a hash using the provided storage.
func (c *command) handleHGet(args ...string) (string, error) {
	if len(args) != 2 {
		return redisCommandNop, ErrWrongNumArgs
	}

	key, field := args[0], args[1]
	value, err := c.storage.HGet(key, field)
	if err != nil && errors.Is(err, nats.ErrKeyNotFound) {
		return redisCommandNil, nil
	} else if err != nil && errors.Is(err, nats.ErrFieldNotFound) {
		return redisCommandNil, nil
	} else if err != nil {
		return redisCommandNop, ErrCmdFailed
	}

	return redisString(value), nil
}

// handleHDel removes the field from a hash using the provided storage.
func (c *command) handleHDel(args ...string) (string, error) {
	if len(args) < 2 {
		return redisCommandNop, ErrWrongNumArgs
	}

	key := args[0]
	fields := args[1:]

	deleted, err := c.storage.HDel(key, fields...)
	if err != nil {
		return redisCommandNop, ErrCmdFailed
	}

	return redisInt(deleted), nil
}

// handleHGetAll retrieves all fields and values from a hash using the provided storage.
func (c *command) handleHGetAll(args ...string) (string, error) {
	if len(args) != 1 {
		return redisCommandNop, ErrWrongNumArgs
	}

	key := args[0]
	fieldsValues, err := c.storage.HGetAll(key)
	if err != nil && errors.Is(err, nats.ErrKeyNotFound) {
		return redisCommandNil, nil
	} else if err != nil {
		return redisCommandNop, ErrCmdFailed
	}

	return redisStringArray(fieldsValues...), nil
}

// handleHKeys retrieves all fields from a hash using the provided storage.
func (c *command) handleHKeys(args ...string) (string, error) {
	if len(args) != 1 {
		return redisCommandNop, ErrWrongNumArgs
	}

	key := args[0]
	fields, err := c.storage.HKeys(key)
	if err != nil && errors.Is(err, nats.ErrKeyNotFound) {
		return redisCommandNil, nil
	} else if err != nil {
		return redisCommandNop, ErrCmdFailed
	}

	return redisStringArray(fields...), nil
}

// handleHLen retrieves the number of fields in a hash using the provided storage.
func (c *command) handleHLen(args ...string) (string, error) {
	if len(args) != 1 {
		return redisCommandNop, ErrWrongNumArgs
	}

	key := args[0]
	length, err := c.storage.HLen(key)
	if err != nil && errors.Is(err, nats.ErrKeyNotFound) {
		return redisCommandNil, nil
	} else if err != nil {
		return redisCommandNop, ErrCmdFailed
	}

	return redisInt(length), nil
}

// handleHExists checks if the field exists in a hash using the provided storage.
func (c *command) handleHExists(args ...string) (string, error) {
	if len(args) != 2 {
		return redisCommandNop, ErrWrongNumArgs
	}

	key, field := args[0], args[1]
	exists, err := c.storage.HExists(key, field)
	if err != nil && errors.Is(err, nats.ErrKeyNotFound) {
		return redisCommandNil, nil
	} else if err != nil && errors.Is(err, nats.ErrFieldNotFound) {
		return redisCommandNil, nil
	} else if err != nil {
		return redisCommandNop, ErrCmdFailed
	}

	return redisInt(exists), nil
}

// handleLPush prepends the value to the list stored at the key using the provided storage.
func (c *command) handleLPush(args ...string) (string, error) {
	if len(args) < 2 {
		return redisCommandNop, ErrWrongNumArgs
	}

	key := args[0]
	values := args[1:]

	length, err := c.storage.LPush(key, values...)
	if err != nil {
		return redisCommandNop, ErrCmdFailed
	}

	return redisInt(length), nil
}

// handleLPop removes and returns the first element of the list stored at the key using the provided storage.
func (c *command) handleLPop(args ...string) (string, error) {
	if len(args) != 1 {
		return redisCommandNop, ErrWrongNumArgs
	}

	key := args[0]
	value, err := c.storage.LPop(key)
	if err != nil && errors.Is(err, nats.ErrKeyNotFound) {
		return redisCommandNil, nil
	} else if err != nil {
		return redisCommandNop, ErrCmdFailed
	}

	return redisString(value), nil
}

// handleLRange retrieves the elements of the list stored at the key using the provided storage.
func (c *command) handleLRange(args ...string) (string, error) {
	if len(args) != 3 {
		return redisCommandNop, ErrWrongNumArgs
	}

	key := args[0]
	start, err := strconv.Atoi(args[1])
	if err != nil {
		return redisCommandNop, ErrCmdFailed
	}

	stop, err := strconv.Atoi(args[2])
	if err != nil {
		return redisCommandNop, ErrCmdFailed
	}

	values, err := c.storage.LRange(key, start, stop)
	if err != nil {
		return redisCommandNop, ErrCmdFailed
	} else if len(values) == 0 {
		return redisCommandNil, nil
	}

	return redisStringArray(values...), nil
}

// handleSelect switches the active database to the given ID.
func (c *command) handleSelect(args ...string) (string, error) {
	if len(args) != 1 {
		return redisCommandNop, ErrWrongNumArgs
	}

	dbID := args[0]

	dbIDAsInt, err := strconv.Atoi(dbID)
	if err != nil || dbIDAsInt < 0 || dbIDAsInt >= len(c.storagePool) {
		return redisCommandNop, ErrInvalidDB
	}

	c.storage = c.storagePool[dbIDAsInt]

	return redisCommandOk, nil
}
