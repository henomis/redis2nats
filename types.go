package redisnats

import "github.com/henomis/redis2nats/nats"

type redisCommand = string

const (
	redisRESPCommandPrefix = "*"
	redisCommandPrefix     = "$"
	redisCommandDelimiter  = '\n'
	redisCommandSeparator  = "\r\n"

	redisCommandNop  redisCommand = ""
	redisCommandPong redisCommand = "+PONG"
	redisCommandOk   redisCommand = "+OK"
	redisCommandNil  redisCommand = "$-1"
)

type redisOption = string

const (
	redisOptionSetXX redisOption = "XX"
	redisOptionSetNX redisOption = "NX"
)

var redisOptionToNatsOption = map[redisOption]nats.Option{
	redisOptionSetXX: nats.OptionSetXX,
	redisOptionSetNX: nats.OptionSetNX,
}
