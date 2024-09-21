package redisnats

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/henomis/redis2nats/nats"
)

type redisCommand = string

const (
	redisCRLF                          = "\r\n"
	redisbulkStringPrefix              = "$"
	redisLF                            = '\n'
	redisArrayPrefix                   = "*"
	redisNOP              redisCommand = ""
	redisNotFound         redisCommand = "_\r\n"
	defaultKeysPattern    redisCommand = "*"

	// redisMapsPrefix       = "%"

	// redisCommandNil  redisCommand = "-1"
)

var (
	redisPong = fmtSimpleString("PONG")
	redisOK   = fmtSimpleString("OK")
	redisNil  = fmtNullBulkString()
)

type redisOption = string

const (
	redisOptionSetXX redisOption = "XX"
	redisOptionSetNX redisOption = "NX"
)

var redisOptionToNatsOption = map[redisOption]nats.Option{
	redisOptionSetXX: nats.OptionSetXX,
	// redisOptionSetNX: nats.OptionSetNX,
}

func fmtSimpleString(value string) string {
	return fmt.Sprintf("+%s%s", value, redisCRLF)
}

func fmtSimpleError(value string) string {
	return fmt.Sprintf("-ERR %s%s", value, redisCRLF)
}

func fmtInt(value int) string {
	return fmt.Sprintf(":%d%s", value, redisCRLF)
}

func fmtBulkString(value string) string {
	return fmt.Sprintf("%s%d%s%s%s", redisbulkStringPrefix, len(value), redisCRLF, value, redisCRLF)
}

func fmtNullBulkString() string {
	return fmt.Sprintf("%s%d%s", redisbulkStringPrefix, -1, redisCRLF)
}

func fmtArrayOfString(values ...string) string {
	var response strings.Builder
	response.WriteString(redisArrayPrefix)
	response.WriteString(strconv.Itoa(len(values)))
	response.WriteString(redisCRLF)

	for _, value := range values {
		if value == "" {
			response.WriteString(fmtNullBulkString())
		} else {
			response.WriteString(fmtBulkString(value))
		}
	}

	return response.String()
}
