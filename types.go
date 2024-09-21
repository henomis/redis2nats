package redisnats

import (
	"fmt"
	"strconv"
	"strings"
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
)

type Option = string

const (
	optionSetXX Option = "XX"
	optionSetNX Option = "NX"
	optionSetEX Option = "EX"
)

var (
	redisPong = fmtSimpleString("PONG")
	redisOK   = fmtSimpleString("OK")
	redisNil  = fmtNullBulkString()
)

func fmtSimpleString(value string) string {
	return fmt.Sprintf("+%s%s", value, redisCRLF)
}

func fmtSimpleError(value string) string {
	return fmt.Sprintf("-ERR %s%s", value, redisCRLF)
}

func fmtInt(value int) string {
	return fmt.Sprintf(":%d%s", value, redisCRLF)
}

func fmtInt64(value int64) string {
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
