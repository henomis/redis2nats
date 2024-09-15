package redisnats

import (
	"errors"
)

var ErrInvalidCommand = errors.New("invalid command")
var ErrInvalidDB = errors.New("invalid database ID")
var ErrWrongNumArgs = errors.New("wrong number of arguments")
var ErrCmdFailed = errors.New("failed to set value")
var ErrInvalidBulkData = errors.New("invalid bulk data")
var ErrCommandNotSupported = errors.New("command not supported")
