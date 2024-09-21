package nats

import "errors"

var ErrGeneral = errors.New("general error")
var ErrKeyNotFound = errors.New("key not found")
var ErrExpKeyNotFound = errors.New("expiration key not found")
var ErrFieldNotFound = errors.New("field not found")
var ErrOptionNotFound = errors.New("option not found")
var ErrOptionNotSupported = errors.New("option not supported")
