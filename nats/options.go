package nats

import "slices"

type Option = string

const (
	OptionSetXX Option = "XX"
	OptionSetNX Option = "NX"
)

func isOptionEnabled(option Option, options []Option) bool {
	return slices.Index(options, option) != -1
}
