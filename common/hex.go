package common

import (
	"fmt"
	"strconv"
	"strings"
)

// ParseHexInt parses a "0x"-prefixed hex string into an int64.
func ParseHexInt(s string) (int64, error) {
	s = strings.TrimPrefix(s, "0x")
	if s == "" {
		return 0, fmt.Errorf("empty hex")
	}
	return strconv.ParseInt(s, 16, 64)
}

// IntToHex formats an int64 as a "0x"-prefixed lowercase hex string.
func IntToHex(n int64) string {
	return "0x" + strconv.FormatInt(n, 16)
}
