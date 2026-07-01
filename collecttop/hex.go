package collecttop

import (
	"fmt"
	"strconv"
	"strings"
)

// parseHexInt parses a "0x"-prefixed hex string into an int64.
func parseHexInt(s string) (int64, error) {
	s = strings.TrimPrefix(s, "0x")
	if s == "" {
		return 0, fmt.Errorf("empty hex")
	}
	return strconv.ParseInt(s, 16, 64)
}

// intToHex formats an int64 as a "0x"-prefixed lowercase hex string.
func intToHex(n int64) string {
	return "0x" + strconv.FormatInt(n, 16)
}

// normTxType maps a tx type hex string to a canonical 0x00..0x04 form.
// Missing/empty type is treated as Legacy (0x0).
func normTxType(t string) int {
	if t == "" {
		return 0
	}
	v, err := parseHexInt(t)
	if err != nil {
		return 0
	}
	return int(v)
}
