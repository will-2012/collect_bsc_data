package common

import (
	"fmt"
	"os"
	"time"
)

// EnvOr returns the env var value if set, otherwise the fallback.
func EnvOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// EnvInt returns the env var parsed as int, or the fallback if unset/invalid.
func EnvInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	var n int
	if _, err := fmt.Sscanf(v, "%d", &n); err != nil {
		return fallback
	}
	return n
}

// ParseDate parses a YYYY-MM-DD date as UTC midnight.
func ParseDate(s string) (time.Time, error) {
	return time.ParseInLocation("2006-01-02", s, time.UTC)
}

// EnsureDir creates dir (and parents) if it does not already exist.
func EnsureDir(dir string) error {
	return os.MkdirAll(dir, 0755)
}
