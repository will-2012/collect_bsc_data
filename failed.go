package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// FailedLog is a concurrency-safe appender for blocks that exhausted retries.
type FailedLog struct {
	mu   sync.Mutex
	f    *os.File
	path string
}

func failedLogPath(outDir string) string {
	return filepath.Join(outDir, "failed_blocks.log")
}

// openFailedLog opens (append/create) the failed-blocks log.
func openFailedLog(outDir string) (*FailedLog, error) {
	path := failedLogPath(outDir)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &FailedLog{f: f, path: path}, nil
}

// Add records a failed block number with a reason. Format: "<block>\t<reason>".
func (fl *FailedLog) Add(block int64, reason string) {
	fl.mu.Lock()
	defer fl.mu.Unlock()
	reason = strings.ReplaceAll(reason, "\n", " ")
	fmt.Fprintf(fl.f, "%d\t%s\n", block, reason)
}

func (fl *FailedLog) Close() error {
	if fl.f == nil {
		return nil
	}
	return fl.f.Close()
}

// readFailedBlocks returns the unique block numbers recorded in the failed log.
func readFailedBlocks(outDir string) ([]int64, error) {
	f, err := os.Open(failedLogPath(outDir))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	seen := make(map[int64]bool)
	var out []int64
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		field := line
		if tab := strings.IndexByte(line, '\t'); tab >= 0 {
			field = line[:tab]
		}
		n, err := strconv.ParseInt(field, 10, 64)
		if err != nil {
			continue
		}
		if !seen[n] {
			seen[n] = true
			out = append(out, n)
		}
	}
	return out, sc.Err()
}

// truncateFailedLog clears the failed log (used after a successful re-scan).
func truncateFailedLog(outDir string) error {
	return os.Truncate(failedLogPath(outDir), 0)
}
