package main

import (
	"context"
	"fmt"
)

// findStartBlock returns the first block whose timestamp >= target (unix seconds).
// Searches the inclusive range [lo, hi].
func findStartBlock(ctx context.Context, c *Client, lo, hi, target int64) (int64, error) {
	// Standard lower-bound binary search.
	for lo < hi {
		mid := lo + (hi-lo)/2
		ts, err := c.HeaderTimestamp(ctx, mid)
		if err != nil {
			return 0, fmt.Errorf("ts at %d: %w", mid, err)
		}
		if ts < target {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	ts, err := c.HeaderTimestamp(ctx, lo)
	if err != nil {
		return 0, err
	}
	if ts < target {
		return 0, fmt.Errorf("no block with timestamp >= %d", target)
	}
	return lo, nil
}

// findEndBlock returns the last block whose timestamp <= target (unix seconds).
// Searches the inclusive range [lo, hi].
func findEndBlock(ctx context.Context, c *Client, lo, hi, target int64) (int64, error) {
	// Upper-bound: find first block with timestamp > target, then step back one.
	l, h := lo, hi
	for l < h {
		mid := l + (h-l)/2
		ts, err := c.HeaderTimestamp(ctx, mid)
		if err != nil {
			return 0, fmt.Errorf("ts at %d: %w", mid, err)
		}
		if ts <= target {
			l = mid + 1
		} else {
			h = mid
		}
	}
	// l is the first block with ts > target (or hi). The answer is l-1,
	// unless even lo is already > target.
	ts, err := c.HeaderTimestamp(ctx, l)
	if err != nil {
		return 0, err
	}
	if ts <= target {
		return l, nil // whole range is <= target; hi itself qualifies
	}
	if l == lo {
		return 0, fmt.Errorf("no block with timestamp <= %d", target)
	}
	return l - 1, nil
}

// resolveRange resolves the start and end block numbers for the configured date range.
func resolveRange(ctx context.Context, c *Client, cfg *Config) (start, end int64, err error) {
	latest, err := c.BlockNumber(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("eth_blockNumber: %w", err)
	}
	startTarget := cfg.StartDate.Unix()
	endTarget := cfg.EndDate.Unix()

	start, err = findStartBlock(ctx, c, 0, latest, startTarget)
	if err != nil {
		return 0, 0, fmt.Errorf("resolve start: %w", err)
	}
	end, err = findEndBlock(ctx, c, start, latest, endTarget)
	if err != nil {
		return 0, 0, fmt.Errorf("resolve end: %w", err)
	}
	if end < start {
		return 0, 0, fmt.Errorf("resolved end %d < start %d", end, start)
	}
	return start, end, nil
}
