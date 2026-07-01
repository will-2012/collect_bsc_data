package common

import (
	"context"
	"fmt"
	"time"
)

// FindStartBlock returns the first block whose timestamp >= target (unix seconds),
// searching the inclusive range [lo, hi] via lower-bound binary search.
func FindStartBlock(ctx context.Context, c *Client, lo, hi, target int64) (int64, error) {
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

// FindEndBlock returns the last block whose timestamp <= target (unix seconds),
// searching the inclusive range [lo, hi] via upper-bound binary search.
func FindEndBlock(ctx context.Context, c *Client, lo, hi, target int64) (int64, error) {
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

// ResolveRange resolves the inclusive [start,end] block numbers spanning the
// given UTC time range (start inclusive, end inclusive).
func ResolveRange(ctx context.Context, c *Client, start, end time.Time) (startBlock, endBlock int64, err error) {
	latest, err := c.BlockNumber(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("eth_blockNumber: %w", err)
	}
	startBlock, err = FindStartBlock(ctx, c, 0, latest, start.Unix())
	if err != nil {
		return 0, 0, fmt.Errorf("resolve start: %w", err)
	}
	endBlock, err = FindEndBlock(ctx, c, startBlock, latest, end.Unix())
	if err != nil {
		return 0, 0, fmt.Errorf("resolve end: %w", err)
	}
	if endBlock < startBlock {
		return 0, 0, fmt.Errorf("resolved end %d < start %d", endBlock, startBlock)
	}
	return startBlock, endBlock, nil
}

// ChunkBounds returns the inclusive [start,end] block ranges for each chunk.
func ChunkBounds(startBlock, endBlock, chunkSize int64) [][2]int64 {
	var chunks [][2]int64
	for s := startBlock; s <= endBlock; s += chunkSize {
		e := s + chunkSize - 1
		if e > endBlock {
			e = endBlock
		}
		chunks = append(chunks, [2]int64{s, e})
	}
	return chunks
}
