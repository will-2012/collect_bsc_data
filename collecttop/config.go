package collecttop

import (
	"flag"
	"fmt"
	"os"
	"time"
)

// Config holds all runtime configuration, sourced from flags with env fallback.
type Config struct {
	Endpoint    string
	Concurrency int
	StartDate   time.Time // inclusive UTC start
	EndDate     time.Time // inclusive UTC end
	StartBlock  int64     // if >= 0 with EndBlock, overrides date range (skips resolution)
	EndBlock    int64     // if >= 0 with StartBlock, overrides date range
	ChunkSize   int64
	OutDir      string
	RescanFail  bool // re-scan blocks listed in failed_blocks.log instead of normal run
}

// BlockRangeOverride reports whether explicit start/end blocks were given.
func (c *Config) BlockRangeOverride() bool { return c.StartBlock >= 0 && c.EndBlock >= 0 }

// defaultEndpoint is intentionally empty: the JSON-RPC endpoint (which may carry
// an API key) must be supplied via -endpoint or the BSC_ENDPOINT env var.
const defaultEndpoint = ""

// envOr returns the env var value if set, otherwise the fallback.
func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// parseDate parses a YYYY-MM-DD date as UTC midnight.
func parseDate(s string) (time.Time, error) {
	return time.ParseInLocation("2006-01-02", s, time.UTC)
}

// LoadConfig parses the collect-top flags (with env defaults) from args and validates them.
func LoadConfig(args []string) (*Config, error) {
	fs := flag.NewFlagSet("collect-top", flag.ExitOnError)
	var (
		endpoint    = fs.String("endpoint", envOr("BSC_ENDPOINT", defaultEndpoint), "JSON-RPC endpoint")
		concurrency = fs.Int("concurrency", envInt("BSC_CONCURRENCY", 500), "worker concurrency (up to ~2000)")
		startDate   = fs.String("start_date", envOr("BSC_START_DATE", "2025-05-01"), "inclusive UTC start date YYYY-MM-DD")
		endDate     = fs.String("end_date", envOr("BSC_END_DATE", "2026-06-30"), "inclusive UTC end date YYYY-MM-DD")
		chunkSize   = fs.Int64("chunk_size", int64(envInt("BSC_CHUNK_SIZE", 100000)), "blocks per chunk")
		outDir      = fs.String("out_dir", envOr("BSC_OUT_DIR", "./out"), "output directory")
		rescanFail  = fs.Bool("rescan_failed", false, "re-scan blocks listed in failed_blocks.log and exit")
		startBlock  = fs.Int64("start_block", -1, "explicit inclusive start block (overrides date range; requires end_block)")
		endBlock    = fs.Int64("end_block", -1, "explicit inclusive end block (overrides date range; requires start_block)")
	)
	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	start, err := parseDate(*startDate)
	if err != nil {
		return nil, fmt.Errorf("bad start_date %q: %w", *startDate, err)
	}
	// End date is inclusive of the whole day: 23:59:59 UTC.
	endDay, err := parseDate(*endDate)
	if err != nil {
		return nil, fmt.Errorf("bad end_date %q: %w", *endDate, err)
	}
	end := endDay.Add(24*time.Hour - time.Second)

	if !end.After(start) {
		return nil, fmt.Errorf("end_date must be after start_date")
	}
	if *concurrency < 1 {
		return nil, fmt.Errorf("concurrency must be >= 1")
	}
	if *chunkSize < 1 {
		return nil, fmt.Errorf("chunk_size must be >= 1")
	}
	if *endpoint == "" {
		return nil, fmt.Errorf("endpoint required: set -endpoint or BSC_ENDPOINT")
	}
	if (*startBlock >= 0) != (*endBlock >= 0) {
		return nil, fmt.Errorf("start_block and end_block must be set together")
	}
	if *startBlock >= 0 && *endBlock < *startBlock {
		return nil, fmt.Errorf("end_block must be >= start_block")
	}

	return &Config{
		Endpoint:    *endpoint,
		Concurrency: *concurrency,
		StartDate:   start,
		EndDate:     end,
		StartBlock:  *startBlock,
		EndBlock:    *endBlock,
		ChunkSize:   *chunkSize,
		OutDir:      *outDir,
		RescanFail:  *rescanFail,
	}, nil
}

func envInt(key string, fallback int) int {
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
