package importmysql

import (
	"flag"
	"fmt"
	"time"

	"bsc_stats/common"
)

// Config holds runtime configuration for the import-mysql subcommand.
type Config struct {
	Endpoint      string
	DSN           string // go-sql-driver MySQL DSN
	Concurrency   int    // RPC fetch concurrency
	DBConns       int    // max concurrent MySQL writers (kept well below Concurrency)
	StartDate     time.Time
	EndDate       time.Time
	StartBlock    int64 // >=0 with EndBlock overrides the date range
	EndBlock      int64
	ChunkSize     int64
	RescanFail    bool          // re-import blocks recorded as failed, then exit
	Progress      time.Duration // interval between progress log lines
	Confirmations int64         // don't import blocks within this many of chain head (reorg safety)
}

// BlockRangeOverride reports whether explicit start/end blocks were given.
func (c *Config) BlockRangeOverride() bool { return c.StartBlock >= 0 && c.EndBlock >= 0 }

// LoadConfig parses the import-mysql flags (with env defaults) from args.
func LoadConfig(args []string) (*Config, error) {
	fs := flag.NewFlagSet("import-mysql", flag.ExitOnError)
	var (
		endpoint    = fs.String("endpoint", common.EnvOr("BSC_ENDPOINT", ""), "JSON-RPC endpoint")
		dsn         = fs.String("mysql_dsn", common.EnvOr("BSC_MYSQL_DSN", ""), "MySQL DSN, e.g. user:pass@tcp(host:3306)/bsc")
		concurrency = fs.Int("concurrency", common.EnvInt("BSC_CONCURRENCY", 100), "RPC fetch concurrency (simultaneous requests to the endpoint)")
		dbConns     = fs.Int("db_conns", common.EnvInt("BSC_DB_CONNS", 16), "max concurrent MySQL writers (keep well below concurrency)")
		chunkSize   = fs.Int64("chunk_size", int64(common.EnvInt("BSC_CHUNK_SIZE", 10000)), "blocks per resumable chunk")
		progress    = fs.Duration("progress_interval", time.Minute, "interval between progress log lines")
		confirms    = fs.Int64("confirmations", 15, "skip blocks within this many of chain head (reorg safety)")
		startDate   = fs.String("start_date", common.EnvOr("BSC_START_DATE", "2025-05-01"), "inclusive UTC start date YYYY-MM-DD")
		endDate     = fs.String("end_date", common.EnvOr("BSC_END_DATE", "2026-06-30"), "inclusive UTC end date YYYY-MM-DD")
		startBlock  = fs.Int64("start_block", -1, "explicit inclusive start block (overrides dates; requires end_block)")
		endBlock    = fs.Int64("end_block", -1, "explicit inclusive end block (overrides dates; requires start_block)")
		rescanFail  = fs.Bool("rescan_failed", false, "re-import blocks recorded as failed, then exit")
	)
	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	start, err := common.ParseDate(*startDate)
	if err != nil {
		return nil, fmt.Errorf("bad start_date %q: %w", *startDate, err)
	}
	endDay, err := common.ParseDate(*endDate)
	if err != nil {
		return nil, fmt.Errorf("bad end_date %q: %w", *endDate, err)
	}
	end := endDay.Add(24*time.Hour - time.Second) // end date inclusive of the whole day

	if !end.After(start) {
		return nil, fmt.Errorf("end_date must be after start_date")
	}
	if *endpoint == "" {
		return nil, fmt.Errorf("endpoint required: set -endpoint or BSC_ENDPOINT")
	}
	if *dsn == "" {
		return nil, fmt.Errorf("mysql_dsn required: set -mysql_dsn or BSC_MYSQL_DSN")
	}
	if *concurrency < 1 {
		return nil, fmt.Errorf("concurrency must be >= 1")
	}
	if *dbConns < 1 {
		return nil, fmt.Errorf("db_conns must be >= 1")
	}
	if *chunkSize < 1 {
		return nil, fmt.Errorf("chunk_size must be >= 1")
	}
	if *progress < time.Second {
		return nil, fmt.Errorf("progress_interval must be >= 1s")
	}
	if *confirms < 0 {
		return nil, fmt.Errorf("confirmations must be >= 0")
	}
	if (*startBlock >= 0) != (*endBlock >= 0) {
		return nil, fmt.Errorf("start_block and end_block must be set together")
	}
	if *startBlock >= 0 && *endBlock < *startBlock {
		return nil, fmt.Errorf("end_block must be >= start_block")
	}

	return &Config{
		Endpoint:      *endpoint,
		DSN:           *dsn,
		Concurrency:   *concurrency,
		DBConns:       *dbConns,
		StartDate:     start,
		EndDate:       end,
		StartBlock:    *startBlock,
		EndBlock:      *endBlock,
		ChunkSize:     *chunkSize,
		RescanFail:    *rescanFail,
		Progress:      *progress,
		Confirmations: *confirms,
	}, nil
}
