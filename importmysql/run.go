package importmysql

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"bsc_stats/common"
)

// Run executes the import-mysql subcommand: parse args, open MySQL, resolve the
// range, and import blocks+txs. It manages its own signal handling and exits the
// process on fatal errors.
func Run(args []string) {
	cfg, err := LoadConfig(args)
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	// Cancel on SIGINT/SIGTERM so an in-flight chunk stops without being marked
	// done, making restart resumable.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := OpenDB(ctx, cfg.DSN, cfg.DBConns)
	if err != nil {
		log.Fatalf("open mysql: %v", err)
	}
	defer db.Close()
	if err := db.CreateSchema(ctx); err != nil {
		log.Fatalf("schema: %v", err)
	}

	client := common.NewClient(cfg.Endpoint, cfg.Concurrency)

	// Re-scan mode: only re-import previously failed blocks, then exit.
	if cfg.RescanFail {
		im := newImporter(cfg, client, db, common.NewProgress(0))
		residual, err := compensate(ctx, db, im)
		if err != nil {
			exitOnCompensateErr(ctx, err)
		}
		if residual > 0 {
			log.Printf("WARNING: %d block(s) still failing after rescan; rerun later", residual)
			os.Exit(1)
		}
		log.Printf("rescan complete")
		return
	}

	var startBlock, endBlock int64
	if cfg.BlockRangeOverride() {
		startBlock, endBlock = cfg.StartBlock, cfg.EndBlock
		log.Printf("using explicit block range: %d .. %d", startBlock, endBlock)
	} else {
		log.Printf("resolving block range for %s .. %s (UTC)",
			cfg.StartDate.Format(time.RFC3339), cfg.EndDate.Format(time.RFC3339))
		startBlock, endBlock, err = common.ResolveRange(ctx, client, cfg.StartDate, cfg.EndDate)
		if err != nil {
			log.Fatalf("resolve range: %v", err)
		}
	}
	totalBlocks := endBlock - startBlock + 1
	log.Printf("block range resolved: %d .. %d (%d blocks)", startBlock, endBlock, totalBlocks)

	progress := common.NewProgress(totalBlocks)
	go progress.Run(ctx, cfg.Progress)

	im := newImporter(cfg, client, db, progress)
	importErr := im.Run(ctx, startBlock, endBlock)
	progress.Report()

	if importErr != nil {
		if ctx.Err() != nil {
			log.Printf("import interrupted (%v); completed chunks are saved, restart to resume", importErr)
			os.Exit(1)
		}
		log.Fatalf("import failed: %v", importErr)
	}

	// Compensate: automatically re-import any blocks that exhausted retries so a
	// normal run reaches full coverage without a separate manual pass. If some
	// still fail, exit non-zero so a partial import is not mistaken for complete.
	residual, err := compensate(ctx, db, im)
	if err != nil {
		exitOnCompensateErr(ctx, err)
	}
	if residual > 0 {
		log.Printf("WARNING: %d block(s) still failed after compensation; rerun with -rescan_failed later", residual)
		os.Exit(1)
	}
	log.Printf("import complete")
}

// maxRescanRounds bounds the automatic compensation retries per run.
const maxRescanRounds = 3

// compensate re-imports failed blocks (idempotently) until none remain or the
// round budget is spent. Returns the count of blocks still failing.
func compensate(ctx context.Context, db *DB, im *Importer) (int, error) {
	for r := 1; r <= maxRescanRounds; r++ {
		failed, err := db.ReadFailed(ctx)
		if err != nil {
			return 0, err
		}
		if len(failed) == 0 {
			return 0, nil
		}
		log.Printf("compensating %d failed block(s) (round %d/%d)", len(failed), r, maxRescanRounds)
		if err := im.RescanFailed(ctx); err != nil {
			return 0, err
		}
	}
	failed, err := db.ReadFailed(ctx)
	if err != nil {
		return 0, err
	}
	return len(failed), nil
}

// exitOnCompensateErr exits: 1 (resumable) if interrupted, else fatal.
func exitOnCompensateErr(ctx context.Context, err error) {
	if ctx.Err() != nil {
		log.Printf("compensation interrupted; rerun to resume")
		os.Exit(1)
	}
	log.Fatalf("compensate: %v", err)
}
