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
		residual, err := compensate(ctx, im)
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
	// Reorg safety: never import within Confirmations of chain head. A block
	// imported before finality that later reorgs cannot be healed (upserts are
	// no-ops), so cap the end below head. For a historical backfill this is a
	// no-op; it only bites a run whose end approaches head.
	if head, err := client.BlockNumber(ctx); err != nil {
		log.Printf("warning: could not read chain head for confirmations cap: %v", err)
	} else if safeEnd := head - cfg.Confirmations; endBlock > safeEnd {
		if safeEnd < startBlock {
			log.Fatalf("range %d..%d is within %d confirmations of head %d; nothing safe to import",
				startBlock, endBlock, cfg.Confirmations, head)
		}
		log.Printf("capping end block %d -> %d (head %d - %d confirmations)", endBlock, safeEnd, head, cfg.Confirmations)
		endBlock = safeEnd
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
	residual, err := compensate(ctx, im)
	if err != nil {
		exitOnCompensateErr(ctx, err)
	}
	if residual > 0 {
		log.Printf("WARNING: %d block(s) still failed after compensation; rerun with -rescan_failed later", residual)
		os.Exit(1)
	}

	// Whole-range coverage assertion: within-chunk continuity is guaranteed, but
	// this catches a range that was never fully covered (e.g. a changed
	// start_block/chunk_size across resumes shifting the chunk grid).
	if n, err := db.CountBlocks(ctx, startBlock, endBlock); err != nil {
		log.Printf("warning: coverage check failed: %v", err)
	} else if expected := endBlock - startBlock + 1; n < expected {
		log.Printf("WARNING: coverage %d/%d blocks in %d..%d — %d missing; not a complete import",
			n, expected, startBlock, endBlock, expected-n)
		os.Exit(1)
	}
	log.Printf("import complete")
}

// maxRescanRounds bounds the automatic compensation retries per run.
const maxRescanRounds = 3

// compensate re-imports failed blocks (idempotently) until none remain or the
// round budget is spent. Returns the count of blocks still failing.
func compensate(ctx context.Context, im *Importer) (int, error) {
	var still int
	for r := 1; r <= maxRescanRounds; r++ {
		n, err := im.RescanFailed(ctx)
		if err != nil {
			return 0, err
		}
		still = n
		if still == 0 {
			break
		}
	}
	return still, nil
}

// exitOnCompensateErr exits: 1 (resumable) if interrupted, else fatal.
func exitOnCompensateErr(ctx context.Context, err error) {
	if ctx.Err() != nil {
		log.Printf("compensation interrupted; rerun to resume")
		os.Exit(1)
	}
	log.Fatalf("compensate: %v", err)
}
