package collecttop

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const topN = 100

// Run executes the collect-top subcommand: parse args, scan the range, and
// write outputs. It manages its own signal handling and exits the process on
// fatal errors.
func Run(args []string) {
	cfg, err := LoadConfig(args)
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	if err := ensureDir(cfg.OutDir); err != nil {
		log.Fatalf("create out_dir: %v", err)
	}

	// Cancel on SIGINT/SIGTERM so an in-flight chunk stops cleanly (and is not
	// persisted), making restart resumable.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	client := NewClient(cfg.Endpoint, cfg.Concurrency)

	failed, err := openFailedLog(cfg.OutDir)
	if err != nil {
		log.Fatalf("open failed log: %v", err)
	}
	defer failed.Close()

	// Re-scan mode: only re-fetch previously failed blocks, then exit.
	if cfg.RescanFail {
		sc := newScanner(cfg, client, failed, newProgress(0))
		if err := sc.RescanFailed(ctx); err != nil {
			log.Fatalf("rescan: %v", err)
		}
		return
	}

	var startBlock, endBlock int64
	if cfg.BlockRangeOverride() {
		startBlock, endBlock = cfg.StartBlock, cfg.EndBlock
		log.Printf("using explicit block range: %d .. %d", startBlock, endBlock)
	} else {
		log.Printf("resolving block range for %s .. %s (UTC)",
			cfg.StartDate.Format(time.RFC3339), cfg.EndDate.Format(time.RFC3339))
		startBlock, endBlock, err = resolveRange(ctx, client, cfg)
		if err != nil {
			log.Fatalf("resolve range: %v", err)
		}
	}
	totalBlocks := endBlock - startBlock + 1
	log.Printf("block range resolved: %d .. %d (%d blocks)", startBlock, endBlock, totalBlocks)

	progress := newProgress(totalBlocks)
	go progress.run(ctx, 10*time.Minute)

	sc := newScanner(cfg, client, failed, progress)
	scanErr := sc.Run(ctx, startBlock, endBlock)

	// Always print a final progress summary.
	progress.report()

	if scanErr != nil {
		if ctx.Err() != nil {
			log.Printf("scan interrupted (%v); completed chunks are saved, restart to resume", scanErr)
			os.Exit(1)
		}
		log.Fatalf("scan failed: %v", scanErr)
	}

	log.Printf("scan complete; merging chunks")
	merged, err := mergeChunks(cfg.OutDir)
	if err != nil {
		log.Fatalf("merge: %v", err)
	}

	// Warn if any blocks exhausted retries: the report omits their txs.
	failedBlocks, err := readFailedBlocks(cfg.OutDir)
	if err != nil {
		log.Printf("warning: could not read failed_blocks.log: %v", err)
	}
	if len(failedBlocks) > 0 {
		log.Printf("WARNING: %d block(s) failed all retries and are NOT in the report; rerun with -rescan_failed then regenerate", len(failedBlocks))
	}

	sorted := merged.sortedAddrs()
	contracts, eoa, err := classifyTop(ctx, client, sorted, topN, cfg.Concurrency)
	if err != nil {
		log.Fatalf("classify: %v", err)
	}

	if err := writeOutputs(cfg, merged, startBlock, endBlock, int64(len(failedBlocks)), contracts, eoa); err != nil {
		log.Fatalf("write outputs: %v", err)
	}

	log.Printf("done. outputs written to %s (summary.json, top100_contracts.csv, top100_eoa.csv, report.md)", cfg.OutDir)
}
