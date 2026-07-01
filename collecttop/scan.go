package collecttop

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Scanner drives the chunked, concurrent block scan.
type Scanner struct {
	cfg      *Config
	client   *Client
	failed   *FailedLog
	progress *Progress
}

func newScanner(cfg *Config, client *Client, failed *FailedLog, progress *Progress) *Scanner {
	return &Scanner{cfg: cfg, client: client, failed: failed, progress: progress}
}

// chunkBounds returns the inclusive [start,end] block ranges for each chunk.
func chunkBounds(startBlock, endBlock, chunkSize int64) [][2]int64 {
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

// Run scans all chunks. Completed chunks (result file present) are skipped.
func (s *Scanner) Run(ctx context.Context, startBlock, endBlock int64) error {
	chunks := chunkBounds(startBlock, endBlock, s.cfg.ChunkSize)
	log.Printf("scanning blocks %d..%d in %d chunks (chunk_size=%d, concurrency=%d)",
		startBlock, endBlock, len(chunks), s.cfg.ChunkSize, s.cfg.Concurrency)

	for _, ch := range chunks {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		cs, ce := ch[0], ch[1]
		fname := chunkFileName(s.cfg.OutDir, cs)
		if _, err := os.Stat(fname); err == nil {
			// Already completed: count its blocks toward progress and skip.
			s.progress.addBlocks(ce - cs + 1)
			log.Printf("chunk %d..%d already done, skipping", cs, ce)
			continue
		}
		if err := s.scanChunk(ctx, cs, ce); err != nil {
			return err
		}
	}
	return nil
}

// scanChunk processes one chunk with a bounded worker pool and writes its result.
func (s *Scanner) scanChunk(ctx context.Context, start, end int64) error {
	cr := newChunkResult(start, end)
	var mu sync.Mutex // guards cr

	sem := make(chan struct{}, s.cfg.Concurrency)
	var wg sync.WaitGroup

	for n := start; n <= end; n++ {
		if ctx.Err() != nil {
			break
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(blockNum int64) {
			defer wg.Done()
			defer func() { <-sem }()

			b, err := s.client.GetBlock(ctx, blockNum)
			if err != nil {
				if ctx.Err() == nil {
					// Buffer the failure on the chunk result; it is flushed to the
					// failed log only after the chunk is persisted. If the chunk is
					// interrupted before that, the failure is dropped so the redone
					// chunk on resume does not leave a stale, double-counted entry.
					mu.Lock()
					cr.Failed = append(cr.Failed, FailedBlock{Block: blockNum, Reason: err.Error()})
					mu.Unlock()
					s.progress.addFailed(1)
				}
				// Block still counts as "processed" for progress so ETA stays sane.
				s.progress.addBlocks(1)
				return
			}
			mu.Lock()
			cr.addBlock(b)
			mu.Unlock()
			s.progress.addBlocks(1)
			s.progress.addTx(int64(len(b.Transactions)))
		}(n)
	}
	wg.Wait()

	if ctx.Err() != nil {
		// Do not persist a partial chunk; it must remain incomplete for resume.
		return ctx.Err()
	}
	if err := writeChunk(s.cfg.OutDir, cr); err != nil {
		return err
	}
	// Chunk is persisted; now it is safe to record its failed blocks.
	for _, fb := range cr.Failed {
		s.failed.Add(fb.Block, fb.Reason)
	}
	log.Printf("chunk %d..%d done: tx=%d, addrs=%d", start, end, cr.TotalTx, len(cr.ToCounts))
	return nil
}

// RescanFailed re-fetches the blocks listed in failed_blocks.log. Successfully
// fetched blocks are folded into a dedicated rescan chunk file; the log is then
// cleared. Blocks that fail again are written back to a fresh log.
func (s *Scanner) RescanFailed(ctx context.Context) error {
	blocks, err := readFailedBlocks(s.cfg.OutDir)
	if err != nil {
		return err
	}
	if len(blocks) == 0 {
		log.Printf("no failed blocks to re-scan")
		return nil
	}
	log.Printf("re-scanning %d failed blocks", len(blocks))

	cr := newChunkResult(blocks[0], blocks[len(blocks)-1])
	var mu sync.Mutex
	var stillFailed []int64
	var failMu sync.Mutex

	sem := make(chan struct{}, s.cfg.Concurrency)
	var wg sync.WaitGroup
	for _, n := range blocks {
		if ctx.Err() != nil {
			break
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(blockNum int64) {
			defer wg.Done()
			defer func() { <-sem }()
			b, err := s.client.GetBlock(ctx, blockNum)
			if err != nil {
				failMu.Lock()
				stillFailed = append(stillFailed, blockNum)
				failMu.Unlock()
				return
			}
			mu.Lock()
			cr.addBlock(b)
			mu.Unlock()
		}(n)
	}
	wg.Wait()
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// Persist recovered blocks into a uniquely-named rescan chunk so merge picks
	// it up without overwriting any real chunk file.
	if cr.TotalTx > 0 || len(cr.ToCounts) > 0 {
		name := filepath.Join(s.cfg.OutDir, fmt.Sprintf("chunk_rescan_%d.txt", time.Now().UnixNano()))
		if err := writeChunkTo(s.cfg.OutDir, name, cr); err != nil {
			return err
		}
	}

	// Rewrite the failed log with only the blocks that failed again.
	if err := truncateFailedLog(s.cfg.OutDir); err != nil {
		return err
	}
	if len(stillFailed) > 0 {
		fl, err := openFailedLog(s.cfg.OutDir)
		if err != nil {
			return err
		}
		for _, n := range stillFailed {
			fl.Add(n, "rescan failed")
		}
		fl.Close()
		log.Printf("re-scan complete: recovered %d, still failed %d", len(blocks)-len(stillFailed), len(stillFailed))
	} else {
		log.Printf("re-scan complete: all %d blocks recovered", len(blocks))
	}
	return nil
}
