package importmysql

import (
	"context"
	"log"
	"sync"

	"bsc_stats/common"
)

// Importer drives the chunked, concurrent import into MySQL.
type Importer struct {
	cfg      *Config
	client   *common.Client
	db       *DB
	progress *common.Progress
}

func newImporter(cfg *Config, client *common.Client, db *DB, progress *common.Progress) *Importer {
	return &Importer{cfg: cfg, client: client, db: db, progress: progress}
}

// Run imports all chunks in [startBlock, endBlock]. Chunks already recorded
// complete in import_progress are skipped, so a stopped/crashed run resumes.
func (im *Importer) Run(ctx context.Context, startBlock, endBlock int64) error {
	chunks := common.ChunkBounds(startBlock, endBlock, im.cfg.ChunkSize)
	log.Printf("importing blocks %d..%d in %d chunks (chunk_size=%d, concurrency=%d, db_conns=%d)",
		startBlock, endBlock, len(chunks), im.cfg.ChunkSize, im.cfg.Concurrency, im.cfg.DBConns)

	for _, ch := range chunks {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		cs, ce := ch[0], ch[1]
		done, err := im.db.ChunkDone(ctx, cs)
		if err != nil {
			return err
		}
		if done {
			im.progress.AddBlocks(ce - cs + 1)
			log.Printf("chunk %d..%d already done, skipping", cs, ce)
			continue
		}
		if err := im.importChunk(ctx, cs, ce); err != nil {
			return err
		}
	}
	return nil
}

// importChunk imports one chunk with a bounded worker pool. It marks the chunk
// done (and records its failed blocks) atomically only after every block is
// durable, so an interrupted chunk is fully redone on resume; idempotent
// upserts make the redo a no-op.
func (im *Importer) importChunk(ctx context.Context, start, end int64) error {
	var mu sync.Mutex
	var failures []common.FailedBlock

	sem := make(chan struct{}, im.cfg.Concurrency)
	var wg sync.WaitGroup

	for n := start; n <= end; n++ {
		if ctx.Err() != nil {
			break
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(bn int64) {
			defer wg.Done()
			defer func() { <-sem }()

			bd, err := FetchBlock(ctx, im.client, bn)
			if err == nil {
				err = im.db.WriteBlock(ctx, bd)
			}
			if err != nil {
				if ctx.Err() == nil {
					// Buffer the failure; it is committed together with the chunk's
					// done-marker so it survives a crash and a later rescan can find it.
					mu.Lock()
					failures = append(failures, common.FailedBlock{Block: bn, Reason: err.Error()})
					mu.Unlock()
					im.progress.AddFailed(1)
				}
				im.progress.AddBlocks(1)
				return
			}
			im.progress.AddBlocks(1)
			im.progress.AddTx(int64(len(bd.Txs)))
		}(n)
	}
	wg.Wait()

	if ctx.Err() != nil {
		// Do not mark the chunk done; it must be redone on resume.
		return ctx.Err()
	}
	if err := im.db.MarkChunkDone(ctx, start, end, failures); err != nil {
		return err
	}
	log.Printf("chunk %d..%d done (%d failed)", start, end, len(failures))
	return nil
}

// RescanFailed re-imports the blocks recorded in import_failed. Recovered blocks
// are removed from the failed table; blocks that fail again keep their (updated)
// entry for a later retry.
func (im *Importer) RescanFailed(ctx context.Context) error {
	blocks, err := im.db.ReadFailed(ctx)
	if err != nil {
		return err
	}
	if len(blocks) == 0 {
		log.Printf("no failed blocks to re-import")
		return nil
	}
	log.Printf("re-importing %d failed blocks", len(blocks))

	var recovered, stillFailed int
	var mu sync.Mutex
	sem := make(chan struct{}, im.cfg.Concurrency)
	var wg sync.WaitGroup

	for _, n := range blocks {
		if ctx.Err() != nil {
			break
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(bn int64) {
			defer wg.Done()
			defer func() { <-sem }()

			bd, err := FetchBlock(ctx, im.client, bn)
			if err == nil {
				err = im.db.WriteBlock(ctx, bd)
			}
			if err != nil {
				if ctx.Err() == nil {
					_ = im.db.RecordFailed(ctx, bn, err.Error())
					mu.Lock()
					stillFailed++
					mu.Unlock()
				}
				return
			}
			if derr := im.db.DeleteFailed(ctx, bn); derr == nil {
				mu.Lock()
				recovered++
				mu.Unlock()
			}
		}(n)
	}
	wg.Wait()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	log.Printf("re-import complete: recovered %d, still failed %d", recovered, stillFailed)
	return nil
}
