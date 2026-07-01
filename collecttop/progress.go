package collecttop

import (
	"context"
	"log"
	"sync/atomic"
	"time"
)

// Progress tracks cumulative work for periodic reporting.
type Progress struct {
	totalBlocks int64
	doneBlocks  int64 // atomic
	totalTx     int64 // atomic
	failed      int64 // atomic
	start       time.Time
}

func newProgress(totalBlocks int64) *Progress {
	return &Progress{totalBlocks: totalBlocks, start: time.Now()}
}

func (p *Progress) addBlocks(n int64) { atomic.AddInt64(&p.doneBlocks, n) }
func (p *Progress) addTx(n int64)     { atomic.AddInt64(&p.totalTx, n) }
func (p *Progress) addFailed(n int64) { atomic.AddInt64(&p.failed, n) }

// report logs a single progress line.
func (p *Progress) report() {
	done := atomic.LoadInt64(&p.doneBlocks)
	tx := atomic.LoadInt64(&p.totalTx)
	failed := atomic.LoadInt64(&p.failed)
	elapsed := time.Since(p.start)

	pct := 0.0
	if p.totalBlocks > 0 {
		pct = float64(done) / float64(p.totalBlocks) * 100
	}
	eta := "n/a"
	if done > 0 && done < p.totalBlocks {
		perBlock := elapsed / time.Duration(done)
		remaining := time.Duration(p.totalBlocks-done) * perBlock
		eta = remaining.Round(time.Second).String()
	} else if done >= p.totalBlocks {
		eta = "0s"
	}

	log.Printf("progress: %d/%d blocks (%.2f%%), tx=%d, failed=%d, elapsed=%s, eta=%s",
		done, p.totalBlocks, pct, tx, failed, elapsed.Round(time.Second), eta)
}

// run starts a background reporter that logs every interval until ctx is done.
func (p *Progress) run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.report()
		}
	}
}
