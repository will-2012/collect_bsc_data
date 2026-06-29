package main

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// Merged is the fully aggregated result across all chunk files.
type Merged struct {
	TotalTx          int64
	TypeCounts       [numTxTypes]int64
	ContractCreation int64
	ToCounts         map[string]int64
}

// AddrCount is an address with its aggregated To count.
type AddrCount struct {
	Address    string
	Count      int64
	Percentage float64
}

// mergeChunks reads every chunk_*.txt file in outDir and aggregates them.
func mergeChunks(outDir string) (*Merged, error) {
	pattern := filepath.Join(outDir, "chunk_*.txt")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}
	sort.Strings(files)

	m := &Merged{ToCounts: make(map[string]int64)}
	for _, fp := range files {
		// Skip leftover temp files (rename target only); glob already excludes chunk_tmp_*
		base := filepath.Base(fp)
		if strings.HasPrefix(base, "chunk_tmp_") {
			continue
		}
		cr, err := readChunk(fp)
		if err != nil {
			return nil, err
		}
		m.TotalTx += cr.TotalTx
		m.ContractCreation += cr.ContractCreation
		for i := 0; i < numTxTypes; i++ {
			m.TypeCounts[i] += cr.TypeCounts[i]
		}
		for addr, c := range cr.ToCounts {
			m.ToCounts[addr] += c
		}
	}
	log.Printf("merged %d chunk files: tx=%d, uniqueAddrs=%d", len(files), m.TotalTx, len(m.ToCounts))
	return m, nil
}

// sortedAddrs returns all To addresses sorted by count desc, then address asc.
func (m *Merged) sortedAddrs() []AddrCount {
	out := make([]AddrCount, 0, len(m.ToCounts))
	var total int64
	for _, c := range m.ToCounts {
		total += c
	}
	for addr, c := range m.ToCounts {
		pct := 0.0
		if total > 0 {
			pct = float64(c) / float64(total) * 100
		}
		out = append(out, AddrCount{Address: addr, Count: c, Percentage: pct})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return out[i].Address < out[j].Address
	})
	return out
}

// classifyTop walks addresses by count desc, calling eth_getCode to split into
// contract vs EOA top-N lists. Stops once both lists are full. getCode is a cheap
// method, so candidates are classified in concurrent batches (sized to parallelism)
// while assignment still happens in count-desc order to keep the rankings correct.
func classifyTop(ctx context.Context, c *Client, sorted []AddrCount, n, parallelism int) (contracts, eoa []AddrCount, err error) {
	contracts = make([]AddrCount, 0, n)
	eoa = make([]AddrCount, 0, n)
	if parallelism < 1 {
		parallelism = 1
	}

	checked := 0
	for start := 0; start < len(sorted); start += parallelism {
		if len(contracts) >= n && len(eoa) >= n {
			break
		}
		if ctx.Err() != nil {
			return contracts, eoa, ctx.Err()
		}
		end := start + parallelism
		if end > len(sorted) {
			end = len(sorted)
		}
		batch := sorted[start:end]

		// Fetch this batch's codes concurrently.
		codes := make([]string, len(batch))
		errs := make([]error, len(batch))
		sem := make(chan struct{}, parallelism)
		var wg sync.WaitGroup
		for i, ac := range batch {
			sem <- struct{}{}
			wg.Add(1)
			go func(i int, addr string) {
				defer wg.Done()
				defer func() { <-sem }()
				codes[i], errs[i] = c.GetCode(ctx, addr)
			}(i, ac.Address)
		}
		wg.Wait()
		if ctx.Err() != nil {
			return contracts, eoa, ctx.Err()
		}

		// Assign in count-desc order so the rankings stay correct.
		for i, ac := range batch {
			if len(contracts) >= n && len(eoa) >= n {
				break
			}
			if errs[i] != nil {
				// On persistent getCode failure, skip this address rather than abort.
				log.Printf("getCode(%s) failed, skipping: %v", ac.Address, errs[i])
				continue
			}
			checked++
			isContract := codes[i] != "" && codes[i] != "0x"
			if isContract {
				if len(contracts) < n {
					contracts = append(contracts, ac)
				}
			} else {
				if len(eoa) < n {
					eoa = append(eoa, ac)
				}
			}
		}
	}
	log.Printf("classified top lists: contracts=%d, eoa=%d (getCode calls=%d)", len(contracts), len(eoa), checked)
	return contracts, eoa, nil
}

// outFile joins out dir with a name.
func outFile(outDir, name string) string { return filepath.Join(outDir, name) }

// ensureDir makes the output directory if missing.
func ensureDir(dir string) error {
	return os.MkdirAll(dir, 0755)
}
