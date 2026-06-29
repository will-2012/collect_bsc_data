package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// numTxTypes covers tx types 0x00..0x04.
const numTxTypes = 5

// ChunkResult is the aggregated stats for one chunk of blocks.
type ChunkResult struct {
	StartBlock       int64
	EndBlock         int64 // inclusive
	TotalTx          int64
	TypeCounts       [numTxTypes]int64 // index = tx type
	ContractCreation int64             // to == null
	ToCounts         map[string]int64  // lowercased addr -> count

	// Failed buffers blocks that exhausted retries during this chunk. They are
	// flushed to the failed log only when the chunk is persisted, so an
	// interrupted (unpersisted) chunk leaves no stale entries to be double-counted
	// when the chunk is redone on resume. Format: block -> reason.
	Failed []FailedBlock
}

// FailedBlock is a block that failed to fetch, with the reason.
type FailedBlock struct {
	Block  int64
	Reason string
}

func newChunkResult(start, end int64) *ChunkResult {
	return &ChunkResult{StartBlock: start, EndBlock: end, ToCounts: make(map[string]int64)}
}

// addBlock folds one block's transactions into the chunk result.
func (cr *ChunkResult) addBlock(b *Block) {
	for i := range b.Transactions {
		tx := &b.Transactions[i]
		cr.TotalTx++
		t := normTxType(tx.Type)
		if t >= 0 && t < numTxTypes {
			cr.TypeCounts[t]++
		} else {
			cr.TypeCounts[0]++ // unknown/future type folded into legacy bucket for the count total
		}
		if tx.To == nil || *tx.To == "" {
			cr.ContractCreation++
			continue
		}
		cr.ToCounts[strings.ToLower(*tx.To)]++
	}
}

// chunkFileName is the result file for a chunk starting at start.
func chunkFileName(outDir string, start int64) string {
	return filepath.Join(outDir, fmt.Sprintf("chunk_%012d.txt", start))
}

// writeChunkTo writes a chunk result atomically to an explicit path (used by rescan).
func writeChunkTo(outDir, final string, cr *ChunkResult) error {
	tmp, err := os.CreateTemp(outDir, "chunk_tmp_*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	w := bufio.NewWriter(tmp)
	fmt.Fprintf(w, "META\t%d\t%d\t%d\t%d", cr.StartBlock, cr.EndBlock, cr.TotalTx, cr.ContractCreation)
	for _, c := range cr.TypeCounts {
		fmt.Fprintf(w, "\t%d", c)
	}
	fmt.Fprintln(w)
	for addr, cnt := range cr.ToCounts {
		fmt.Fprintf(w, "%s\t%d\n", addr, cnt)
	}
	if err := w.Flush(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	return os.Rename(tmpName, final)
}

// File format (one section after another):
//
//	# start end totalTx contractCreation t0 t1 t2 t3 t4
//	META <start> <end> <totalTx> <contractCreation> <t0> <t1> <t2> <t3> <t4>
//	<addr>\t<count>
//	...
//
// The META line is written first so a truncated file (missing it) is obviously invalid.

// writeChunk writes the result atomically: temp file then rename.
func writeChunk(outDir string, cr *ChunkResult) error {
	final := chunkFileName(outDir, cr.StartBlock)
	tmp, err := os.CreateTemp(outDir, "chunk_tmp_*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	w := bufio.NewWriter(tmp)

	fmt.Fprintf(w, "META\t%d\t%d\t%d\t%d", cr.StartBlock, cr.EndBlock, cr.TotalTx, cr.ContractCreation)
	for _, c := range cr.TypeCounts {
		fmt.Fprintf(w, "\t%d", c)
	}
	fmt.Fprintln(w)

	for addr, cnt := range cr.ToCounts {
		if _, err := fmt.Fprintf(w, "%s\t%d\n", addr, cnt); err != nil {
			tmp.Close()
			os.Remove(tmpName)
			return err
		}
	}
	if err := w.Flush(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	return os.Rename(tmpName, final)
}

// readChunk loads a chunk result file produced by writeChunk.
func readChunk(path string) (*ChunkResult, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	cr := &ChunkResult{ToCounts: make(map[string]int64)}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1024*1024), 16*1024*1024)

	if !sc.Scan() {
		return nil, fmt.Errorf("%s: empty file", path)
	}
	metaFields := strings.Split(sc.Text(), "\t")
	// META start end totalTx contractCreation + numTxTypes counts
	if len(metaFields) != 5+numTxTypes || metaFields[0] != "META" {
		return nil, fmt.Errorf("%s: bad META line", path)
	}
	cr.StartBlock = mustInt(metaFields[1])
	cr.EndBlock = mustInt(metaFields[2])
	cr.TotalTx = mustInt(metaFields[3])
	cr.ContractCreation = mustInt(metaFields[4])
	for i := 0; i < numTxTypes; i++ {
		cr.TypeCounts[i] = mustInt(metaFields[5+i])
	}

	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			continue
		}
		tab := strings.IndexByte(line, '\t')
		if tab < 0 {
			return nil, fmt.Errorf("%s: bad addr line %q", path, line)
		}
		addr := line[:tab]
		cnt, err := strconv.ParseInt(line[tab+1:], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%s: bad count in %q: %w", path, line, err)
		}
		cr.ToCounts[addr] += cnt
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return cr, nil
}

func mustInt(s string) int64 {
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}
