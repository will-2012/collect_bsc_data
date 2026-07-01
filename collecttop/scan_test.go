package collecttop

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// newTestScanner wires a Scanner against the mock with a small chunk size.
func newTestScanner(t *testing.T, m *mockServer, dir string, chunkSize int64) (*Scanner, *FailedLog) {
	t.Helper()
	cfg := &Config{
		Endpoint:    m.URL(),
		Concurrency: 4,
		ChunkSize:   chunkSize,
		OutDir:      dir,
	}
	if err := ensureDir(dir); err != nil {
		t.Fatal(err)
	}
	fl, err := openFailedLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { fl.Close() })
	prog := newProgress(0)
	return newScanner(cfg, m.client(), fl, prog), fl
}

// --- resume: completed chunk skipped; temp/half-written file not treated complete ---

func TestResumeSkipsCompletedChunk(t *testing.T) {
	m := newMockServer(t)
	to := "0xdead"
	for n := int64(0); n < 20; n++ {
		m.addBlock(mockBlock{number: n, timestamp: 1000 + n, txs: []Transaction{{Type: "0x0", To: &to}}})
	}
	dir := t.TempDir()
	sc, _ := newTestScanner(t, m, dir, 10)

	// Pre-create the result file for the first chunk (0..9) with a sentinel value
	// that differs from a fresh scan, so we can detect whether it was overwritten.
	pre := newChunkResult(0, 9)
	pre.TotalTx = 999
	pre.ToCounts["0xsentinel"] = 7
	if err := writeChunk(dir, pre); err != nil {
		t.Fatal(err)
	}

	if err := sc.Run(context.Background(), 0, 19); err != nil {
		t.Fatal(err)
	}

	// First chunk file must be untouched (sentinel preserved => skipped, not rescanned).
	got, err := readChunk(chunkFileName(dir, 0))
	if err != nil {
		t.Fatal(err)
	}
	if got.TotalTx != 999 || got.ToCounts["0xsentinel"] != 7 {
		t.Errorf("completed chunk was overwritten: %+v", got)
	}

	// Second chunk (10..19) must have been scanned for real: 10 blocks * 1 tx.
	got2, err := readChunk(chunkFileName(dir, 10))
	if err != nil {
		t.Fatal(err)
	}
	if got2.TotalTx != 10 {
		t.Errorf("second chunk TotalTx=%d want 10", got2.TotalTx)
	}
	if got2.ToCounts["0xdead"] != 10 {
		t.Errorf("second chunk to-count=%d want 10", got2.ToCounts["0xdead"])
	}
}

func TestHalfWrittenTempFileNotComplete(t *testing.T) {
	m := newMockServer(t)
	to := "0xdead"
	for n := int64(0); n < 10; n++ {
		m.addBlock(mockBlock{number: n, timestamp: 1000 + n, txs: []Transaction{{Type: "0x0", To: &to}}})
	}
	dir := t.TempDir()
	sc, _ := newTestScanner(t, m, dir, 10)

	// Simulate a crash mid-write: a leftover temp file exists, but the final
	// chunk_<start>.txt does not. The scanner keys completion on the final name,
	// so the chunk must be (re)scanned and the temp file must not be merged.
	tmp := filepath.Join(dir, "chunk_tmp_garbage")
	if err := os.WriteFile(tmp, []byte("META\tgarbage not a real chunk\n"), 0644); err != nil {
		t.Fatal(err)
	}

	if err := sc.Run(context.Background(), 0, 9); err != nil {
		t.Fatal(err)
	}
	// Final chunk file must now exist with the real scanned data.
	got, err := readChunk(chunkFileName(dir, 0))
	if err != nil {
		t.Fatalf("expected real chunk file to be written: %v", err)
	}
	if got.TotalTx != 10 {
		t.Errorf("TotalTx=%d want 10 (chunk was treated complete by mistake?)", got.TotalTx)
	}

	// merge must ignore the chunk_tmp_ file and not error on its garbage content.
	merged, err := mergeChunks(dir)
	if err != nil {
		t.Fatalf("merge must skip temp file, got error: %v", err)
	}
	if merged.TotalTx != 10 {
		t.Errorf("merged TotalTx=%d want 10", merged.TotalTx)
	}
}

// --- retry/backoff: failures eventually recorded to failed_blocks.log, no hang ---

func TestRetryRecoversTransientFailure(t *testing.T) {
	m := newMockServer(t)
	to := "0xdead"
	for n := int64(0); n < 5; n++ {
		m.addBlock(mockBlock{number: n, timestamp: 1000 + n, txs: []Transaction{{Type: "0x0", To: &to}}})
	}
	// Block 2 fails 2 times then succeeds (maxRetry=4 on the test client).
	m.failBlock = 2
	m.failTimes = 2

	dir := t.TempDir()
	sc, _ := newTestScanner(t, m, dir, 10)

	done := make(chan error, 1)
	go func() { done <- sc.Run(context.Background(), 0, 4) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("scan hung")
	}

	got, err := readChunk(chunkFileName(dir, 0))
	if err != nil {
		t.Fatal(err)
	}
	// All 5 blocks recovered.
	if got.TotalTx != 5 {
		t.Errorf("TotalTx=%d want 5 (transient retry should recover block 2)", got.TotalTx)
	}
	// No failed-block log entries.
	failed, _ := readFailedBlocks(dir)
	if len(failed) != 0 {
		t.Errorf("failed blocks=%v want none after recovery", failed)
	}
}

func TestRetryExhaustionRecordedToFailedLog(t *testing.T) {
	m := newMockServer(t)
	to := "0xdead"
	for n := int64(0); n < 5; n++ {
		m.addBlock(mockBlock{number: n, timestamp: 1000 + n, txs: []Transaction{{Type: "0x0", To: &to}}})
	}
	// Block 3 always fails => retries exhausted => recorded.
	m.failAlways[3] = true

	dir := t.TempDir()
	sc, _ := newTestScanner(t, m, dir, 10)

	done := make(chan error, 1)
	go func() { done <- sc.Run(context.Background(), 0, 4) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("scan hung on permanent failure")
	}

	// Chunk still persisted with the 4 good blocks.
	got, err := readChunk(chunkFileName(dir, 0))
	if err != nil {
		t.Fatal(err)
	}
	if got.TotalTx != 4 {
		t.Errorf("TotalTx=%d want 4 (one block permanently failed)", got.TotalTx)
	}

	// failed_blocks.log must contain block 3, and only after the chunk persisted.
	failed, err := readFailedBlocks(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(failed) != 1 || failed[0] != 3 {
		t.Errorf("failed blocks=%v want [3]", failed)
	}

	// And the reason text should be present in the raw log.
	raw, _ := os.ReadFile(filepath.Join(dir, "failed_blocks.log"))
	if !strings.Contains(string(raw), "3\t") {
		t.Errorf("failed log missing block 3 line: %q", string(raw))
	}
}

func TestInterruptedChunkDoesNotRecordFailures(t *testing.T) {
	// If the chunk is cancelled before it persists, buffered failures must NOT be
	// flushed to the failed log (they'd be double-counted on resume).
	m := newMockServer(t)
	to := "0xdead"
	for n := int64(0); n < 5; n++ {
		m.addBlock(mockBlock{number: n, timestamp: 1000 + n, txs: []Transaction{{Type: "0x0", To: &to}}})
	}
	m.failAlways[3] = true

	dir := t.TempDir()
	sc, _ := newTestScanner(t, m, dir, 10)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	_ = sc.Run(ctx, 0, 4) // returns ctx err; we don't assert the specific value

	// No chunk file, no failed log entries.
	if _, err := os.Stat(chunkFileName(dir, 0)); err == nil {
		t.Errorf("partial chunk must not be persisted on cancel")
	}
	failed, _ := readFailedBlocks(dir)
	if len(failed) != 0 {
		t.Errorf("cancelled chunk recorded failures %v, want none", failed)
	}
}

// --- end-to-end through the scanner + merge + classify, all on the mock ---

func TestEndToEndSmallScan(t *testing.T) {
	m := newMockServer(t)
	contract := "0xc0ffee"
	eoa := "0xe0a"
	m.setCode(contract, "0x60016002")
	m.setCode(eoa, "0x")
	for n := int64(0); n < 10; n++ {
		m.addBlock(mockBlock{number: n, timestamp: 1000 + n, txs: []Transaction{
			{Type: "0x2", To: &contract},
			{Type: "0x0", To: &eoa},
			{Type: "0x2", To: nil}, // contract creation
		}})
	}
	dir := t.TempDir()
	sc, _ := newTestScanner(t, m, dir, 4)

	if err := sc.Run(context.Background(), 0, 9); err != nil {
		t.Fatal(err)
	}
	merged, err := mergeChunks(dir)
	if err != nil {
		t.Fatal(err)
	}
	if merged.TotalTx != 30 {
		t.Errorf("TotalTx=%d want 30", merged.TotalTx)
	}
	if merged.ContractCreation != 10 {
		t.Errorf("contractCreation=%d want 10", merged.ContractCreation)
	}
	if merged.ToCounts[contract] != 10 || merged.ToCounts[eoa] != 10 {
		t.Errorf("to counts: contract=%d eoa=%d want 10/10", merged.ToCounts[contract], merged.ToCounts[eoa])
	}

	sorted := merged.sortedAddrs()
	contracts, eoas, err := classifyTop(context.Background(), m.client(), sorted, 100, 16)
	if err != nil {
		t.Fatal(err)
	}
	if len(contracts) != 1 || contracts[0].Address != contract {
		t.Errorf("contracts=%+v want [%s]", contracts, contract)
	}
	if len(eoas) != 1 || eoas[0].Address != eoa {
		t.Errorf("eoa=%+v want [%s]", eoas, eoa)
	}
}
