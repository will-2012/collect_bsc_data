package main

import (
	"context"
	"strings"
	"testing"
	"time"
)

// --- block range resolution by timestamp (binary search) ---

// buildLinearChain populates the mock with blocks 0..n-1 whose timestamps are
// base + number*step seconds.
func buildLinearChain(m *mockServer, n int, base, step int64) {
	for i := 0; i < n; i++ {
		m.addBlock(mockBlock{number: int64(i), timestamp: base + int64(i)*step})
	}
}

func TestFindStartBlock(t *testing.T) {
	m := newMockServer(t)
	// ts(block i) = 1000 + 10*i. Blocks 0..99.
	buildLinearChain(m, 100, 1000, 10)
	c := m.client()
	ctx := context.Background()

	cases := []struct {
		target int64
		want   int64
	}{
		{target: 1000, want: 0},  // exact first
		{target: 1001, want: 1},  // between 1000 and 1010 => first >= is block 1
		{target: 1010, want: 1},  // exact
		{target: 1055, want: 6},  // ts(5)=1050, ts(6)=1060 => 6
		{target: 990, want: 0},   // before all => block 0
	}
	for _, tc := range cases {
		got, err := findStartBlock(ctx, c, 0, m.latest, tc.target)
		if err != nil {
			t.Fatalf("target=%d: %v", tc.target, err)
		}
		if got != tc.want {
			t.Errorf("findStartBlock(target=%d)=%d want %d", tc.target, got, tc.want)
		}
	}
}

func TestFindEndBlock(t *testing.T) {
	m := newMockServer(t)
	buildLinearChain(m, 100, 1000, 10) // ts(i)=1000+10i
	c := m.client()
	ctx := context.Background()

	cases := []struct {
		target int64
		want   int64
	}{
		{target: 1990, want: 99}, // ts(99)=1990 exact last
		{target: 2000, want: 99}, // beyond last => last block
		{target: 1000, want: 0},  // exact first => block 0 (last <= target)
		{target: 1059, want: 5},  // ts(5)=1050 <= 1059 < ts(6)=1060 => 5
		{target: 1060, want: 6},  // exact
	}
	for _, tc := range cases {
		got, err := findEndBlock(ctx, c, 0, m.latest, tc.target)
		if err != nil {
			t.Fatalf("target=%d: %v", tc.target, err)
		}
		if got != tc.want {
			t.Errorf("findEndBlock(target=%d)=%d want %d", tc.target, got, tc.want)
		}
	}
}

func TestResolveRange(t *testing.T) {
	m := newMockServer(t)
	// One block per hour starting 2025-05-01 00:00 UTC.
	base := time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC).Unix()
	hour := int64(3600)
	for i := 0; i < 48; i++ {
		m.addBlock(mockBlock{number: int64(i), timestamp: base + int64(i)*hour})
	}
	c := m.client()

	start, _ := parseDate("2025-05-01")
	endDay, _ := parseDate("2025-05-02")
	cfg := &Config{StartDate: start, EndDate: endDay.Add(24*time.Hour - time.Second)}

	sb, eb, err := resolveRange(context.Background(), c, cfg)
	if err != nil {
		t.Fatal(err)
	}
	// start: first ts >= 2025-05-01 00:00 => block 0.
	// end: last ts <= 2025-05-02 23:59:59 => block 47 (2025-05-02 23:00).
	if sb != 0 {
		t.Errorf("start block=%d want 0", sb)
	}
	if eb != 47 {
		t.Errorf("end block=%d want 47", eb)
	}
}

// --- per-block stats extraction via real RPC parsing through the mock ---

func TestGetBlockAndStats(t *testing.T) {
	m := newMockServer(t)
	to1 := "0xAbC"
	to2 := "0xdef"
	m.addBlock(mockBlock{
		number:    1,
		timestamp: 1000,
		txs: []Transaction{
			{Type: "0x0", To: &to1},
			{Type: "0x2", To: &to2},
			{Type: "", To: &to1},   // missing type => legacy bucket
			{Type: "0x2", To: nil}, // contract creation
			{Type: "0x4", To: &to2},
		},
	})
	c := m.client()
	b, err := c.GetBlock(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}

	cr := newChunkResult(1, 1)
	cr.addBlock(b)

	if cr.TotalTx != 5 {
		t.Errorf("TotalTx=%d want 5", cr.TotalTx)
	}
	if cr.TypeCounts[0] != 2 { // 0x0 + missing
		t.Errorf("legacy=%d want 2", cr.TypeCounts[0])
	}
	if cr.TypeCounts[2] != 2 {
		t.Errorf("dynfee=%d want 2", cr.TypeCounts[2])
	}
	if cr.TypeCounts[4] != 1 {
		t.Errorf("setcode=%d want 1", cr.TypeCounts[4])
	}
	if cr.ContractCreation != 1 {
		t.Errorf("contractCreation=%d want 1", cr.ContractCreation)
	}
	// to-address counting is case-folded; to1 appears twice.
	if cr.ToCounts["0xabc"] != 2 {
		t.Errorf("to1 count=%d want 2", cr.ToCounts["0xabc"])
	}
	if cr.ToCounts["0xdef"] != 2 {
		t.Errorf("to2 count=%d want 2", cr.ToCounts["0xdef"])
	}
	// contract-creation tx must NOT appear in the to map.
	if len(cr.ToCounts) != 2 {
		t.Errorf("unique to addrs=%d want 2", len(cr.ToCounts))
	}
}

// --- merge: summing chunk files, percentages, top selection ---

func TestMergeChunks(t *testing.T) {
	dir := t.TempDir()

	cr1 := newChunkResult(0, 9)
	cr1.TotalTx = 10
	cr1.TypeCounts[0] = 6
	cr1.TypeCounts[2] = 4
	cr1.ContractCreation = 1
	cr1.ToCounts["0xa"] = 5
	cr1.ToCounts["0xb"] = 5
	if err := writeChunk(dir, cr1); err != nil {
		t.Fatal(err)
	}

	cr2 := newChunkResult(10, 19)
	cr2.TotalTx = 20
	cr2.TypeCounts[0] = 10
	cr2.TypeCounts[2] = 10
	cr2.ContractCreation = 2
	cr2.ToCounts["0xa"] = 15 // overlaps cr1's 0xa
	cr2.ToCounts["0xc"] = 5
	if err := writeChunk(dir, cr2); err != nil {
		t.Fatal(err)
	}

	m, err := mergeChunks(dir)
	if err != nil {
		t.Fatal(err)
	}
	if m.TotalTx != 30 {
		t.Errorf("TotalTx=%d want 30", m.TotalTx)
	}
	if m.TypeCounts[0] != 16 || m.TypeCounts[2] != 14 {
		t.Errorf("typecounts=%v want [16,_,14,...]", m.TypeCounts)
	}
	if m.ContractCreation != 3 {
		t.Errorf("contractCreation=%d want 3", m.ContractCreation)
	}
	if m.ToCounts["0xa"] != 20 {
		t.Errorf("0xa=%d want 20 (summed across chunks)", m.ToCounts["0xa"])
	}

	// percentage math: total to-count = 20+5+5 = 30.
	sorted := m.sortedAddrs()
	if sorted[0].Address != "0xa" || sorted[0].Count != 20 {
		t.Errorf("top addr=%+v want 0xa/20", sorted[0])
	}
	wantPct := float64(20) / float64(30) * 100
	if diff := sorted[0].Percentage - wantPct; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("0xa pct=%v want %v", sorted[0].Percentage, wantPct)
	}
}

func TestSortedAddrsTopSelectionAndTieBreak(t *testing.T) {
	counts := map[string]int64{}
	// 150 addrs with descending counts, plus a tie pair.
	for i := 0; i < 150; i++ {
		counts[strings.Repeat("z", 0)+itoaAddr(i)] = int64(1000 - i)
	}
	// tie: two addrs at count 1000 (same as addr0). addr0 = "0xaddr0000".
	m := &Merged{ToCounts: counts}
	sorted := m.sortedAddrs()
	if len(sorted) != 150 {
		t.Fatalf("len=%d want 150", len(sorted))
	}
	// strictly non-increasing counts
	for i := 1; i < len(sorted); i++ {
		if sorted[i].Count > sorted[i-1].Count {
			t.Fatalf("not sorted desc at %d: %d > %d", i, sorted[i].Count, sorted[i-1].Count)
		}
	}
}

func itoaAddr(i int) string {
	const hexd = "0123456789abcdef"
	// produce a unique lowercased pseudo-address string
	s := "0xaddr"
	s += string(hexd[(i/16)%16])
	s += string(hexd[i%16])
	s += "00"
	return s
}

// --- contract vs EOA classification via mocked eth_getCode ---

func TestClassifyTop(t *testing.T) {
	m := newMockServer(t)
	c := m.client()

	// Build 250 candidate addresses with decreasing counts. Even-indexed are
	// contracts (non-empty code), odd are EOA (default "0x").
	var sorted []AddrCount
	for i := 0; i < 250; i++ {
		addr := addrN(i)
		sorted = append(sorted, AddrCount{Address: addr, Count: int64(1000 - i)})
		if i%2 == 0 {
			m.setCode(addr, "0x6080604052") // contract
		} else {
			m.setCode(addr, "0x") // EOA
		}
	}

	contracts, eoa, err := classifyTop(context.Background(), c, sorted, 100, 16)
	if err != nil {
		t.Fatal(err)
	}
	if len(contracts) != 100 {
		t.Errorf("contracts=%d want 100", len(contracts))
	}
	if len(eoa) != 100 {
		t.Errorf("eoa=%d want 100", len(eoa))
	}
	// Each list must preserve count-desc order and contain only its class.
	for _, ac := range contracts {
		// even index -> contract; verify via the mocked code map
		if m.code[ac.Address] == "0x" || m.code[ac.Address] == "" {
			t.Errorf("contract list contains EOA %s", ac.Address)
		}
	}
	for _, ac := range eoa {
		if m.code[ac.Address] != "0x" {
			t.Errorf("eoa list contains contract %s", ac.Address)
		}
	}
	// Should stop early once both full: well under 250 getCode calls in principle,
	// but with interleaved classes it needs ~200. Just assert it didn't scan more
	// than the candidate pool.
	if m.getCode > 250 {
		t.Errorf("getCode calls=%d exceeds candidate pool", m.getCode)
	}
}

func TestClassifyEmptyCodeIsEOA(t *testing.T) {
	m := newMockServer(t)
	c := m.client()
	// Address returns "0x" (empty) => must be EOA.
	a := "0xeoa"
	m.setCode(a, "0x")
	sorted := []AddrCount{{Address: a, Count: 1}}
	contracts, eoa, err := classifyTop(context.Background(), c, sorted, 100, 16)
	if err != nil {
		t.Fatal(err)
	}
	if len(contracts) != 0 || len(eoa) != 1 {
		t.Errorf("empty-code classify: contracts=%d eoa=%d want 0/1", len(contracts), len(eoa))
	}
}

func addrN(i int) string {
	const hexd = "0123456789abcdef"
	s := []byte("0x")
	// 4 hex digits is enough for <65536 distinct addrs
	for shift := 12; shift >= 0; shift -= 4 {
		s = append(s, hexd[(i>>uint(shift))&0xf])
	}
	return string(s)
}
