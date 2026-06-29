package main

import (
	"os"
	"testing"
)

func strptr(s string) *string { return &s }

func TestChunkRoundTrip(t *testing.T) {
	dir, err := os.MkdirTemp("", "cbd_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cr := newChunkResult(100, 199)
	b := &Block{Transactions: []Transaction{
		{Type: "0x0", To: strptr("0xAAA")},
		{Type: "0x2", To: strptr("0xaaa")}, // same addr, different case
		{Type: "", To: strptr("0xbbb")},    // missing type => legacy
		{Type: "0x2", To: nil},             // contract creation
	}}
	cr.addBlock(b)

	if cr.TotalTx != 4 {
		t.Fatalf("TotalTx=%d want 4", cr.TotalTx)
	}
	if cr.TypeCounts[0] != 2 {
		t.Fatalf("legacy=%d want 2", cr.TypeCounts[0])
	}
	if cr.TypeCounts[2] != 2 {
		t.Fatalf("dynfee=%d want 2", cr.TypeCounts[2])
	}
	if cr.ContractCreation != 1 {
		t.Fatalf("contractCreation=%d want 1", cr.ContractCreation)
	}
	if cr.ToCounts["0xaaa"] != 2 {
		t.Fatalf("0xaaa=%d want 2 (case-folded)", cr.ToCounts["0xaaa"])
	}

	if err := writeChunk(dir, cr); err != nil {
		t.Fatal(err)
	}
	got, err := readChunk(chunkFileName(dir, 100))
	if err != nil {
		t.Fatal(err)
	}
	if got.TotalTx != cr.TotalTx || got.ContractCreation != cr.ContractCreation ||
		got.ToCounts["0xaaa"] != 2 || got.TypeCounts[2] != 2 {
		t.Fatalf("round trip mismatch: %+v", got)
	}
}

func TestChunkBounds(t *testing.T) {
	got := chunkBounds(10, 25, 10)
	want := [][2]int64{{10, 19}, {20, 25}}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("chunk %d: got %v want %v", i, got[i], want[i])
		}
	}
}

func TestSortedAddrs(t *testing.T) {
	m := &Merged{ToCounts: map[string]int64{"0xa": 5, "0xb": 10, "0xc": 10}}
	s := m.sortedAddrs()
	// desc by count; ties broken by address asc
	if s[0].Address != "0xb" || s[1].Address != "0xc" || s[2].Address != "0xa" {
		t.Fatalf("unexpected order: %+v", s)
	}
	if s[0].Count != 10 || s[0].Percentage <= 0 {
		t.Fatalf("bad top entry: %+v", s[0])
	}
}

func TestNormTxType(t *testing.T) {
	cases := map[string]int{"": 0, "0x0": 0, "0x1": 1, "0x2": 2, "0x4": 4}
	for in, want := range cases {
		if got := normTxType(in); got != want {
			t.Fatalf("normTxType(%q)=%d want %d", in, got, want)
		}
	}
}
