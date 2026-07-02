package importmysql

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"bsc_stats/common"
)

// rpcMock serves eth_getBlockByNumber(full=false) and eth_getBlockReceipts.
type rpcMock struct {
	srv      *httptest.Server
	header   map[string]interface{}
	receipts interface{}
}

func newRPCMock(t *testing.T) *rpcMock {
	m := &rpcMock{}
	m.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     int    `json:"id"`
			Method string `json:"method"`
		}
		json.NewDecoder(r.Body).Decode(&req)
		var result interface{}
		switch req.Method {
		case "eth_getBlockByNumber":
			result = m.header
		case "eth_getBlockReceipts":
			result = m.receipts
		default:
			http.Error(w, "unknown", 400)
			return
		}
		raw, _ := json.Marshal(result)
		json.NewEncoder(w).Encode(map[string]interface{}{"jsonrpc": "2.0", "id": req.ID, "result": json.RawMessage(raw)})
	}))
	t.Cleanup(m.srv.Close)
	return m
}

func (m *rpcMock) client() *common.Client {
	c := common.NewClient(m.srv.URL, 4)
	c.SetRetryPolicy(2, time.Millisecond)
	return c
}

var (
	bHash = "0x11" + strings.Repeat("00", 31) // 32 bytes
	tHash = "0x22" + strings.Repeat("00", 31)
	from1 = "0xaa" + strings.Repeat("00", 19) // 20 bytes
	to1   = "0xbb" + strings.Repeat("00", 19)
)

func TestFetchBlockUsesReceiptGasUsed(t *testing.T) {
	m := newRPCMock(t)
	m.header = map[string]interface{}{
		"number":       "0x64", // 100
		"hash":         bHash,
		"timestamp":    "0x5",      // 5
		"gasUsed":      "0x3e8",    // 1000
		"gasLimit":     "0x1e8480", // 2000000
		"transactions": []string{tHash},
	}
	m.receipts = []map[string]interface{}{
		{"transactionHash": tHash, "from": from1, "to": to1, "gasUsed": "0x2710", "blockHash": bHash, "blockNumber": "0x64"}, // 10000
	}

	bd, err := FetchBlock(context.Background(), m.client(), 100)
	if err != nil {
		t.Fatal(err)
	}
	if bd.Number != 100 || bd.Time != 5 || bd.GasUsed != 1000 || bd.GasLimit != 2000000 || bd.TxCount != 1 {
		t.Errorf("block fields wrong: %+v", bd)
	}
	if len(bd.Txs) != 1 {
		t.Fatalf("txs=%d want 1", len(bd.Txs))
	}
	tx := bd.Txs[0]
	// The critical assertion: tx gas_used comes from the RECEIPT (10000), not the
	// block body's tx gas limit.
	if tx.GasUsed != 10000 {
		t.Errorf("tx.GasUsed=%d want 10000 (from receipt)", tx.GasUsed)
	}
	if hex.EncodeToString(tx.From) != "aa"+strings.Repeat("00", 19) {
		t.Errorf("from decoded wrong: %x", tx.From)
	}
	if hex.EncodeToString(tx.To) != "bb"+strings.Repeat("00", 19) {
		t.Errorf("to decoded wrong: %x", tx.To)
	}
	if tx.BlockTime != 5 || tx.BlockNumber != 100 {
		t.Errorf("denormalized block fields wrong: time=%d num=%d", tx.BlockTime, tx.BlockNumber)
	}
}

func TestFetchBlockContractCreationNilTo(t *testing.T) {
	m := newRPCMock(t)
	m.header = map[string]interface{}{
		"number": "0x1", "hash": bHash, "timestamp": "0x1",
		"gasUsed": "0x1", "gasLimit": "0x2", "transactions": []string{tHash},
	}
	m.receipts = []map[string]interface{}{
		{"transactionHash": tHash, "from": from1, "to": nil, "gasUsed": "0x1", "blockHash": bHash, "blockNumber": "0x1"},
	}
	bd, err := FetchBlock(context.Background(), m.client(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if bd.Txs[0].To != nil {
		t.Errorf("contract-creation To must be nil, got %x", bd.Txs[0].To)
	}
}

func TestFetchBlockReceiptCountMismatch(t *testing.T) {
	m := newRPCMock(t)
	// Header says 2 txs, receipts has 1 => inconsistent, must error (block retried).
	m.header = map[string]interface{}{
		"number": "0x1", "hash": bHash, "timestamp": "0x1",
		"gasUsed": "0x1", "gasLimit": "0x2", "transactions": []string{tHash, tHash},
	}
	m.receipts = []map[string]interface{}{
		{"transactionHash": tHash, "from": from1, "to": to1, "gasUsed": "0x1"},
	}
	if _, err := FetchBlock(context.Background(), m.client(), 1); err == nil {
		t.Fatal("expected error on receipt/tx count mismatch")
	}
}

// Receipts whose blockHash disagrees with the header must be rejected (guards
// against a sibling/reorged/stale-cached receipts batch from a load-balanced node).
func TestFetchBlockRejectsWrongBlockReceipts(t *testing.T) {
	m := newRPCMock(t)
	m.header = map[string]interface{}{
		"number": "0x1", "hash": bHash, "timestamp": "0x1",
		"gasUsed": "0x1", "gasLimit": "0x2", "transactions": []string{tHash},
	}
	otherHash := "0x99" + strings.Repeat("00", 31)
	m.receipts = []map[string]interface{}{
		{"transactionHash": tHash, "from": from1, "to": to1, "gasUsed": "0x1", "blockHash": otherHash, "blockNumber": "0x1"},
	}
	if _, err := FetchBlock(context.Background(), m.client(), 1); err == nil {
		t.Fatal("expected error when receipt blockHash != header hash")
	}
}
