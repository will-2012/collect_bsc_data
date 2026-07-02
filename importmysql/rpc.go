package importmysql

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"bsc_stats/common"
)

// BlockData is one block plus its transactions, ready to persist.
type BlockData struct {
	Number   int64
	Hash     []byte // 32 bytes
	Time     int64  // unix seconds
	GasUsed  int64
	GasLimit int64
	TxCount  int
	Txs      []TxData
}

// TxData is one transaction row. BlockHash/BlockTime are denormalized from the
// block so the time-range + address query needs no join, and tx->block lookups
// have the block key in hand.
type TxData struct {
	Hash        []byte // 32
	BlockNumber int64
	BlockHash   []byte // 32
	BlockTime   int64
	GasUsed     int64
	From        []byte // 20
	To          []byte // 20, or nil for contract creation
}

// rpcHeader is the full=false block header: everything we need for the block
// row plus the tx-hash list for the count/reconciliation.
type rpcHeader struct {
	Number       string   `json:"number"`
	Hash         string   `json:"hash"`
	Timestamp    string   `json:"timestamp"`
	GasUsed      string   `json:"gasUsed"`
	GasLimit     string   `json:"gasLimit"`
	Transactions []string `json:"transactions"`
}

// rpcReceipt is the subset of a transaction receipt we need. gasUsed here is the
// real gas consumed (not the tx gas limit), which is what the share metric needs.
type rpcReceipt struct {
	TransactionHash string  `json:"transactionHash"`
	From            string  `json:"from"`
	To              *string `json:"to"`
	GasUsed         string  `json:"gasUsed"`
	BlockHash       string  `json:"blockHash"`
	BlockNumber     string  `json:"blockNumber"`
}

// FetchBlock retrieves a block header and its receipts and assembles a BlockData.
// It uses two light calls (eth_getBlockByNumber full=false + eth_getBlockReceipts)
// rather than the heavy full-tx body, because per-tx gas_used only lives in receipts.
func FetchBlock(ctx context.Context, c *common.Client, n int64) (*BlockData, error) {
	hdrRaw, err := c.Call(ctx, "eth_getBlockByNumber", common.IntToHex(n), false)
	if err != nil {
		return nil, fmt.Errorf("getBlockByNumber: %w", err)
	}
	var h rpcHeader
	if err := json.Unmarshal(hdrRaw, &h); err != nil {
		return nil, fmt.Errorf("decode header: %w", err)
	}
	if h.Number == "" {
		return nil, fmt.Errorf("block %d not found", n)
	}

	bd := &BlockData{Number: n, TxCount: len(h.Transactions)}
	if bd.Hash, err = decodeHex(h.Hash, 32); err != nil {
		return nil, fmt.Errorf("block hash: %w", err)
	}
	if bd.Time, err = common.ParseHexInt(h.Timestamp); err != nil {
		return nil, fmt.Errorf("timestamp: %w", err)
	}
	if bd.GasUsed, err = common.ParseHexInt(h.GasUsed); err != nil {
		return nil, fmt.Errorf("block gasUsed: %w", err)
	}
	if bd.GasLimit, err = common.ParseHexInt(h.GasLimit); err != nil {
		return nil, fmt.Errorf("block gasLimit: %w", err)
	}

	rcptRaw, err := c.Call(ctx, "eth_getBlockReceipts", common.IntToHex(n))
	if err != nil {
		return nil, fmt.Errorf("getBlockReceipts: %w", err)
	}
	var receipts []rpcReceipt
	if err := json.Unmarshal(rcptRaw, &receipts); err != nil {
		return nil, fmt.Errorf("decode receipts: %w", err)
	}
	// Reconciliation: a receipt per transaction. A mismatch means the node
	// returned an inconsistent view; fail the block so it is retried rather than
	// silently persisting partial tx rows.
	if len(receipts) != bd.TxCount {
		return nil, fmt.Errorf("block %d: %d receipts != %d transactions", n, len(receipts), bd.TxCount)
	}
	// The header and receipts come from two independent calls against a
	// load-balanced endpoint. Verify they describe the SAME block: every receipt
	// must carry this block's hash+number, and the receipt tx-hash set must equal
	// the header's tx list (not just match in count). This rejects a sibling/
	// reorged/stale-cached receipts batch that would otherwise stamp the header's
	// hash/time onto txs that don't belong to it.
	headerTxs := make(map[string]bool, len(h.Transactions))
	for _, th := range h.Transactions {
		headerTxs[strings.ToLower(th)] = true
	}

	bd.Txs = make([]TxData, 0, len(receipts))
	for i := range receipts {
		r := &receipts[i]
		if !strings.EqualFold(r.BlockHash, h.Hash) {
			return nil, fmt.Errorf("block %d: receipt blockHash %s != header %s", n, r.BlockHash, h.Hash)
		}
		if rn, err := common.ParseHexInt(r.BlockNumber); err != nil || rn != n {
			return nil, fmt.Errorf("block %d: receipt blockNumber %s mismatch", n, r.BlockNumber)
		}
		if !headerTxs[strings.ToLower(r.TransactionHash)] {
			return nil, fmt.Errorf("block %d: receipt tx %s not in header tx set", n, r.TransactionHash)
		}
		tx := TxData{BlockNumber: n, BlockHash: bd.Hash, BlockTime: bd.Time}
		if tx.Hash, err = decodeHex(r.TransactionHash, 32); err != nil {
			return nil, fmt.Errorf("tx hash: %w", err)
		}
		if tx.From, err = decodeHex(r.From, 20); err != nil {
			return nil, fmt.Errorf("tx from: %w", err)
		}
		if tx.GasUsed, err = common.ParseHexInt(r.GasUsed); err != nil {
			return nil, fmt.Errorf("tx gasUsed: %w", err)
		}
		if r.To != nil && *r.To != "" { // nil To == contract creation
			if tx.To, err = decodeHex(*r.To, 20); err != nil {
				return nil, fmt.Errorf("tx to: %w", err)
			}
		}
		bd.Txs = append(bd.Txs, tx)
	}
	return bd, nil
}

// decodeHex parses a "0x"-prefixed hex string into exactly n raw bytes.
func decodeHex(s string, n int) ([]byte, error) {
	b, err := hex.DecodeString(strings.TrimPrefix(s, "0x"))
	if err != nil {
		return nil, err
	}
	if len(b) != n {
		return nil, fmt.Errorf("expected %d bytes, got %d (%q)", n, len(b), s)
	}
	return b, nil
}
