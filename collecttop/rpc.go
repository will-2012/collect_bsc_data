package collecttop

import (
	"context"
	"encoding/json"
	"fmt"

	"bsc_stats/common"
)

// Transaction is the subset of a tx we need.
type Transaction struct {
	Type string  `json:"type"` // hex, e.g. "0x2"; may be absent
	To   *string `json:"to"`   // nil for contract creation
}

// Block is the subset of a block we need.
type Block struct {
	Number       string        `json:"number"`
	Timestamp    string        `json:"timestamp"`
	Transactions []Transaction `json:"transactions"`
}

// GetBlock fetches a full block with all transaction objects.
func GetBlock(ctx context.Context, c *common.Client, n int64) (*Block, error) {
	res, err := c.Call(ctx, "eth_getBlockByNumber", common.IntToHex(n), true)
	if err != nil {
		return nil, err
	}
	var b Block
	if err := json.Unmarshal(res, &b); err != nil {
		return nil, err
	}
	if b.Number == "" {
		return nil, fmt.Errorf("block %d not found", n)
	}
	return &b, nil
}
