// Package common holds the low-level primitives shared by every subcommand:
// the retrying JSON-RPC client, hex helpers, date->block resolution, chunk
// math, progress reporting, the failed-block log, and config helpers.
package common

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"time"
)

// Client is a minimal JSON-RPC client over net/http with exponential-backoff retry.
type Client struct {
	endpoint string
	http     *http.Client
	maxRetry int
	baseWait time.Duration
}

// NewClient builds an RPC client. The http transport is tuned so a large worker
// pool can keep many connections to the same host alive.
func NewClient(endpoint string, concurrency int) *Client {
	tr := &http.Transport{
		MaxIdleConns:        concurrency * 2,
		MaxIdleConnsPerHost: concurrency * 2,
		MaxConnsPerHost:     concurrency * 2,
		IdleConnTimeout:     90 * time.Second,
	}
	return &Client{
		endpoint: endpoint,
		http:     &http.Client{Transport: tr, Timeout: 60 * time.Second},
		maxRetry: 6,
		baseWait: 200 * time.Millisecond,
	}
}

// SetRetryPolicy overrides the retry count and base backoff. Handy for tests
// (fast retries) or callers that want a policy other than the default 6/200ms.
func (c *Client) SetRetryPolicy(maxRetry int, baseWait time.Duration) {
	c.maxRetry = maxRetry
	c.baseWait = baseWait
}

type rpcRequest struct {
	JSONRPC string        `json:"jsonrpc"`
	ID      int           `json:"id"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *rpcError) Error() string { return fmt.Sprintf("rpc error %d: %s", e.Code, e.Message) }

type rpcResponse struct {
	Result json.RawMessage `json:"result"`
	Error  *rpcError       `json:"error"`
}

// callRaw performs a single JSON-RPC call without retry, returning the raw result.
func (c *Client) callRaw(ctx context.Context, method string, params ...interface{}) (json.RawMessage, error) {
	reqBody, err := json.Marshal(rpcRequest{JSONRPC: "2.0", ID: 1, Method: method, Params: params})
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http status %d: %s", resp.StatusCode, truncate(body, 200))
	}

	var rr rpcResponse
	if err := json.Unmarshal(body, &rr); err != nil {
		return nil, fmt.Errorf("decode response: %w (body=%s)", err, truncate(body, 200))
	}
	if rr.Error != nil {
		return nil, rr.Error
	}
	return rr.Result, nil
}

// Call performs a JSON-RPC call with exponential backoff retry. The decoded
// raw result is returned; callers unmarshal it into whatever shape they need.
func (c *Client) Call(ctx context.Context, method string, params ...interface{}) (json.RawMessage, error) {
	var lastErr error
	for attempt := 0; attempt <= c.maxRetry; attempt++ {
		if attempt > 0 {
			wait := c.baseWait * time.Duration(1<<uint(attempt-1))
			// jitter to avoid thundering herd
			wait += time.Duration(rand.Int63n(int64(c.baseWait)))
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(wait):
			}
		}
		res, err := c.callRaw(ctx, method, params...)
		if err == nil {
			return res, nil
		}
		lastErr = err
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	return nil, fmt.Errorf("after %d retries: %w", c.maxRetry, lastErr)
}

// blockHeader decodes the fields we need from a full=false block (timestamp only here).
type blockHeader struct {
	Number    string `json:"number"`
	Timestamp string `json:"timestamp"`
}

// BlockNumber returns the latest block height.
func (c *Client) BlockNumber(ctx context.Context) (int64, error) {
	res, err := c.Call(ctx, "eth_blockNumber")
	if err != nil {
		return 0, err
	}
	var hex string
	if err := json.Unmarshal(res, &hex); err != nil {
		return 0, err
	}
	return ParseHexInt(hex)
}

// HeaderTimestamp fetches just the timestamp of a block (full=false) for binary search.
func (c *Client) HeaderTimestamp(ctx context.Context, n int64) (int64, error) {
	res, err := c.Call(ctx, "eth_getBlockByNumber", IntToHex(n), false)
	if err != nil {
		return 0, err
	}
	var h blockHeader
	if err := json.Unmarshal(res, &h); err != nil {
		return 0, err
	}
	if h.Timestamp == "" {
		return 0, fmt.Errorf("block %d not found", n)
	}
	return ParseHexInt(h.Timestamp)
}

// GetCode returns the contract code at addr. Empty ("0x") means EOA.
func (c *Client) GetCode(ctx context.Context, addr string) (string, error) {
	res, err := c.Call(ctx, "eth_getCode", addr, "latest")
	if err != nil {
		return "", err
	}
	var code string
	if err := json.Unmarshal(res, &code); err != nil {
		return "", err
	}
	return code, nil
}

func truncate(b []byte, n int) string {
	if len(b) > n {
		return string(b[:n])
	}
	return string(b)
}
