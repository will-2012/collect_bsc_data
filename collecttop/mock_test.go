package collecttop

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"bsc_stats/common"
)

// mockBlock is a canned block the mock server can serve.
type mockBlock struct {
	number    int64
	timestamp int64
	txs       []Transaction
}

// mockServer is a stdlib httptest-based JSON-RPC server returning canned
// eth_blockNumber / eth_getBlockByNumber / eth_getCode responses.
type mockServer struct {
	t       *testing.T
	srv     *httptest.Server
	mu      sync.Mutex
	latest  int64
	blocks  map[int64]mockBlock
	code    map[string]string // addr(lower) -> code hex
	calls   int64             // total RPC calls served (atomic)
	getCode int64             // eth_getCode calls (atomic)

	// failBlock, when set, returns an HTTP 500 for eth_getBlockByNumber of that
	// block for the first failTimes attempts (per-attempt counter).
	failBlock  int64
	failTimes  int
	failSeen   int
	failAlways map[int64]bool // blocks that always 500 on full-block fetch
}

func newMockServer(t *testing.T) *mockServer {
	m := &mockServer{
		t:          t,
		blocks:     map[int64]mockBlock{},
		code:       map[string]string{},
		failAlways: map[int64]bool{},
	}
	m.srv = httptest.NewServer(http.HandlerFunc(m.handle))
	t.Cleanup(m.srv.Close)
	return m
}

func (m *mockServer) URL() string { return m.srv.URL }

// client builds a Client pointed at the mock with fast retries for tests.
func (m *mockServer) client() *common.Client {
	c := common.NewClient(m.URL(), 8)
	c.SetRetryPolicy(4, time.Millisecond)
	return c
}

func (m *mockServer) addBlock(b mockBlock) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.blocks[b.number] = b
	if b.number > m.latest {
		m.latest = b.number
	}
}

func (m *mockServer) setCode(addr, code string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.code[addr] = code
}

func (m *mockServer) writeResult(w http.ResponseWriter, id int, result interface{}) {
	raw, err := json.Marshal(result)
	if err != nil {
		m.t.Fatalf("marshal result: %v", err)
	}
	resp := map[string]interface{}{"jsonrpc": "2.0", "id": id, "result": json.RawMessage(raw)}
	_ = json.NewEncoder(w).Encode(resp)
}

func (m *mockServer) handle(w http.ResponseWriter, r *http.Request) {
	atomic.AddInt64(&m.calls, 1)
	var req struct {
		ID     int           `json:"id"`
		Method string        `json:"method"`
		Params []interface{} `json:"params"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	switch req.Method {
	case "eth_blockNumber":
		m.mu.Lock()
		latest := m.latest
		m.mu.Unlock()
		m.writeResult(w, req.ID, common.IntToHex(latest))

	case "eth_getBlockByNumber":
		hexNum, _ := req.Params[0].(string)
		full, _ := req.Params[1].(bool)
		n, err := common.ParseHexInt(hexNum)
		if err != nil {
			http.Error(w, "bad block param", http.StatusBadRequest)
			return
		}

		m.mu.Lock()
		// Transient/permanent failure injection.
		if m.failAlways[n] {
			m.mu.Unlock()
			http.Error(w, "injected always-fail", http.StatusInternalServerError)
			return
		}
		if m.failBlock != 0 && n == m.failBlock && m.failSeen < m.failTimes {
			m.failSeen++
			m.mu.Unlock()
			http.Error(w, "injected transient", http.StatusInternalServerError)
			return
		}
		b, ok := m.blocks[n]
		m.mu.Unlock()

		if !ok {
			// Unknown block => RPC null result (as real nodes do).
			m.writeResult(w, req.ID, nil)
			return
		}
		if full {
			m.writeResult(w, req.ID, map[string]interface{}{
				"number":       common.IntToHex(b.number),
				"timestamp":    common.IntToHex(b.timestamp),
				"transactions": b.txs,
			})
		} else {
			m.writeResult(w, req.ID, map[string]interface{}{
				"number":    common.IntToHex(b.number),
				"timestamp": common.IntToHex(b.timestamp),
			})
		}

	case "eth_getCode":
		atomic.AddInt64(&m.getCode, 1)
		addr, _ := req.Params[0].(string)
		m.mu.Lock()
		code, ok := m.code[addr]
		m.mu.Unlock()
		if !ok {
			code = "0x" // default: EOA
		}
		m.writeResult(w, req.ID, code)

	default:
		http.Error(w, fmt.Sprintf("unknown method %q", req.Method), http.StatusBadRequest)
	}
}
