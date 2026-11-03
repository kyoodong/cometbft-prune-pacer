package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	pruningv1 "github.com/cometbft/cometbft/api/cometbft/services/pruning/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Retain identifies one of the four retain heights exposed by PruningService.
type Retain int

const (
	RetainBlock Retain = iota
	RetainBlockResults
	RetainTxIndexer
	RetainBlockIndexer
)

func (r Retain) String() string {
	return [...]string{"block", "block_results", "tx_indexer", "block_indexer"}[r]
}

// Node is everything the pacer needs from a CometBFT node. The interface
// exists so the decision loop can be tested against a fake node.
type Node interface {
	Status(ctx context.Context) (latest, earliest int64, catchingUp bool, err error)
	// TxHashesAt returns hashes of indexed txs at exactly this height.
	// It uses tx_search "tx.height=H", which the kv indexer serves with a
	// prefix seek on "tx.height/H/" instead of a full scan.
	TxHashesAt(ctx context.Context, height int64) ([]string, error)
	TxIndexed(ctx context.Context, hash string) (bool, error)
	// BlockIndexed uses block_search "block.height=H", served by Has(height).
	BlockIndexed(ctx context.Context, height int64) (bool, error)
	// GetRetain returns ok=false when the retain height was never set.
	GetRetain(ctx context.Context, r Retain) (h int64, ok bool, err error)
	SetRetain(ctx context.Context, r Retain, h int64) error
	MemAvailable() (uint64, error)
	Metric(ctx context.Context, name string) (int64, bool)
}

type liveNode struct {
	rpc     string
	metrics string
	http    *http.Client
	conn    *grpc.ClientConn
	prune   pruningv1.PruningServiceClient
}

func dialNode(c *Config) (*liveNode, error) {
	conn, err := grpc.NewClient(c.GRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &liveNode{
		rpc:     c.RPCAddr,
		metrics: c.MetricsAddr,
		http:    &http.Client{Timeout: 60 * time.Second},
		conn:    conn,
		prune:   pruningv1.NewPruningServiceClient(conn),
	}, nil
}

func (n *liveNode) getJSON(ctx context.Context, path string, q url.Values, out any) error {
	u := n.rpc + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	resp, err := n.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	var env struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Data string `json:"data"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	if env.Error != nil {
		return &rpcError{msg: env.Error.Data}
	}
	return json.Unmarshal(env.Result, out)
}

type rpcError struct{ msg string }

func (e *rpcError) Error() string { return e.msg }

func (n *liveNode) Status(ctx context.Context) (int64, int64, bool, error) {
	var r struct {
		SyncInfo struct {
			Latest     string `json:"latest_block_height"`
			Earliest   string `json:"earliest_block_height"`
			CatchingUp bool   `json:"catching_up"`
		} `json:"sync_info"`
	}
	if err := n.getJSON(ctx, "/status", nil, &r); err != nil {
		return 0, 0, false, err
	}
	l, _ := strconv.ParseInt(r.SyncInfo.Latest, 10, 64)
	e, _ := strconv.ParseInt(r.SyncInfo.Earliest, 10, 64)
	return l, e, r.SyncInfo.CatchingUp, nil
}

func (n *liveNode) TxHashesAt(ctx context.Context, h int64) ([]string, error) {
	var r struct {
		Txs []struct {
			Hash string `json:"hash"`
		} `json:"txs"`
	}
	q := url.Values{"query": {fmt.Sprintf(`"tx.height=%d"`, h)}, "per_page": {"100"}}
	if err := n.getJSON(ctx, "/tx_search", q, &r); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(r.Txs))
	for _, t := range r.Txs {
		out = append(out, t.Hash)
	}
	return out, nil
}

func (n *liveNode) TxIndexed(ctx context.Context, hash string) (bool, error) {
	var r json.RawMessage
	err := n.getJSON(ctx, "/tx", url.Values{"hash": {"0x" + hash}}, &r)
	var re *rpcError
	if errors.As(err, &re) && strings.Contains(re.msg, "not found") {
		return false, nil
	}
	return err == nil, err
}

func (n *liveNode) BlockIndexed(ctx context.Context, h int64) (bool, error) {
	var r struct {
		Total string `json:"total_count"`
	}
	q := url.Values{"query": {fmt.Sprintf(`"block.height=%d"`, h)}}
	if err := n.getJSON(ctx, "/block_search", q, &r); err != nil {
		return false, err
	}
	return r.Total != "0", nil
}

func (n *liveNode) GetRetain(ctx context.Context, r Retain) (int64, bool, error) {
	var h uint64
	var err error
	switch r {
	case RetainBlock:
		var res *pruningv1.GetBlockRetainHeightResponse
		if res, err = n.prune.GetBlockRetainHeight(ctx, &pruningv1.GetBlockRetainHeightRequest{}); err == nil {
			h = res.PruningServiceRetainHeight
		}
	case RetainBlockResults:
		var res *pruningv1.GetBlockResultsRetainHeightResponse
		if res, err = n.prune.GetBlockResultsRetainHeight(ctx, &pruningv1.GetBlockResultsRetainHeightRequest{}); err == nil {
			h = res.PruningServiceRetainHeight
		}
	case RetainTxIndexer:
		var res *pruningv1.GetTxIndexerRetainHeightResponse
		if res, err = n.prune.GetTxIndexerRetainHeight(ctx, &pruningv1.GetTxIndexerRetainHeightRequest{}); err == nil {
			h = res.Height
		}
	case RetainBlockIndexer:
		var res *pruningv1.GetBlockIndexerRetainHeightResponse
		if res, err = n.prune.GetBlockIndexerRetainHeight(ctx, &pruningv1.GetBlockIndexerRetainHeightRequest{}); err == nil {
			h = res.Height
		}
	}
	if err != nil {
		// An indexer retain height that was never set comes back as
		// Internal "Failed to get ... retain height" (node log: key not found).
		if s, ok := status.FromError(err); ok && s.Code() == codes.Internal && strings.Contains(s.Message(), "Failed to get") {
			return 0, false, nil
		}
		return 0, false, err
	}
	return int64(h), h > 0, nil
}

func (n *liveNode) SetRetain(ctx context.Context, r Retain, h int64) error {
	u := uint64(h)
	var err error
	switch r {
	case RetainBlock:
		_, err = n.prune.SetBlockRetainHeight(ctx, &pruningv1.SetBlockRetainHeightRequest{Height: u})
	case RetainBlockResults:
		_, err = n.prune.SetBlockResultsRetainHeight(ctx, &pruningv1.SetBlockResultsRetainHeightRequest{Height: u})
	case RetainTxIndexer:
		_, err = n.prune.SetTxIndexerRetainHeight(ctx, &pruningv1.SetTxIndexerRetainHeightRequest{Height: u})
	case RetainBlockIndexer:
		_, err = n.prune.SetBlockIndexerRetainHeight(ctx, &pruningv1.SetBlockIndexerRetainHeightRequest{Height: u})
	}
	return err
}

func (n *liveNode) MemAvailable() (uint64, error) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) >= 2 && fields[0] == "MemAvailable:" {
			kb, err := strconv.ParseUint(fields[1], 10, 64)
			return kb * 1024, err
		}
	}
	return 0, errors.New("MemAvailable not found in /proc/meminfo")
}

// Metric returns a cometbft_state_* gauge, if Prometheus is reachable.
func (n *liveNode) Metric(ctx context.Context, name string) (int64, bool) {
	if n.metrics == "" {
		return 0, false
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, n.metrics, nil)
	resp, err := n.http.Do(req)
	if err != nil {
		return 0, false
	}
	defer func() { _ = resp.Body.Close() }()
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	prefix := "cometbft_state_" + name + "{"
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, prefix) {
			i := strings.LastIndexByte(line, ' ')
			v, err := strconv.ParseFloat(line[i+1:], 64)
			return int64(v), err == nil
		}
	}
	return 0, false
}
