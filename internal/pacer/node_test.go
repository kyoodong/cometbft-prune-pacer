package pacer

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	pruningv1 "github.com/cometbft/cometbft/api/cometbft/services/pruning/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// noPanic runs f and turns a panic into a test error.
func noPanic(t *testing.T, name string, f func()) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("%s panicked: %v", name, r)
		}
	}()
	f()
}

// TestF07MalformedAddrReturnsError: an RPC or metrics address that net/url
// cannot parse (host:port without a scheme, trailing space) must surface as
// an error, not as a nil-request panic inside http.Client.Do.
func TestF07MalformedAddrReturnsError(t *testing.T) {
	ctx := context.Background()
	for _, addr := range []string{"127.0.0.1:26657", "http://127.0.0.1:26657 "} {
		n := &LiveNode{rpc: addr, metrics: addr + "/metrics", http: &http.Client{Timeout: time.Second}}
		noPanic(t, fmt.Sprintf("Status(rpc=%q)", addr), func() {
			if _, _, _, err := n.Status(ctx); err == nil {
				t.Errorf("Status(rpc=%q): want error, got nil", addr)
			}
		})
		noPanic(t, fmt.Sprintf("Metric(metrics=%q)", addr+"/metrics"), func() {
			if _, ok := n.Metric(ctx, "application_block_retain_height"); ok {
				t.Errorf("Metric(%q): want ok=false", addr)
			}
		})
	}
}

// fakePruning mimics the node's PruningService, including the Internal error
// returned for an indexer retain height that was never set.
type fakePruning struct {
	pruningv1.UnimplementedPruningServiceServer
	tx uint64
}

func (s *fakePruning) GetTxIndexerRetainHeight(context.Context, *pruningv1.GetTxIndexerRetainHeightRequest) (*pruningv1.GetTxIndexerRetainHeightResponse, error) {
	if s.tx == 0 {
		return nil, status.Error(codes.Internal, "Failed to get tx indexer retain height (see logs for trace ID: x)")
	}
	return &pruningv1.GetTxIndexerRetainHeightResponse{Height: s.tx}, nil
}

func (s *fakePruning) SetTxIndexerRetainHeight(_ context.Context, r *pruningv1.SetTxIndexerRetainHeightRequest) (*pruningv1.SetTxIndexerRetainHeightResponse, error) {
	s.tx = r.Height
	return &pruningv1.SetTxIndexerRetainHeightResponse{}, nil
}

// The api package is gogoproto-generated; this checks it round-trips over a
// real gRPC connection with the default codec.
func TestLiveNodeGRPCRoundTrip(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	pruningv1.RegisterPruningServiceServer(srv, &fakePruning{})
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	n, err := DialNode(&Config{GRPCAddr: lis.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, ok, err := n.GetRetain(ctx, RetainTxIndexer); err != nil || ok {
		t.Fatalf("unset: ok=%v err=%v", ok, err)
	}
	if err := n.SetRetain(ctx, RetainTxIndexer, 176956724); err != nil {
		t.Fatal(err)
	}
	if h, ok, err := n.GetRetain(ctx, RetainTxIndexer); err != nil || !ok || h != 176956724 {
		t.Fatalf("got %d ok=%v err=%v", h, ok, err)
	}
	if _, _, err := n.GetRetain(ctx, RetainBlock); status.Code(err) != codes.Unimplemented {
		t.Fatalf("want Unimplemented, got %v", err)
	}
}

// RPC parsing against responses shaped like CometBFT's.
func TestLiveNodeRPC(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/status":
			_, _ = w.Write([]byte(`{"result":{"sync_info":{"latest_block_height":"190000000","earliest_block_height":"176946601","catching_up":false}}}`))
		case "/tx_search":
			if r.URL.Query().Get("query") != `"tx.height=100"` {
				t.Errorf("query %q", r.URL.Query().Get("query"))
			}
			_, _ = w.Write([]byte(`{"result":{"txs":[{"hash":"AA"},{"hash":"BB"}],"total_count":"2"}}`))
		case "/tx":
			_, _ = w.Write([]byte(`{"error":{"code":-32603,"message":"Internal error","data":"tx (BB) not found"}}`))
		case "/block_search":
			_, _ = w.Write([]byte(`{"result":{"blocks":[],"total_count":"0"}}`))
		}
	}))
	defer ts.Close()
	n, _ := DialNode(&Config{GRPCAddr: "127.0.0.1:1", RPCAddr: ts.URL})
	ctx := context.Background()
	if l, e, c, err := n.Status(ctx); err != nil || l != 190000000 || e != 176946601 || c {
		t.Fatalf("status %d %d %v %v", l, e, c, err)
	}
	if hs, err := n.TxHashesAt(ctx, 100); err != nil || len(hs) != 2 || hs[1] != "BB" {
		t.Fatalf("tx_search %v %v", hs, err)
	}
	if ok, err := n.TxIndexed(ctx, "BB"); err != nil || ok {
		t.Fatalf("tx not found: ok=%v err=%v", ok, err)
	}
	if ok, err := n.BlockIndexed(ctx, 5); err != nil || ok {
		t.Fatalf("block_search: ok=%v err=%v", ok, err)
	}
}

type hungPruning struct {
	pruningv1.UnimplementedPruningServiceServer
}

func (*hungPruning) GetTxIndexerRetainHeight(ctx context.Context, _ *pruningv1.GetTxIndexerRetainHeightRequest) (*pruningv1.GetTxIndexerRetainHeightResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// A hung privileged gRPC server must not stall the pacer forever.
func TestGRPCCallsHaveDeadline(t *testing.T) {
	defer func(d time.Duration) { rpcTimeout = d }(rpcTimeout)
	rpcTimeout = 100 * time.Millisecond

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	pruningv1.RegisterPruningServiceServer(srv, &hungPruning{})
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()
	n, err := DialNode(&Config{GRPCAddr: lis.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, _, err := n.GetRetain(context.Background(), RetainTxIndexer); done <- err }()
	select {
	case err := <-done:
		if status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("want DeadlineExceeded, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("GetRetain still blocked after 3s")
	}
}

// tx_search returns at most 100 txs per page (CometBFT maxPerPage). With 150
// txs at one height the witness must still hold all of them, or waitGone can
// finish while 50 are left.
func TestTxHashesAtReadsAllPages(t *testing.T) {
	const n = 150
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		per, _ := strconv.Atoi(r.URL.Query().Get("per_page"))
		per = min(max(per, 1), 100)
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		page = max(page, 1)
		pages := (n + per - 1) / per
		if page > pages { // like CometBFT validatePage
			fmt.Fprintf(w, `{"error":{"code":-32603,"message":"Internal error","data":"page should be within [1, %d] range, given %d"}}`, pages, page)
			return
		}
		var hs []string
		for i := (page - 1) * per; i < min(page*per, n); i++ {
			hs = append(hs, fmt.Sprintf(`{"hash":"H%d"}`, i))
		}
		fmt.Fprintf(w, `{"result":{"txs":[%s],"total_count":"%d"}}`, strings.Join(hs, ","), n)
	}))
	defer ts.Close()
	node, _ := DialNode(&Config{GRPCAddr: "127.0.0.1:1", RPCAddr: ts.URL})
	hs, err := node.TxHashesAt(context.Background(), 1299)
	if err != nil || len(hs) != n || hs[n-1] != fmt.Sprintf("H%d", n-1) {
		t.Fatalf("got %d hashes, err=%v; want %d", len(hs), err, n)
	}
}

// RPC errors must say what happened: a proxy's HTML 502 used to surface as a
// bare JSON parse error, and "Method not found" (empty data) as "".
func TestRPCErrorsAreDescriptive(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/status":
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte("<html>502 Bad Gateway</html>"))
		case "/block_search":
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":-1,"error":{"code":-32601,"message":"Method not found","data":""}}`))
		}
	}))
	defer ts.Close()
	n, _ := DialNode(&Config{GRPCAddr: "127.0.0.1:1", RPCAddr: ts.URL})
	ctx := context.Background()
	if _, _, _, err := n.Status(ctx); err == nil || !strings.Contains(err.Error(), "502") {
		t.Errorf("Status on HTML 502: %v", err)
	}
	if _, err := n.BlockIndexed(ctx, 5); err == nil || !strings.Contains(err.Error(), "Method not found") {
		t.Errorf("block_search Method not found: %v", err)
	}
}

// A non-numeric height must be an error, not latest=0 (which looked like
// "ceiling reached").
func TestStatusBadHeightIsError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"result":{"sync_info":{"latest_block_height":"","earliest_block_height":"1"}}}`))
	}))
	defer ts.Close()
	n, _ := DialNode(&Config{GRPCAddr: "127.0.0.1:1", RPCAddr: ts.URL})
	if l, _, _, err := n.Status(context.Background()); err == nil {
		t.Fatalf("latest=%d err=nil for an empty height", l)
	}
}
