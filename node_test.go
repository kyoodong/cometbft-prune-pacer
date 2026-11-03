package main

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	pruningv1 "github.com/cometbft/cometbft/api/cometbft/services/pruning/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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
	go srv.Serve(lis)
	defer srv.Stop()

	n, err := dialNode(&Config{GRPCAddr: lis.Addr().String()})
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
			w.Write([]byte(`{"result":{"sync_info":{"latest_block_height":"190000000","earliest_block_height":"176946601","catching_up":false}}}`))
		case "/tx_search":
			if r.URL.Query().Get("query") != `"tx.height=100"` {
				t.Errorf("query %q", r.URL.Query().Get("query"))
			}
			w.Write([]byte(`{"result":{"txs":[{"hash":"AA"},{"hash":"BB"}],"total_count":"2"}}`))
		case "/tx":
			w.Write([]byte(`{"error":{"code":-32603,"message":"Internal error","data":"tx (BB) not found"}}`))
		case "/block_search":
			w.Write([]byte(`{"result":{"blocks":[],"total_count":"0"}}`))
		}
	}))
	defer ts.Close()
	n, _ := dialNode(&Config{GRPCAddr: "127.0.0.1:1", RPCAddr: ts.URL})
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
