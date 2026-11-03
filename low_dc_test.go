package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"

	pruningv1 "github.com/cometbft/cometbft/api/cometbft/services/pruning/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// dcOffPruning answers like a node with data companion pruning disabled: the
// companion block retain key was never initialised (node.go
// initCompanionRetainHeights runs only when it is enabled).
type dcOffPruning struct {
	pruningv1.UnimplementedPruningServiceServer
}

func (dcOffPruning) GetBlockRetainHeight(context.Context, *pruningv1.GetBlockRetainHeightRequest) (*pruningv1.GetBlockRetainHeightResponse, error) {
	return nil, status.Error(codes.Internal, "Failed to get companion block retain height (see logs for trace ID: x)")
}

// Without --home the config files are not read, so the gRPC answer is the
// only sign that the node will never prune its indexes.
func TestDataCompanionOffDetectedWithoutHome(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	pruningv1.RegisterPruningServiceServer(srv, &dcOffPruning{})
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	cfg := &Config{GRPCAddr: lis.Addr().String(), RPCAddr: "http://127.0.0.1:1", MaxStep: 100}
	n, err := dialNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	err = preflight(context.Background(), cfg, n, slog.New(slog.NewTextHandler(io.Discard, nil)))
	var cf *checkFailure
	if !errors.As(err, &cf) || cf.ID != "C3" {
		t.Fatalf("want C3 failure, got %v", err)
	}
}
