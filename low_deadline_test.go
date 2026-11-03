package main

import (
	"context"
	"net"
	"testing"
	"time"

	pruningv1 "github.com/cometbft/cometbft/api/cometbft/services/pruning/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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
	n, err := dialNode(&Config{GRPCAddr: lis.Addr().String()})
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
