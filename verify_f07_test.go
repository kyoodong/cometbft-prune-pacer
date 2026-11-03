package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	pruningv1 "github.com/cometbft/cometbft/api/cometbft/services/pruning/v1"
	"google.golang.org/grpc"
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
		n := &liveNode{rpc: addr, metrics: addr + "/metrics", http: &http.Client{Timeout: time.Second}}
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

// blockRetainOnly answers the C1/C2 probe so preflight gets past gRPC.
type blockRetainOnly struct {
	pruningv1.UnimplementedPruningServiceServer
}

func (*blockRetainOnly) GetBlockRetainHeight(context.Context, *pruningv1.GetBlockRetainHeightRequest) (*pruningv1.GetBlockRetainHeightResponse, error) {
	return &pruningv1.GetBlockRetainHeightResponse{}, nil
}

// TestF07RunWithHostPortAddr drives the real entry point run() past C1/C2
// with a reachable gRPC server and a host:port --rpc-addr / --metrics-addr.
// run must return an error (exit 1), not panic (exit 2).
func TestF07RunWithHostPortAddr(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	pruningv1.RegisterPruningServiceServer(srv, &blockRetainOnly{})
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()
	t.Setenv("SENTRY_DSN", "")

	base := []string{"--max-step", "300", "--min-retain", "1000", "--listen", "", "--grpc-addr", lis.Addr().String()}
	cases := map[string][]string{
		"rpc-addr host:port":     {"--rpc-addr", "127.0.0.1:26657", "--metrics-addr", ""},
		"metrics-addr host:port": {"--rpc-addr", "http://127.0.0.1:1", "--metrics-addr", "127.0.0.1:26660/metrics"},
	}
	for name, extra := range cases {
		noPanic(t, "run "+name, func() {
			err := run(append(append([]string{}, base...), extra...))
			t.Logf("%s: run returned %v", name, err)
			if err == nil {
				t.Errorf("%s: want error", name)
			}
		})
	}
}
