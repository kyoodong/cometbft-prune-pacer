package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

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
	n, _ := dialNode(&Config{GRPCAddr: "127.0.0.1:1", RPCAddr: ts.URL})
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
	n, _ := dialNode(&Config{GRPCAddr: "127.0.0.1:1", RPCAddr: ts.URL})
	if l, _, _, err := n.Status(context.Background()); err == nil {
		t.Fatalf("latest=%d err=nil for an empty height", l)
	}
}
