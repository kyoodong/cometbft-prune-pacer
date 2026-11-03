package main

import (
	"context"
	"log/slog"
	"strings"
	"testing"
)

// /metrics has no authentication; serving it beyond loopback is worth a warning.
func TestListenNotLoopbackWarns(t *testing.T) {
	var logs strings.Builder
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	newMetrics().serve(ctx, "0.0.0.0:0", slog.New(slog.NewTextHandler(&logs, nil)))
	if !strings.Contains(logs.String(), "metrics_listen_not_loopback") {
		t.Fatalf("no warning:\n%s", logs.String())
	}
}
