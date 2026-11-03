package main

import (
	"context"
	"log/slog"
	"strings"
	"testing"
)

// Arguments left after the flags used to be ignored: "--max-step 5 run"
// dropped "run", and a mistyped subcommand stopped flag parsing silently.
func TestLeftoverArgsRejected(t *testing.T) {
	for _, args := range [][]string{
		{"--max-step", "5", "--min-retain", "5", "run"},
		{"rnu", "--max-step", "5", "--min-retain", "5"},
	} {
		if _, err := parseConfig(args); err == nil {
			t.Errorf("%q accepted", args)
		}
	}
}

// Operators copy laddr from config.toml ("tcp://127.0.0.1:26659"); gRPC
// rejected it with "too many colons in address" and the pacer reported C1.
func TestGRPCAddrAcceptsTCPScheme(t *testing.T) {
	c, err := parseConfig([]string{"--max-step", "5", "--min-retain", "5", "--grpc-addr", "tcp://127.0.0.1:26659"})
	if err != nil || c.GRPCAddr != "127.0.0.1:26659" {
		t.Fatalf("GRPCAddr %q err=%v", c.GRPCAddr, err)
	}
}

// The privileged gRPC has no authentication: a remote --grpc-addr is worth a warning.
func TestRemoteGRPCAddrWarns(t *testing.T) {
	var logs strings.Builder
	cfg := &Config{MaxStep: 300, MinRetain: 1000, GRPCAddr: "10.0.0.5:26659"}
	if err := preflight(context.Background(), cfg, newFakeNode(1000, 5000, 0, 0), slog.New(slog.NewTextHandler(&logs, nil))); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), "is not loopback") {
		t.Fatalf("no warning for a remote --grpc-addr:\n%s", logs.String())
	}
}
