package main

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

type noMemNode struct{ *fakeNode }

func (noMemNode) MemAvailable() (uint64, error) { return 0, errors.New("no /proc/meminfo") }

// Without MemAvailable the --min-mem-avail floor is off; that must be said.
func TestMemAvailableUnreadableWarns(t *testing.T) {
	var logs strings.Builder
	cfg := &Config{MaxStep: 300, MinRetain: 1000, GRPCAddr: "127.0.0.1:26659"}
	n := noMemNode{newFakeNode(1000, 5000, 0, 0)}
	if err := preflight(context.Background(), cfg, n, slog.New(slog.NewTextHandler(&logs, nil))); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), "id=C9") {
		t.Fatalf("no C9 warning:\n%s", logs.String())
	}
}
