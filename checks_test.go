package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pruningv1 "github.com/cometbft/cometbft/api/cometbft/services/pruning/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// C6: a retain height far above the lowest indexed height must stop startup.
func TestPreflightRejectsLargeExistingTarget(t *testing.T) {
	f := newFakeNode(1000, 5000, 0, 0)
	f.retain[RetainTxIndexer] = 3000
	cfg := &Config{MaxStep: 300, MinRetain: 1000}
	err := preflight(context.Background(), cfg, f, slog.New(slog.NewTextHandler(io.Discard, nil)))
	var cf *checkFailure
	if !errors.As(err, &cf) || cf.ID != "C6" {
		t.Fatalf("want C6 failure, got %v", err)
	}
}

// F3 (mechanism): C6 must stop startup when the tx indexer retain height is
// far above the lowest indexed height, even if the block indexer retain
// height is low. indexTarget returns min(tx, block), which hides it.
func TestVerifyF03PreflightCatchesLargeTxRetain(t *testing.T) {
	f := newFakeNode(1000, 5000, 0, 0)
	f.retain[RetainTxIndexer], f.retain[RetainBlockIndexer] = 3000, 1000
	cfg := &Config{MaxStep: 300, MinRetain: 1000}
	err := preflight(context.Background(), cfg, f, slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Logf("preflight tx=3000 blk=1000 low=1000 max-step=300: err=%v", err)
	var cf *checkFailure
	if !errors.As(err, &cf) || cf.ID != "C6" {
		t.Fatalf("want C6 failure, got %v", err)
	}
}

// F3 (severity): with tx retain already at 3000 the node's own index pruner
// (state/pruner.go pruneIndexesRoutine: tx Prune first, then block Prune, in
// one goroutine, re-run on every node start) deletes [1,3000) by itself.
// The question is whether running the pacer on top makes any single Prune
// larger than the node alone would. Compare node-only vs node+pacer.
func TestVerifyF03PacerDoesNotWorsenLargeTxRetain(t *testing.T) {
	run := func(withPacer, nodePrunedBeforeStart bool) *cometPruner {
		f := newFakeNode(1000, 5000, 0, 0)
		f.retain[RetainTxIndexer], f.retain[RetainBlockIndexer] = 3000, 1000
		c := &cometPruner{fakeNode: f}
		if nodePrunedBeforeStart {
			c.prune() // the node's pruner already ran its first tick
		}
		if !withPacer {
			for i := 0; i < 50; i++ {
				c.prune()
			}
			return c
		}
		p := testPacer(f, 300, 1000)
		p.node = c
		p.sleep = func(ctx context.Context, _ time.Duration) error { c.prune(); return ctx.Err() }
		ctx := context.Background()
		if err := p.resume(ctx); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 15; i++ {
			if err := p.step(ctx); err != nil {
				t.Fatal(err)
			}
		}
		return c
	}

	for _, pre := range []bool{true, false} {
		node := run(false, pre)
		both := run(true, pre)
		t.Logf("nodePrunedBeforeStart=%v: max tx heights per Prune node-only=%d node+pacer=%d; pacer tx Sets=%v; final retain tx=%d blk=%d",
			pre, node.maxTxHeightsPerPrune, both.maxTxHeightsPerPrune, both.setHistory,
			both.retain[RetainTxIndexer], both.retain[RetainBlockIndexer])
		if both.retain[RetainBlockIndexer] <= 3000 || len(both.setHistory) == 0 {
			t.Fatalf("vacuous: pacer did not step past the large tx retain (blk=%d, tx sets=%v)",
				both.retain[RetainBlockIndexer], both.setHistory)
		}
		if both.maxTxHeightsPerPrune > node.maxTxHeightsPerPrune {
			t.Fatalf("pacer made a single Prune larger: %d > node-only %d",
				both.maxTxHeightsPerPrune, node.maxTxHeightsPerPrune)
		}
		prev := int64(3000)
		for _, h := range both.setHistory {
			if h-prev > 300 {
				t.Fatalf("pacer raised tx retain %d -> %d (> max-step 300)", prev, h)
			}
			prev = h
		}
	}
}

// lowHome writes a node home whose config.toml passes C3/C4.
func lowHome(t *testing.T, laddr, appToml string) string {
	t.Helper()
	home := t.TempDir()
	cfg := filepath.Join(home, "config")
	if err := os.MkdirAll(cfg, 0o755); err != nil {
		t.Fatal(err)
	}
	cc := "[tx_index]\nindexer = \"kv\"\n[storage.pruning.data_companion]\nenabled = true\n[grpc.privileged]\nladdr = \"" + laddr + "\"\n"
	if err := os.WriteFile(filepath.Join(cfg, "config.toml"), []byte(cc), 0o644); err != nil {
		t.Fatal(err)
	}
	if appToml != "" {
		if err := os.WriteFile(filepath.Join(cfg, "app.toml"), []byte(appToml), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return home
}

// C7 is informational: an app.toml it cannot decode must not stop the daemon.
func TestC7UnreadableAppTomlOnlyWarns(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	for name, app := range map[string]string{
		"string value": "min-retain-blocks = \"100\"\n",
		"bad toml":     "min-retain-blocks = \n",
	} {
		if err := checkConfigFiles(lowHome(t, "tcp://127.0.0.1:26659", app), log); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// The privileged gRPC exposure warning must parse the address, not match
// substrings.
func TestLocalLaddr(t *testing.T) {
	for l, want := range map[string]bool{
		"tcp://127.0.0.1:26659":              true,
		"tcp://localhost:26659":              true,
		"tcp://[::1]:26659":                  true,
		"unix:///var/run/privileged.sock":    true,
		"127.0.0.1:26659":                    true,
		"tcp://0.0.0.0:26659":                false,
		"tcp://10.0.0.5:26659":               false,
		"tcp://127.0.0.1.evil.example:26659": false,
	} {
		if got := localLaddr(l); got != want {
			t.Errorf("localLaddr(%q) = %v, want %v", l, got, want)
		}
	}
}

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
