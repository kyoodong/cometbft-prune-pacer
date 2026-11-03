package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"

	pruningv1 "github.com/cometbft/cometbft/api/cometbft/services/pruning/v1"
	"google.golang.org/grpc"
)

// blockRetainOnly answers the C1/C2 probe so preflight gets past gRPC.
type blockRetainOnly struct {
	pruningv1.UnimplementedPruningServiceServer
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

// f10Pruning is a privileged gRPC PruningService whose indexer retain
// heights are already far above the lowest indexed height (C6 fails).
type f10Pruning struct {
	pruningv1.UnimplementedPruningServiceServer
}

func (f10Pruning) GetBlockRetainHeight(context.Context, *pruningv1.GetBlockRetainHeightRequest) (*pruningv1.GetBlockRetainHeightResponse, error) {
	return &pruningv1.GetBlockRetainHeightResponse{}, nil
}

func (f10Pruning) GetTxIndexerRetainHeight(context.Context, *pruningv1.GetTxIndexerRetainHeightRequest) (*pruningv1.GetTxIndexerRetainHeightResponse, error) {
	return &pruningv1.GetTxIndexerRetainHeightResponse{Height: 4000}, nil
}

func (f10Pruning) GetBlockIndexerRetainHeight(context.Context, *pruningv1.GetBlockIndexerRetainHeightRequest) (*pruningv1.GetBlockIndexerRetainHeightResponse, error) {
	return &pruningv1.GetBlockIndexerRetainHeightResponse{Height: 4000}, nil
}

type f10Event struct {
	Message     string            `json:"message"`
	Fingerprint []string          `json:"fingerprint"`
	Tags        map[string]string `json:"tags"`
}

// f10Ingest is a fake Sentry ingest endpoint that records every event the
// real sentry-go HTTP transport delivers.
type f10Ingest struct {
	mu     sync.Mutex
	events []f10Event
}

func (s *f10Ingest) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var body io.Reader = r.Body
	if r.Header.Get("Content-Encoding") == "gzip" {
		zr, err := gzip.NewReader(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		body = zr
	}
	sc := bufio.NewScanner(body)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	s.mu.Lock()
	defer s.mu.Unlock()
	for sc.Scan() {
		var ev f10Event
		if json.Unmarshal(sc.Bytes(), &ev) == nil && ev.Message != "" {
			s.events = append(s.events, ev)
		}
	}
	w.WriteHeader(http.StatusOK)
}

// f10Child runs the real main() in a subprocess so the test sees the real
// exit code. It only does something when started by the F10 test.
func TestVerifyF10Child(t *testing.T) {
	if os.Getenv("F10_CHILD") != "1" {
		t.Skip("helper process for TestVerifyF10CheckFailedAcrossRestarts")
	}
	os.Args = append([]string{"cometbft-prune-pacer", "run"}, strings.Split(os.Getenv("F10_ARGS"), "\x1f")...)
	main()
	os.Exit(0)
}

// f10PreventExitStatus reads RestartPreventExitStatus= from the README
// systemd unit, the restart policy operators deploy.
func f10PreventExitStatus(t *testing.T) map[int]bool {
	b, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	prevent := map[int]bool{}
	for _, line := range strings.Split(string(b), "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), "RestartPreventExitStatus="); ok {
			for _, f := range strings.Fields(v) {
				if n, err := strconv.Atoi(f); err == nil {
					prevent[n] = true
				}
			}
		}
	}
	return prevent
}

// F10: the node is up but C6 fails, so every start exits non-zero and
// systemd (README unit: Restart=on-failure, RestartSec=60) starts the daemon
// again. Emulate systemd for five restart slots (five minutes) around the
// real binary entry point and count what reaches Sentry. README promises
// "Each event is reported at most once per hour".
func TestVerifyF10CheckFailedAcrossRestarts(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	pruningv1.RegisterPruningServiceServer(gs, &f10Pruning{})
	go func() { _ = gs.Serve(lis) }()
	defer gs.Stop()

	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/status":
			fmt.Fprint(w, `{"result":{"sync_info":{"latest_block_height":"5000","earliest_block_height":"1"}}}`)
		case "/block_search":
			var h int64
			_, _ = fmt.Sscanf(r.URL.Query().Get("query"), `"block.height=%d"`, &h)
			total := "0"
			if h >= 1000 { // lowest indexed 1000, retain 4000: 3000 > max-step 300
				total = "1"
			}
			fmt.Fprintf(w, `{"result":{"total_count":%q}}`, total)
		default:
			http.NotFound(w, r) // node metrics unreachable -> C5 check_warn
		}
	}))
	defer rpc.Close()

	ing := &f10Ingest{}
	sentrySrv := httptest.NewServer(ing)
	defer sentrySrv.Close()

	args := []string{"--max-step", "300", "--min-retain", "1000", "--listen", "",
		"--grpc-addr", lis.Addr().String(), "--rpc-addr", rpc.URL, "--metrics-addr", rpc.URL + "/metrics"}
	prevent := f10PreventExitStatus(t)
	const slots = 5
	var codes []int
	for i := 0; i < slots; i++ {
		cmd := exec.Command(os.Args[0], "-test.run=^TestVerifyF10Child$")
		cmd.Env = append(os.Environ(), "F10_CHILD=1", "F10_ARGS="+strings.Join(args, "\x1f"),
			"SENTRY_DSN=http://public@"+strings.TrimPrefix(sentrySrv.URL, "http://")+"/1")
		out, err := cmd.CombinedOutput()
		code := cmd.ProcessState.ExitCode()
		codes = append(codes, code)
		if i == 0 {
			t.Logf("first start (err=%v):\n%s", err, out)
		}
		if !strings.Contains(string(out), "[C6]") { // guard: each start really fails on C6
			t.Fatalf("start %d: want C6 failure, got exit %d:\n%s", i, code, out)
		}
		if code == 0 || prevent[code] { // Restart=on-failure + RestartPreventExitStatus=
			break
		}
	}

	ing.mu.Lock()
	defer ing.mu.Unlock()
	count := map[string]int{} // per fingerprint: different checks are different issues
	for _, e := range ing.events {
		count[strings.Join(e.Fingerprint, " ")]++
		t.Logf("sentry event: %s fingerprint=%v check=%s", e.Message, e.Fingerprint, e.Tags["check"])
	}
	t.Logf("exit codes %v, RestartPreventExitStatus=%v -> sentry events %v", codes, prevent, count)
	if count["check_failed C6"] == 0 { // guard: the fake ingest really receives events
		t.Fatalf("no check_failed reached the fake ingest; test is vacuous")
	}
	// Correct behaviour per README: at most one report per event per hour.
	for k, n := range count {
		if n > 1 {
			t.Errorf("%s sent %d times across restarts, want at most 1: %v", k, n, count)
		}
	}
}

// C1 (node not reachable) exits 1 and is retried every RestartSec; it must
// not reach Sentry, or a down node sends one event per restart.
func TestC1NotReportedToSentry(t *testing.T) {
	ing := &f10Ingest{}
	sentrySrv := httptest.NewServer(ing)
	defer sentrySrv.Close()

	args := []string{"--max-step", "300", "--min-retain", "1000", "--listen", "", "--grpc-addr", "127.0.0.1:1"}
	cmd := exec.Command(os.Args[0], "-test.run=^TestVerifyF10Child$")
	cmd.Env = append(os.Environ(), "F10_CHILD=1", "F10_ARGS="+strings.Join(args, "\x1f"),
		"SENTRY_DSN=http://public@"+strings.TrimPrefix(sentrySrv.URL, "http://")+"/1")
	out, _ := cmd.CombinedOutput()
	if code := cmd.ProcessState.ExitCode(); code != 1 || !strings.Contains(string(out), "[C1]") {
		t.Fatalf("want C1 failure with exit 1, got exit %d:\n%s", code, out)
	}
	ing.mu.Lock()
	defer ing.mu.Unlock()
	if len(ing.events) != 0 {
		t.Fatalf("C1 reached Sentry: %+v", ing.events)
	}
}
