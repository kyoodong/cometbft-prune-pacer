package main

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

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
