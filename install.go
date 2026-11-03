package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
)

const (
	binPath  = "/usr/local/bin/cometbft-prune-pacer"
	unitPath = "/etc/systemd/system/cometbft-prune-pacer.service"
)

// install copies this binary to binPath and writes a systemd unit that runs
// it with the given run options. Run options are validated first.
func install(args []string) error {
	user := new(string)
	// Pull --user out; everything else is passed through to "run".
	var runArgs []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--user" || a == "-user":
			if i+1 < len(args) {
				*user = args[i+1]
				i++
			}
		case strings.HasPrefix(a, "--user=") || strings.HasPrefix(a, "-user="):
			*user = a[strings.Index(a, "=")+1:]
		default:
			runArgs = append(runArgs, a)
		}
	}
	if *user == "" {
		return errors.New("install: --user is required")
	}
	if _, err := parseConfig(runArgs); err != nil {
		return fmt.Errorf("install: %w", err)
	}
	if os.Geteuid() != 0 {
		return errors.New("install: run as root")
	}

	self, err := os.Executable()
	if err != nil {
		return err
	}
	if err := copyFile(self, binPath, 0o755); err != nil {
		return err
	}
	unit := fmt.Sprintf(`[Unit]
Description=CometBFT prune pacer
After=network-online.target

[Service]
User=%s
ExecStart=%s run %s
Restart=on-failure
RestartSec=60

[Install]
WantedBy=multi-user.target
`, *user, binPath, strings.Join(runArgs, " "))
	if err := os.WriteFile(unitPath, []byte(unit), 0o644); err != nil {
		return err
	}
	if out, err := exec.Command("systemctl", "daemon-reload").CombinedOutput(); err != nil {
		return fmt.Errorf("systemctl daemon-reload: %v: %s", err, out)
	}
	fmt.Printf("installed %s and %s\nstart with: systemctl enable --now cometbft-prune-pacer\n", binPath, unitPath)
	return nil
}

func copyFile(src, dst string, mode os.FileMode) error {
	if src == dst {
		return nil
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	tmp := dst + ".tmp"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, dst)
}
