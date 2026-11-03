package main

import (
	"strings"
	"testing"
)

// A malformed PACER_* value must fail startup, not fall back to a default:
// PACER_DRY_RUN=1 used to mean a real run.
func TestBadEnvValueFails(t *testing.T) {
	for k, v := range map[string]string{
		"PACER_DRY_RUN":      "yes",
		"PACER_START_HEIGHT": "1,000,000",
		"PACER_WAIT":         "abc",
		"PACER_MAX_STEP":     "abc",
	} {
		t.Run(k, func(t *testing.T) {
			t.Setenv("PACER_MAX_STEP", "100")
			t.Setenv("PACER_MIN_RETAIN", "100")
			t.Setenv(k, v)
			_, err := parseConfig(nil)
			if err == nil || !strings.Contains(err.Error(), k) {
				t.Fatalf("%s=%q: want an error naming %s, got %v", k, v, k, err)
			}
		})
	}
}

func TestDryRunEnvBool(t *testing.T) {
	for v, want := range map[string]bool{"1": true, "true": true, "0": false, "false": false} {
		t.Setenv("PACER_MAX_STEP", "100")
		t.Setenv("PACER_MIN_RETAIN", "100")
		t.Setenv("PACER_DRY_RUN", v)
		c, err := parseConfig(nil)
		if err != nil || c.DryRun != want {
			t.Fatalf("PACER_DRY_RUN=%s: DryRun=%v err=%v, want %v", v, c != nil && c.DryRun, err, want)
		}
	}
}
