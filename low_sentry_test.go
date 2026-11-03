package main

import (
	"strings"
	"testing"
)

// A malformed DSN must not be echoed: its user part is the project key.
func TestBadDSNNotEchoed(t *testing.T) {
	t.Setenv("SENTRY_DSN", "https://s3cr3tkey@o123.ingest.sentry.io:abc/456")
	_, _, err := initSentry()
	if err == nil {
		t.Fatal("want an error for a malformed DSN")
	}
	if strings.Contains(err.Error(), "s3cr3tkey") {
		t.Fatalf("error leaks the DSN key: %v", err)
	}
}
