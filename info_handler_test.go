package main

import (
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/getsentry/sentry-go"
)

// Values reach Sentry readable: an error as its text (JSON would make it {}),
// and attributes under WithGroup keep their group as a key prefix.
func TestSentryHandlerFieldValues(t *testing.T) {
	var got map[string]any
	h := newSentryHandler(slog.NewTextHandler(io.Discard, nil))
	h.send = func(_ sentry.Level, _ string, f map[string]any) { got = f }
	log := slog.New(h)

	log.Warn("a", "err", errors.New("boom"))
	if got["err"] != "boom" {
		t.Errorf("err field = %#v, want \"boom\"", got["err"])
	}
	log.WithGroup("node").Warn("b", "height", 5)
	if got["node.height"] != int64(5) {
		t.Errorf("grouped field: %#v", got)
	}
}
