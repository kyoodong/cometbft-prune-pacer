package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/getsentry/sentry-go"
)

// sentryThrottle limits repeated reports of the same event (for example
// paused_low_memory, logged on every poll) to one per window.
const sentryThrottle = time.Hour

// initSentry enables Sentry when SENTRY_DSN is set. It returns a flush
// function that is safe to call when Sentry is disabled.
func initSentry() (enabled bool, flush func(), err error) {
	dsn := os.Getenv("SENTRY_DSN")
	if dsn == "" {
		return false, func() {}, nil
	}
	host, _ := os.Hostname()
	err = sentry.Init(sentry.ClientOptions{
		Dsn:         dsn,
		Environment: os.Getenv("SENTRY_ENVIRONMENT"),
		Release:     "cometbft-prune-pacer@" + version,
		ServerName:  host,
	})
	if err != nil {
		return false, func() {}, fmt.Errorf("sentry init: %w", err)
	}
	return true, func() { sentry.Flush(5 * time.Second) }, nil
}

// sentryHandler forwards Warn and Error log records to Sentry and passes
// every record on to the wrapped handler. Normal progress (Info) stays in
// the logs only.
type sentryHandler struct {
	next  slog.Handler
	attrs []slog.Attr
	mu    *sync.Mutex
	last  map[string]time.Time
	now   func() time.Time
	send  func(level sentry.Level, msg string, fields map[string]any)
}

func newSentryHandler(next slog.Handler) *sentryHandler {
	return &sentryHandler{
		next: next,
		mu:   &sync.Mutex{},
		last: map[string]time.Time{},
		now:  time.Now,
		send: captureSentry,
	}
}

func (h *sentryHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.next.Enabled(ctx, l)
}

func (h *sentryHandler) Handle(ctx context.Context, r slog.Record) error {
	err := h.next.Handle(ctx, r)
	if r.Level < slog.LevelWarn {
		return err
	}
	if !h.allow(r.Message) {
		return err
	}
	fields := map[string]any{}
	for _, a := range h.attrs {
		fields[a.Key] = a.Value.Any()
	}
	r.Attrs(func(a slog.Attr) bool {
		fields[a.Key] = a.Value.Any()
		return true
	})
	level := sentry.LevelWarning
	if r.Level >= slog.LevelError {
		level = sentry.LevelError
	}
	h.send(level, r.Message, fields)
	return err
}

func (h *sentryHandler) allow(event string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	now := h.now()
	if t, ok := h.last[event]; ok && now.Sub(t) < sentryThrottle {
		return false
	}
	h.last[event] = now
	return true
}

func (h *sentryHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	c := *h
	c.next = h.next.WithAttrs(attrs)
	c.attrs = append(append([]slog.Attr{}, h.attrs...), attrs...)
	return &c
}

func (h *sentryHandler) WithGroup(name string) slog.Handler {
	c := *h
	c.next = h.next.WithGroup(name)
	return &c
}

func captureSentry(level sentry.Level, msg string, fields map[string]any) {
	sentry.WithScope(func(s *sentry.Scope) {
		s.SetLevel(level)
		s.SetTag("event", msg)
		if id, ok := fields["id"].(string); ok {
			s.SetTag("check", id)
		}
		s.SetContext("fields", fields)
		// Group by event name so a changing height does not split issues.
		s.SetFingerprint([]string{msg})
		sentry.CaptureMessage(msg)
	})
}

// reportCheckFailure sends a fatal startup check to Sentry before exit.
func reportCheckFailure(cf *checkFailure) {
	captureSentry(sentry.LevelError, "check_failed", map[string]any{"id": cf.ID, "what": cf.What, "fix": cf.Fix})
}
