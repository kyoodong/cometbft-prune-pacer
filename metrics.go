package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// metrics are exposed on --listen for an existing scraper (e.g. Grafana
// Alloy) to pick up. Every method is safe on a nil receiver so the pacer
// does not have to check whether metrics are enabled.
type metrics struct {
	reg         *prometheus.Registry
	latest      prometheus.Gauge
	ceiling     prometheus.Gauge
	target      prometheus.Gauge
	backlog     prometheus.Gauge
	memAvail    prometheus.Gauge
	inProgress  prometheus.Gauge
	stuck       prometheus.Gauge
	paused      *prometheus.GaugeVec
	steps       prometheus.Counter
	emptySteps  prometheus.Counter
	errors      prometheus.Counter
	stepSeconds prometheus.Histogram
}

func newMetrics() *metrics {
	g := func(name, help string) prometheus.Gauge {
		return prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "pacer", Name: name, Help: help})
	}
	c := func(name, help string) prometheus.Counter {
		return prometheus.NewCounter(prometheus.CounterOpts{Namespace: "pacer", Name: name, Help: help})
	}
	m := &metrics{
		reg:        prometheus.NewRegistry(),
		latest:     g("latest_height", "Latest block height reported by the node."),
		ceiling:    g("ceiling_height", "Highest allowed retain height: latest - min-retain."),
		target:     g("target_height", "Indexer retain height most recently set."),
		backlog:    g("backlog_blocks", "Blocks between the indexer retain height and the ceiling."),
		memAvail:   g("mem_available_bytes", "MemAvailable seen at the last step decision."),
		inProgress: g("step_in_progress", "1 while waiting for a step's witnesses to disappear."),
		stuck:      g("step_stuck", "1 when the current step exceeded --step-timeout."),
		paused: prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "pacer", Name: "paused",
			Help: "1 while stepping is paused for the given reason."}, []string{"reason"}),
		steps:      c("steps_total", "Completed pruning steps."),
		emptySteps: c("empty_steps_total", "Steps whose range had no indexed txs."),
		errors:     c("errors_total", "Loop iterations that ended in an error."),
		stepSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: "pacer", Name: "step_duration_seconds",
			Help:    "Time from Set until the step's witnesses were gone.",
			// From the default --poll (30s; nothing is observed sooner) to a day,
			// so steps beyond the default --step-timeout (2h) are not all +Inf.
			Buckets: []float64{30, 60, 120, 300, 600, 1200, 1800, 3600, 7200, 14400, 28800, 86400}}),
	}
	m.reg.MustRegister(m.latest, m.ceiling, m.target, m.backlog, m.memAvail, m.inProgress, m.stuck,
		m.paused, m.steps, m.emptySteps, m.errors, m.stepSeconds)
	for _, r := range []string{"catching_up", "low_memory", "ceiling"} {
		m.paused.WithLabelValues(r).Set(0)
	}
	return m
}

// serve exposes /metrics until ctx is cancelled.
func (m *metrics) serve(ctx context.Context, addr string, log *slog.Logger) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{}))
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics_listen_failed", "addr", addr, "err", err.Error())
		}
	}()
}

func (m *metrics) observeHeights(latest, ceiling int64) {
	if m == nil {
		return
	}
	m.latest.Set(float64(latest))
	m.ceiling.Set(float64(ceiling))
}

func (m *metrics) observeTarget(target, ceiling int64) {
	if m == nil {
		return
	}
	m.target.Set(float64(target))
	m.backlog.Set(float64(max(ceiling-target, 0)))
}

func (m *metrics) setPaused(reason string) {
	if m == nil {
		return
	}
	for _, r := range []string{"catching_up", "low_memory", "ceiling"} {
		v := 0.0
		if r == reason {
			v = 1
		}
		m.paused.WithLabelValues(r).Set(v)
	}
}

func (m *metrics) setMemAvail(b uint64) {
	if m != nil {
		m.memAvail.Set(float64(b))
	}
}

func (m *metrics) stepStarted() {
	if m != nil {
		m.inProgress.Set(1)
		m.stuck.Set(0)
	}
}

func (m *metrics) stepStuck() {
	if m != nil {
		m.stuck.Set(1)
	}
}

func (m *metrics) stepDone(d time.Duration, empty, count bool) {
	if m == nil {
		return
	}
	m.inProgress.Set(0)
	m.stuck.Set(0)
	if !count {
		return
	}
	m.steps.Inc()
	if empty {
		m.emptySteps.Inc()
	}
	m.stepSeconds.Observe(d.Seconds())
}

func (m *metrics) loopError() {
	if m != nil {
		m.errors.Inc()
	}
}
