// Package runinfo identifies one run of one service: run id, build, host. It
// feeds the structured logger, the build_info metrics and the run manifest, so
// a log line, a metric series and a manifest can be tied to the same run after
// the fact. It lives at the non-deterministic edge (wall clock, environment).
package runinfo

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Overridable at link time: -ldflags "-X manifester/internal/sys/runinfo.GitCommit=abc123".
var (
	GitCommit = ""
	Version   = ""
)

// EnvRunID names the environment variable that supplies the run id.
const EnvRunID = "MANIFESTER_RUN_ID"

// Info identifies one process.
type Info struct {
	RunID      string    `json:"run_id"`
	Service    string    `json:"service"`
	BrokerID   int       `json:"broker_id"` // -1: not a broker
	Version    string    `json:"version"`
	GitCommit  string    `json:"git_sha"`
	GitDirty   bool      `json:"git_dirty"`
	GoVersion  string    `json:"go_version"`
	Started    time.Time `json:"started_at"`
	Hostname   string    `json:"hostname"`
	RunIDFresh bool      `json:"run_id_generated"` // true: nobody supplied a run id
}

// New builds the Info for service. runID is the -run-id flag; empty falls back
// to $MANIFESTER_RUN_ID, then to a generated "run-<utc time>-<hex>".
func New(service, runID string, brokerID int) Info {
	i := Info{Service: service, BrokerID: brokerID, Started: time.Now(), GoVersion: runtime.Version()}
	if runID == "" {
		runID = os.Getenv(EnvRunID)
	}
	if runID == "" {
		var b [4]byte
		_, _ = rand.Read(b[:])
		runID = fmt.Sprintf("run-%s-%s", i.Started.UTC().Format("20060102T150405Z"), hex.EncodeToString(b[:]))
		i.RunIDFresh = true
	}
	i.RunID = runID
	i.Hostname, _ = os.Hostname()
	i.Version, i.GitCommit, i.GitDirty = build()
	return i
}

func build() (string, string, bool) {
	version, sha := Version, GitCommit
	var dirty bool
	if bi, ok := debug.ReadBuildInfo(); ok {
		if version == "" && bi.Main.Version != "" {
			version = bi.Main.Version
		}
		for _, s := range bi.Settings {
			switch s.Key {
			case "vcs.revision":
				if sha == "" {
					sha = s.Value
				}
			case "vcs.modified":
				dirty = s.Value == "true"
			}
		}
	}
	if version == "" {
		version = "unknown"
	}
	if sha == "" {
		sha = "unknown"
	}
	return version, sha, dirty
}

// ParseLevel maps debug|info|warn|error to a slog level.
func ParseLevel(s string) (slog.Level, error) {
	var l slog.Level
	if err := l.UnmarshalText([]byte(strings.TrimSpace(s))); err != nil {
		return 0, fmt.Errorf("-log-level %q: want debug, info, warn or error", s)
	}
	return l, nil
}

// NewLogger writes JSON lines to w; every line carries run_id, service,
// version and git_sha (and broker_id for a broker).
func NewLogger(w io.Writer, level slog.Level, i Info) *slog.Logger {
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level})
	attrs := []any{"run_id", i.RunID, "service", i.Service}
	if i.BrokerID >= 0 {
		attrs = append(attrs, "broker_id", i.BrokerID)
	}
	attrs = append(attrs, "version", i.Version, "git_sha", i.GitCommit)
	return slog.New(h).With(attrs...)
}

// Register adds <ns>_build_info, <ns>_start_time_seconds and
// <ns>_uptime_seconds to reg.
func Register(reg prometheus.Registerer, ns string, i Info) {
	bi := prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Name: "build_info",
		Help:        "Always 1; the labels identify the build and the run.",
		ConstLabels: prometheus.Labels{"version": i.Version, "go_version": i.GoVersion, "git_sha": i.GitCommit, "run_id": i.RunID}})
	bi.Set(1)
	st := prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Name: "start_time_seconds", Help: "Process start, unix seconds."})
	st.Set(float64(i.Started.UnixNano()) / 1e9)
	up := prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: ns, Name: "uptime_seconds", Help: "Seconds since process start."},
		func() float64 { return time.Since(i.Started).Seconds() })
	reg.MustRegister(bi, st, up)
}

// HealthCheckHandler answers 200 "ok" (for the metrics ports).
func HealthCheckHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	_, _ = w.Write([]byte("ok\n"))
}

// NewRouter serves /metrics and /healthz, the two routes of a metrics port.
func NewRouter(metrics http.Handler) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics)
	mux.HandleFunc("/healthz", HealthCheckHandler)
	return mux
}

// ServeMetrics serves Mux(h) on addr in the background; empty addr is a no-op.
func ServeMetrics(ctx context.Context, log *slog.Logger, service, addr string, h http.Handler) error {
	if addr == "" {
		return nil
	}
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	srv := &http.Server{
		Addr:              addr,
		Handler:           NewRouter(h),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	go func() { _ = srv.Serve(ln) }()
	log.Info(service+" metrics", "addr", ln.Addr().String())
	return nil
}

// LogAndExit logs err and exits 1.
func LogAndExit(log *slog.Logger, err error) {
	log.Error("fatal", "err", err.Error())
	os.Exit(1)
}
