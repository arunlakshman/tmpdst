package main

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/controlplanemetrics"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/shared/runinfo"
)

// The tests here need no service. The runs against real DynamoDB, PostgreSQL
// and S3 are in test/integration, on the built binary.

func TestLogFlags(t *testing.T) {
	cases := []struct {
		name  string
		args  []string
		env   string // MDS_POSTGRES_DSN
		check func(t *testing.T, f cpFlags)
	}{
		{"dynamolog defaults", []string{"-store", kindLog}, "", func(t *testing.T, f cpFlags) {
			l := f.log
			if l.takeoverAfter != 5*time.Second || l.takeoverRankStep != time.Second ||
				l.checkpointEvery != 5*time.Minute || l.windowSlots != 131072 ||
				l.heartbeat != time.Second || l.tableName(f.prefix) != "mds_cp" || nodeName(l) == "" {
				t.Fatalf("defaults: %+v", l)
			}
		}},
		{"table override", []string{"-store", kindLog, "-table", "x"}, "", func(t *testing.T, f cpFlags) {
			if f.log.tableName(f.prefix) != "x" {
				t.Fatal("table override")
			}
		}},
		// -takeover-rank-step reaches the machine config as TakeoverRankStep.
		{"takeover rank step", []string{"-store", kindLog, "-takeover-rank-step", "250ms"}, "", func(t *testing.T, f cpFlags) {
			if f.log.takeoverRankStep != 250*time.Millisecond {
				t.Fatalf("flag value: %v", f.log.takeoverRankStep)
			}
			if got := machineConfig(f).TakeoverRankStep; got != runtime.Duration(250*time.Millisecond) {
				t.Fatalf("machine config: %v", got)
			}
		}},
		{"postgres defaults", []string{"-store", kindPostgres}, "", func(t *testing.T, f cpFlags) {
			if l := f.log; l.postgresPrefix != "mds_" || resolvedDSN(l) != "" {
				t.Fatalf("defaults: %+v", l)
			}
		}},
		// -table and -log-ttl are for dynamolog only; with postgres they are ignored.
		{"postgres flags", []string{"-store", kindPostgres, "-postgres-prefix", "x_", "-postgres-dsn", "postgres://u@h/db", "-table", "other", "-log-ttl", "1h"}, "",
			func(t *testing.T, f cpFlags) {
				if l := f.log; l.postgresPrefix != "x_" || resolvedDSN(l) != "postgres://u@h/db" {
					t.Fatalf("flags: %+v", l)
				}
			}},
		{"postgres env fallback", []string{"-store", kindPostgres}, "postgres://env@h/db", func(t *testing.T, f cpFlags) {
			if got := resolvedDSN(f.log); got != "postgres://env@h/db" {
				t.Fatalf("env fallback: %q", got)
			}
		}},
		{"postgres flag wins over env", []string{"-store", kindPostgres, "-postgres-dsn", "postgres://flag@h/db"}, "postgres://env@h/db", func(t *testing.T, f cpFlags) {
			if got := resolvedDSN(f.log); got != "postgres://flag@h/db" {
				t.Fatalf("the flag must win over the env: %q", got)
			}
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv(postgresDSNEnv, c.env)
			f, _, err := parseFlags(c.args, &bytes.Buffer{})
			if err != nil {
				t.Fatal(err)
			}
			c.check(t, f)
		})
	}
	if _, _, err := parseFlags([]string{"-takeover-rank-step", "soon"}, &bytes.Buffer{}); err == nil {
		t.Fatal("a bad duration must be rejected")
	}
}

func TestMemStoreOpens(t *testing.T) {
	f, _, _ := parseFlags([]string{"-store", "mem", "-metrics-addr", ""}, &bytes.Buffer{})
	n, closeNode, err := openNode(t.Context(), f, controlplanemetrics.New(), slog.Default())
	if err != nil || n == nil {
		t.Fatal(err)
	}
	closeNode()
	f.kind = "nope"
	if _, _, err := openNode(t.Context(), f, controlplanemetrics.New(), slog.Default()); err == nil {
		t.Fatal("unknown store accepted")
	}
}

func TestPostgresNeedsDSN(t *testing.T) {
	t.Setenv(postgresDSNEnv, "")
	f, _, _ := parseFlags([]string{"-store", kindPostgres}, &bytes.Buffer{})
	_, _, _, err := openPostgresLog(t.Context(), f, slog.Default())
	if err == nil || !strings.Contains(err.Error(), "-postgres-dsn or MDS_POSTGRES_DSN") {
		t.Fatalf("empty DSN: %v", err)
	}
}

func TestRedactDSN(t *testing.T) {
	cases := []struct{ dsn, want string }{
		{"postgres://user:hunter2@db.example:5432/mds?sslmode=require&password=hunter2", "db.example:5432/mds"},
		{"postgres://user:hunter2@db.example/mds", "db.example/mds"},
		{"postgres://u:hunter2@[::1]:5432/x", "[::1]:5432/x"},
		{"host=db password=hunter2 dbname=mds", ""},
		{"hunter2", ""},
		{"", ""},
	}
	for _, c := range cases {
		got := redactDSN(c.dsn)
		if got != c.want || strings.Contains(got, "hunter2") {
			t.Errorf("redactDSN(%q) = %q, want %q", c.dsn, got, c.want)
		}
	}
}

// The DSN must not show in the flag values, the run manifest, or an open error.
func TestDSNStaysOutOfFlagsManifestAndErrors(t *testing.T) {
	const dsn = "postgres://user:hunter2@db.example/mds" //nolint:gosec // G101: a fake password for this test
	t.Setenv(postgresDSNEnv, dsn)
	f, fs, err := parseFlags([]string{"-store", kindPostgres, "-postgres-dsn", dsn}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if got := fs.Lookup("postgres-dsn").Value.String(); got != "(set)" {
		t.Fatalf("flag value shows %q", got)
	}
	m := runinfo.NewManifest(runinfo.New("mdscontrolplane", "t", -1), fs, "us-east-1", nil)
	m.Args = []string{"-store", kindPostgres, "-postgres-dsn", dsn}
	args := m.Args
	hidden := hideDSN(m, resolvedDSN(f.log))
	body, err := json.Marshal(hidden)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), "hunter2") {
		t.Fatalf("manifest holds the password: %s", body)
	}
	if args[3] != dsn {
		t.Fatal("hideDSN changed the caller's Args slice")
	}
	// Nothing listens on port 1, so the open fails. The error must not show the password.
	const badDSN = "postgres://user:hunter2@127.0.0.1:1/db?sslmode=disable&connect_timeout=1" //nolint:gosec // G101: a fake password for this test
	t.Setenv(postgresDSNEnv, "")
	bad, _, _ := parseFlags([]string{"-store", kindPostgres, "-postgres-dsn", badDSN}, &bytes.Buffer{})
	_, _, _, err = openPostgresLog(t.Context(), bad, slog.Default())
	if err == nil || strings.Contains(err.Error(), "hunter2") {
		t.Fatalf("want an open error without the password, got %v", err)
	}
}
