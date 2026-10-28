package main

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"
)

func TestDetcheck(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), DeterminismAnalyzer, "a")
}

func TestAllowList(t *testing.T) {
	al := allowedImports{exact: map[string]bool{"fmt": true}, prefix: []string{"manifester/internal/core"}}
	for path, want := range map[string]bool{"fmt": true, "os": false, "manifester/internal/core": true, "manifester/internal/core/x": true, "manifester/corex": false} {
		if got := al.allows(path); got != want {
			t.Errorf("allows(%q) = %v", path, got)
		}
	}
}

func TestLoadAllowFile(t *testing.T) {
	old := allowedImportsFile
	defer func() { allowedImportsFile = old }()
	f := filepath.Join(t.TempDir(), "allow.txt")
	if err := os.WriteFile(f, []byte("# c\n\nfmt\nmanifester/internal/core/...\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	allowedImportsFile = f
	al, err := loadAllow()
	if err != nil || !al.allows("fmt") || !al.allows("manifester/internal/core/a") || al.allows("os") {
		t.Fatal(al, err)
	}
	allowedImportsFile = filepath.Join(t.TempDir(), "missing")
	if _, err := loadAllow(); err == nil {
		t.Fatal("want error")
	}
	allowedImportsFile = ""
	if al, err := loadAllow(); err != nil || len(al.exact)+len(al.prefix) == 0 {
		t.Fatal(err)
	}
}
