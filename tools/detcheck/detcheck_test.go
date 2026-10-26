package main

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"
)

func TestDetcheck(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), Analyzer, "a")
}

func TestAllowList(t *testing.T) {
	al := allowList{exact: map[string]bool{"fmt": true}, prefix: []string{"manifester/core"}}
	for path, want := range map[string]bool{"fmt": true, "os": false, "manifester/core": true, "manifester/core/x": true, "manifester/corex": false} {
		if got := al.allows(path); got != want {
			t.Errorf("allows(%q) = %v", path, got)
		}
	}
}

func TestLoadAllowFile(t *testing.T) {
	old := allowFile
	defer func() { allowFile = old }()
	f := filepath.Join(t.TempDir(), "allow.txt")
	if err := os.WriteFile(f, []byte("# c\n\nfmt\nmanifester/core/...\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	allowFile = f
	al, err := loadAllow()
	if err != nil || !al.allows("fmt") || !al.allows("manifester/core/a") || al.allows("os") {
		t.Fatal(al, err)
	}
	allowFile = filepath.Join(t.TempDir(), "missing")
	if _, err := loadAllow(); err == nil {
		t.Fatal("want error")
	}
	allowFile = ""
	if al, err := loadAllow(); err != nil || len(al.exact)+len(al.prefix) == 0 {
		t.Fatal(err)
	}
}
