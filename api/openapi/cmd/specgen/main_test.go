package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRun(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "controlplane.json"), []byte(`{"a":[1]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"-dir", dir, "-check"}, nil); err == nil {
		t.Fatal("check without a yaml file should fail")
	}
	if err := run([]string{"-dir", dir}, nil); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"-dir", dir, "-check"}, nil); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "controlplane.yaml"), []byte("stale\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"-dir", dir, "-check"}, nil); err == nil {
		t.Fatal("stale yaml should fail the check")
	}
	if err := run([]string{"-bogus"}, nil); err == nil {
		t.Fatal("bad flag should fail")
	}
	if err := run([]string{"-dir", filepath.Join(dir, "none")}, nil); err == nil {
		t.Fatal("missing json should fail")
	}
}
