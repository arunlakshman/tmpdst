package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"manifester/internal/controlplane/stateupdater"
	"manifester/internal/core"
)

func TestMachineConfigSetsTheIdempotencyWindow(t *testing.T) {
	f, _, err := parseFlags([]string{"-window-slots", "4096"}, os.Stderr)
	if err != nil {
		t.Fatal(err)
	}
	c := machineConfig(f)
	if c.WindowSlots != 4096 {
		t.Errorf("WindowSlots = %d, want 4096 from -window-slots", c.WindowSlots)
	}
	if c.TrimStride != trimStride {
		t.Errorf("TrimStride = %d, want %d", c.TrimStride, trimStride)
	}
	if c.KeepSlots != 4096/8 {
		t.Errorf("KeepSlots = %d, want %d", c.KeepSlots, 4096/8)
	}
}

// topicCount asks a store how many topics it holds.
func topicCount(t *testing.T, kv stateupdater.KeyValueStore) int {
	t.Helper()
	d, err := stateupdater.New(kv).Describe(core.DescribeTopics{})
	if err != nil {
		t.Fatal(err)
	}
	return len(core.ResponseAs[core.DescribeTopicsResp](d).Topics)
}

func TestPebbleStateReopensEmpty(t *testing.T) {
	p := &pebbleState{flagDir: filepath.Join(t.TempDir(), "state")}
	if err := p.start(); err != nil {
		t.Fatal(err)
	}
	defer p.close()
	first, err := p.openStore()
	if err != nil {
		t.Fatal(err)
	}
	if code := stateupdater.New(first).CreateTopic("orders", 2); code != core.OK {
		t.Fatalf("CreateTopic code = %v", code)
	}
	if n := topicCount(t, first); n != 1 {
		t.Fatalf("topics before the restart = %d, want 1", n)
	}
	// A second open is a restart: the old state must be gone.
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := p.openStore()
	if err != nil {
		t.Fatal(err)
	}
	if n := topicCount(t, second); n != 0 {
		t.Fatalf("topics after the restart = %d, want 0", n)
	}
}

func TestPebbleStateRefusesAForeignDirectory(t *testing.T) {
	dir := t.TempDir()
	keep := filepath.Join(dir, "photos.txt")
	if err := os.WriteFile(keep, []byte("mine"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := (&pebbleState{flagDir: dir}).start()
	if err == nil || !strings.Contains(err.Error(), "refusing to wipe") {
		t.Fatalf("err = %v, want a refusal", err)
	}
	if _, statErr := os.Stat(keep); statErr != nil {
		t.Fatalf("the foreign file must survive: %v", statErr)
	}
}

func TestPebbleStateRemovesItsTemporaryDirectory(t *testing.T) {
	p := &pebbleState{}
	if err := p.start(); err != nil {
		t.Fatal(err)
	}
	dir := p.dir
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("temporary directory missing: %v", err)
	}
	p.close()
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("temporary directory %s must be removed, stat err = %v", dir, err)
	}
}
