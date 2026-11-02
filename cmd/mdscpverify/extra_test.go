package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/manifesterio/mds/internal/core"
)

// Records are added per partition, a repeated key counts once, and a cut last line is ignored.
func TestRecordsPerPartition(t *testing.T) {
	lines := `{"key":"a","assigned":[{"topic":"t","partition":0,"base":0,"count":10},{"topic":"t","partition":1,"base":0,"count":5}]}
{"key":"b","assigned":[{"topic":"t","partition":0,"base":10,"count":10}]}
{"key":"a","assigned":[{"topic":"t","partition":0,"base":0,"count":10}]}
{"key":"c","assigned":[{"topic":"t","partit`
	path := filepath.Join(t.TempDir(), "acked.jsonl")
	if err := os.WriteFile(path, []byte(lines), 0o600); err != nil {
		t.Fatal(err)
	}
	totals, keys, err := recordsPerPartition(path)
	if err != nil {
		t.Fatal(err)
	}
	if keys != 2 {
		t.Errorf("distinct keys %d, want 2 (the repeat and the cut line do not count)", keys)
	}
	if got := totals[core.NewTopicPartition("t", 0)]; got != 20 {
		t.Errorf("partition 0 records %d, want 20", got)
	}
	if got := totals[core.NewTopicPartition("t", 1)]; got != 5 {
		t.Errorf("partition 1 records %d, want 5", got)
	}
}
