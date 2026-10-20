package cpobs

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestSeqMetrics(t *testing.T) {
	m := New()
	m.SeqQueueDepth(3)
	m.SeqInflight(2)
	m.SeqQueueWait(time.Millisecond)
	m.SeqGroup(4, 80)
	m.SeqTx("ok", 30*time.Millisecond)
	m.SeqCache("end", true)
	m.SeqCache("end", false)
	m.SeqRetry("transaction_canceled")
	m.TallyDynamo(context.Background(), time.Millisecond)
	out := gather(t, m)
	for _, want := range []string{"manicp_seq_queue_depth 3", "manicp_seq_inflight_groups 2", "manicp_seq_queue_wait_seconds_count 1",
		"manicp_seq_group_segments_count 1", "manicp_seq_group_items_sum 80", `manicp_seq_tx_items_bucket{le="70"} 0`, `manicp_seq_tx_items_bucket{le="80"} 1`, "manicp_seq_tx_items_count 1", `manicp_seq_tx_seconds_count{outcome="ok"} 1`,
		`manicp_seq_cache_total{cache="end",result="hit"} 1`, `manicp_seq_cache_total{cache="end",result="miss"} 1`,
		`manicp_seq_retries_total{reason="transaction_canceled"} 1`} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q", want)
		}
	}
	var nm *Metrics
	nm.SeqQueueDepth(1)
	nm.SeqInflight(1)
	nm.SeqQueueWait(0)
	nm.SeqGroup(1, 1)
	nm.SeqTx("ok", 0)
	nm.SeqCache("end", true)
	nm.SeqRetry("x")
	nm.TallyDynamo(context.Background(), 0)
}
