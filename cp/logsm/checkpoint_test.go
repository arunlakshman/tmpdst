package logsm

import (
	"bytes"
	"compress/flate"
	"fmt"
	"slices"
	"sort"
	"testing"
	"time"

	"manifester/core"
	"manifester/veritas"
	"manifester/veritas/veritastest"
)

func TestCheckpointRoundTrip(t *testing.T) {
	l := NewMemLog()
	r := newMemReplica("r", l)
	c := NewCommitter(r, 1)
	for i := 0; i < 5; i++ {
		if d, err := c.Do(bg, commitEntry(fmt.Sprint("k", i), pb("a", int32(i), 2), pb("b", 0, 3))); err != nil || d.Err != core.OK {
			t.Fatal(d, err)
		}
	}
	cp := r.Checkpoint()
	if cp.Slot != 5 || len(cp.Keys) != 5 || len(cp.Ends) != 6 || cp.Hash != r.StateHash() {
		t.Fatalf("checkpoint %+v", cp)
	}
	if !sort.SliceIsSorted(cp.Keys, func(i, j int) bool { return bytes.Compare(cp.Keys[i][:], cp.Keys[j][:]) < 0 }) {
		t.Fatal("keys not sorted")
	}
	got, err := UnmarshalCheckpoint(cp.Marshal())
	if err != nil || got.Slot != cp.Slot || got.Hash != cp.Hash || !slices.Equal(got.Ends, cp.Ends) || !slices.Equal(got.Keys, cp.Keys) {
		t.Fatalf("round trip %+v err %v", got, err)
	}
	rs := got.Restore()
	if _, ok := rs.Keys[KeyHash("k3")]; !ok || rs.Ends[core.TP("b", 0)] != 15 {
		t.Fatalf("restored %+v", rs.Ends)
	}
	b := cp.Marshal()
	for _, bad := range [][]byte{nil, b[:5], b[:len(b)-1], append(slices.Clone(b), 0)} {
		if _, err := UnmarshalCheckpoint(bad); err == nil {
			t.Fatalf("corrupt checkpoint of %d bytes decoded", len(bad))
		}
	}
}

// synthCheckpoint is a checkpoint of nParts partitions (100 per topic, offsets
// up to a few million) and nKeys random 16-byte keys.
func synthCheckpoint(nParts, nKeys int) Checkpoint {
	r := veritastest.NewRand(5)
	c := Checkpoint{Slot: 1 << 20, Hash: r.Uint64()}
	for p := 0; p < nParts; p++ {
		c.Ends = append(c.Ends, veritas.End{
			TP:     core.TP(fmt.Sprintf("topic-%03d", p/100), int32(p%100)),
			Offset: core.Offset(r.Intn(5_000_000)),
		})
	}
	for i := 0; i < nKeys; i++ {
		var k [16]byte
		for j := range k {
			k[j] = byte(r.Uint64())
		}
		c.Keys = append(c.Keys, k)
	}
	slices.SortFunc(c.Keys, func(a, b [16]byte) int { return bytes.Compare(a[:], b[:]) })
	return c
}

func median(ds []time.Duration) time.Duration {
	slices.Sort(ds)
	return ds[len(ds)/2]
}

// TestX5 measures the checkpoint of 40k partitions' ends plus an hour of key
// set (100k keys of 16 bytes): serialised size and load time.
func TestX5(t *testing.T) {
	cases := []struct{ parts, keys int }{{288, 100_000}, {40_000, 100_000}, {40_000, 230_000}}
	for _, tc := range cases {
		c := synthCheckpoint(tc.parts, tc.keys)
		b := c.Marshal()
		var z bytes.Buffer
		w, _ := flate.NewWriter(&z, flate.BestSpeed)
		_, _ = w.Write(b)
		_ = w.Close()
		var dec, load []time.Duration
		for i := 0; i < 15; i++ {
			t0 := time.Now()
			got, err := UnmarshalCheckpoint(b)
			t1 := time.Now()
			got.Restore()
			t2 := time.Now()
			if err != nil || len(got.Keys) != tc.keys {
				t.Fatal(err)
			}
			dec, load = append(dec, t1.Sub(t0)), append(load, t2.Sub(t0))
		}
		t.Logf("X5 partitions=%d keys=%d: %d bytes (%.2f MB), flate %d bytes; decode %v, decode+build maps (load) %v (median of 15)",
			tc.parts, tc.keys, len(b), float64(len(b))/1e6, z.Len(), median(dec).Round(10*time.Microsecond), median(load).Round(10*time.Microsecond))
		if tc.parts == 40_000 && tc.keys == 100_000 && len(b) > 3<<20 {
			t.Fatalf("checkpoint is %d bytes, expected about 1.9 MB", len(b))
		}
	}
}
