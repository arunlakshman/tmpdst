package logsm

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"manifester/core"
)

// Apply rate (the Delos 5.1 playback question): how fast one replica applies
// commands. `go test ./cp/logsm -run xxx -bench Apply -benchtime 20000x`.

func applier() *Replica { return NewReplica(Config{Log: NewMemLog(), Writer: "bench"}) }

// warm touches every partition of the universe once, so the state has
// `universe` partitions with runs, and optionally adds `keys` one-batch
// segments (a long key set).
func warm(r *Replica, universe, keys int) {
	s := newSynth(99, universe, 100)
	for i := 0; i < universe/100; i++ {
		var bs []core.PendingBatch
		for j := 0; j < 100; j++ {
			p := i*100 + j
			bs = append(bs, core.PendingBatch{TP: core.TopicPartition{Topic: fmt.Sprintf("topic-%03d", p/s.per), Partition: int32(p % s.per)}, Records: 5})
		}
		r.observe(r.alloc(), Entry{Kind: KindCommit, Commit: core.CommitSegment{SegmentKey: s.key(), Batches: bs}})
	}
	for i := 0; i < keys; i++ {
		e := s.command()
		e.Commit.Batches = e.Commit.Batches[:1]
		r.observe(r.alloc(), e)
	}
}

func benchApply(b *testing.B, universe, parts, keys int, decode bool) {
	r := applier()
	warm(r, universe, keys)
	s := newSynth(7, universe, parts)
	tmpl := make([]Entry, 256)
	enc := make([][]byte, len(tmpl))
	for i := range tmpl {
		tmpl[i] = s.command()
		enc[i] = Encode(tmpl[i])
	}
	ids := make([]string, b.N)
	for i := range ids {
		ids[i] = s.key()
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var e Entry
		if decode {
			var err error
			if e, err = Decode(enc[i%len(enc)]); err != nil {
				b.Fatal(err)
			}
		} else {
			e = tmpl[i%len(tmpl)]
		}
		e.Commit.SegmentKey = ids[i]
		r.observe(r.alloc(), e)
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "cmd/s")
}

func BenchmarkApply(b *testing.B) {
	for _, universe := range []int{288, 40000} {
		for _, parts := range []int{50, 150} {
			b.Run(fmt.Sprintf("state=%d/parts=%d", universe, parts), func(b *testing.B) { benchApply(b, universe, parts, 0, false) })
		}
	}
}

func BenchmarkDecodeApply(b *testing.B) {
	for _, universe := range []int{288, 40000} {
		b.Run(fmt.Sprintf("state=%d/parts=150", universe), func(b *testing.B) { benchApply(b, universe, 150, 0, true) })
	}
}

// BenchmarkApplyLongKeySet: 230k keys already committed (1 hour at 64
// segments/s), which veritas keeps in a sorted slice.
func BenchmarkApplyLongKeySet(b *testing.B) {
	b.Run("keys=230000/state=40000/parts=150", func(b *testing.B) { benchApply(b, 40000, 150, 230000, true) })
}

func BenchmarkDecode(b *testing.B) {
	e := newSynth(1, 40000, 150).command()
	buf := Encode(e)
	b.SetBytes(int64(len(buf)))
	for i := 0; i < b.N; i++ {
		if _, err := Decode(buf); err != nil {
			b.Fatal(err)
		}
	}
}

// TestBacklogReplay times a replica catching up from the log: a minute of
// commands at 64 and 430 segments/s read from a MemLog (decode + apply).
func TestBacklogReplay(t *testing.T) {
	if testing.Short() {
		t.Skip("timing")
	}
	for _, rate := range []int{64, 430} {
		l := NewMemLog()
		s := newSynth(3, 40000, 150)
		var bytes int
		for i := 0; i < rate*60; i++ {
			e := s.command()
			bytes += len(Encode(e))
			l.Append(bg, int64(i), e)
		}
		r := NewReplica(Config{Log: l, Writer: "r", ReadMax: 1000})
		start := time.Now()
		if err := r.SyncTo(bg, int64(rate*60), true); err != nil {
			t.Fatal(err)
		}
		d := time.Since(start)
		t.Logf("backlog 60 s at %d seg/s: %d entries, %.1f MB encoded (%d B/entry), apply from memory log %v (%.1fx real time)",
			rate, rate*60, float64(bytes)/1e6, bytes/(rate*60), d.Round(time.Millisecond), 60/d.Seconds())
	}
}

// TestRetainedMemory measures the heap veritas.ControlPlane keeps per applied
// batch (it keeps every run and every segment forever): the memory side of
// the apply-rate question.
func TestRetainedMemory(t *testing.T) {
	if testing.Short() {
		t.Skip("timing")
	}
	r := applier()
	s := newSynth(11, 40000, 150)
	es := make([]Entry, 4000)
	for i := range es {
		es[i] = s.command()
	}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for _, e := range es {
		r.observe(r.alloc(), e)
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	batches := len(es) * 150
	per := (float64(after.HeapAlloc) - float64(before.HeapAlloc)) / float64(batches)
	t.Logf("veritas retains about %.0f bytes per applied batch (%d commands x 150 partitions); keep r alive: %d", per, len(es), r.Applied())
}
