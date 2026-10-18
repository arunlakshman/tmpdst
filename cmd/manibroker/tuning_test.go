package main

import (
	"testing"
	"time"

	"manifester/broker"
	"manifester/core"
)

func defaults() tuning {
	d := broker.DefaultConfig()
	return tuning{segmentBytes: "16MiB", window: 200 * time.Millisecond, parts: d.MaxSegmentPartitions,
		batches: d.MaxSegmentBatches, maxBuffered: "auto"}
}

func TestTuningDefaults(t *testing.T) {
	bc := broker.DefaultConfig()
	if err := defaults().apply(&bc, 8<<30); err != nil {
		t.Fatal(err)
	}
	if bc.MaxSegmentBytes != 16<<20 || bc.BatchWindow != 200*core.Millisecond || bc.MaxSegmentPartitions != 96 || bc.MaxSegmentBatches != 256 {
		t.Fatalf("%+v", bc)
	}
	if bc.MaxBufferedBytes != 512<<20 || bc.ResumeBufferedBytes != 256<<20 { // 8 GiB / 16
		t.Fatalf("auto buffer %d/%d", bc.MaxBufferedBytes, bc.ResumeBufferedBytes)
	}
	// Unknown memory: 128 MiB.
	if err := defaults().apply(&bc, 0); err != nil || bc.MaxBufferedBytes != 128<<20 {
		t.Fatal(err, bc.MaxBufferedBytes)
	}
}

func TestTuningOverrides(t *testing.T) {
	tn := defaults()
	tn.segmentBytes, tn.window, tn.parts, tn.batches = "4MB", 50*time.Millisecond, 3, 5
	tn.maxBuffered, tn.resumeBuffered = "256MiB", "1GiB"
	bc := broker.DefaultConfig()
	if err := tn.apply(&bc, 64<<30); err == nil {
		t.Fatal("resume above max accepted")
	}
	tn.resumeBuffered = "64MiB"
	if err := tn.apply(&bc, 64<<30); err != nil {
		t.Fatal(err)
	}
	if bc.MaxSegmentBytes != 4_000_000 || bc.BatchWindow != 50*core.Millisecond || bc.MaxSegmentPartitions != 3 || bc.MaxSegmentBatches != 5 ||
		bc.MaxBufferedBytes != 256<<20 || bc.ResumeBufferedBytes != 64<<20 {
		t.Fatalf("%+v", bc)
	}
	tn.resumeBuffered = ""
	if err := tn.apply(&bc, 0); err != nil || bc.ResumeBufferedBytes != 128<<20 {
		t.Fatal(err, bc.ResumeBufferedBytes)
	}
	for _, bad := range []func(*tuning){
		func(t *tuning) { t.segmentBytes = "lots" }, func(t *tuning) { t.window = 0 },
		func(t *tuning) { t.batches = 0 }, func(t *tuning) { t.maxBuffered = "-1" }, func(t *tuning) { t.maxBuffered = "0" },
	} {
		x := defaults()
		bad(&x)
		if err := x.apply(&bc, 0); err == nil {
			t.Errorf("%+v accepted", x)
		}
	}
}
