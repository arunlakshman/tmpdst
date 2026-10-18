package main

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"manifester/broker"
	"manifester/core"
	"manifester/sys/prod"
)

// tuning is the flag values that shape the segment flush rule and the produce
// buffer (decision 35).
type tuning struct {
	segmentBytes   string
	window         time.Duration
	parts, batches int
	items          int    // control plane commit item budget
	maxBuffered    string // "auto" or a size
	resumeBuffered string // "" = half of the max
}

// apply sets bc from t. memLimit is the machine's memory (0 = unknown); it
// sizes the buffer when maxBuffered is "auto".
func (t tuning) apply(bc *broker.Config, memLimit uint64) error {
	seg, err := parseSize(t.segmentBytes)
	if err != nil {
		return fmt.Errorf("-segment-bytes: %w", err)
	}
	if seg <= 0 || t.window <= 0 || t.parts <= 0 || t.batches <= 0 || t.items <= 0 {
		return errors.New("-segment-bytes, -segment-window, -max-segment-partitions, -max-segment-batches and -max-segment-items must be positive")
	}
	bc.MaxSegmentBytes, bc.BatchWindow = int(seg), core.Duration(t.window)
	bc.MaxSegmentPartitions, bc.MaxSegmentBatches, bc.MaxSegmentItems = t.parts, t.batches, t.items

	if strings.EqualFold(t.maxBuffered, "auto") {
		bc.MaxBufferedBytes, bc.ResumeBufferedBytes = prod.AutoBuffered(memLimit)
	} else {
		m, err := parseSize(t.maxBuffered)
		if err != nil {
			return fmt.Errorf("-max-buffered: %w", err)
		}
		if m <= 0 {
			return errors.New("-max-buffered must be positive (or auto)")
		}
		bc.MaxBufferedBytes, bc.ResumeBufferedBytes = int(m), int(m/2)
	}
	if t.resumeBuffered != "" {
		r, err := parseSize(t.resumeBuffered)
		if err != nil {
			return fmt.Errorf("-resume-buffered: %w", err)
		}
		if r <= 0 || r > int64(bc.MaxBufferedBytes) {
			return errors.New("-resume-buffered must be positive and at most -max-buffered")
		}
		bc.ResumeBufferedBytes = int(r)
	}
	return nil
}
