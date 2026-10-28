package generator

import (
	"fmt"
	"manifester/internal/core"
	"manifester/internal/verify/veritas/veritastest"
	"strings"
)

// ObjectOps returns a generator of object store operations for profile p.
// Operation i carries RequestID i+1. The mix includes the empty key, reads past
// the end of an object, Len <= 0 (read to the end), and prefixes with and
// without a slash, because each of these is a place where stores differ.
func ObjectOps(p Profile) func(r *veritastest.Rand, i int) core.IOCall {
	return func(r *veritastest.Rand, i int) core.IOCall {
		req := core.RequestID(i + 1)
		key := pick(r, p.ObjectKeys)
		if percent(r, p.EmptyKeyPercent) {
			key = ""
		}
		switch pickWeighted(r, p.PutWeight, p.GetWeight, p.DeleteWeight, p.ListWeight) {
		case 0:
			return core.ObjectPut{Req: req, Key: key, Data: randomData(r, p.MaxDataLen)}
		case 1:
			ref := core.SegmentRef{Key: key, Off: int64(r.Intn(p.RangeSpan) - 1), Len: int64(r.Intn(p.RangeSpan) - 1)}
			return core.ObjectGet{Req: req, Ref: ref}
		case 2:
			return core.ObjectDelete{Req: req, Key: key}
		}
		return core.ObjectList{Req: req, Prefix: pick(r, p.ObjectPrefixes)}
	}
}

// randomData returns between 0 and maxLen letters. An empty result is not nil,
// so a Put of empty data differs from a Put with no data field.
func randomData(r *veritastest.Rand, maxLen int) []byte {
	data := make([]byte, r.Intn(maxLen+1))
	for j := range data {
		data[j] = byte('a' + r.Intn(26))
	}
	return data
}

// ShrinkObjectOp returns simpler versions of op, simplest first: less data, a
// range that starts at 0 and reads to the end, and a shorter key or prefix.
// It never returns op itself, so a caller that keeps shrinking always ends.
func ShrinkObjectOp(op core.IOCall) []core.IOCall {
	switch op := op.(type) {
	case core.ObjectPut:
		return shrinkPut(op)
	case core.ObjectGet:
		return shrinkGet(op)
	case core.ObjectDelete:
		var out []core.IOCall
		for _, k := range shorterStrings(op.Key) {
			out = append(out, core.ObjectDelete{Req: op.Req, Key: k})
		}
		return out
	case core.ObjectList:
		var out []core.IOCall
		for _, p := range shorterStrings(op.Prefix) {
			out = append(out, core.ObjectList{Req: op.Req, Prefix: p})
		}
		return out
	}
	return nil
}

func shrinkPut(op core.ObjectPut) []core.IOCall {
	var out []core.IOCall
	if len(op.Data) > 0 {
		out = append(out, core.ObjectPut{Req: op.Req, Key: op.Key, Data: nil})
	}
	if len(op.Data) > 1 {
		out = append(out, core.ObjectPut{Req: op.Req, Key: op.Key, Data: op.Data[:len(op.Data)/2]})
		out = append(out, core.ObjectPut{Req: op.Req, Key: op.Key, Data: op.Data[:len(op.Data)-1]})
	}
	for _, k := range shorterStrings(op.Key) {
		out = append(out, core.ObjectPut{Req: op.Req, Key: k, Data: op.Data})
	}
	return out
}

func shrinkGet(op core.ObjectGet) []core.IOCall {
	var out []core.IOCall
	if op.Ref.Len != 0 {
		ref := op.Ref
		ref.Len = 0
		out = append(out, core.ObjectGet{Req: op.Req, Ref: ref})
	}
	if op.Ref.Off != 0 {
		ref := op.Ref
		ref.Off = 0
		out = append(out, core.ObjectGet{Req: op.Req, Ref: ref})
	}
	for _, k := range shorterStrings(op.Ref.Key) {
		ref := op.Ref
		ref.Key = k
		out = append(out, core.ObjectGet{Req: op.Req, Ref: ref})
	}
	return out
}

// shorterStrings returns shorter versions of s: the first byte alone, then s
// without its last byte. The empty string is not shortened, because an empty
// key is a different case and not a smaller one.
func shorterStrings(s string) []string {
	var out []string
	if len(s) > 1 {
		out = append(out, s[:1])
	}
	if len(s) > 2 {
		out = append(out, s[:len(s)-1])
	}
	return out
}

// FormatObjectOp returns op as a Go expression that you can paste into a
// regression test.
func FormatObjectOp(op core.IOCall) string {
	switch op := op.(type) {
	case core.ObjectPut:
		return fmt.Sprintf("core.ObjectPut{Req: %d, Key: %q%s}", op.Req, op.Key, formatDataField(op.Data))
	case core.ObjectGet:
		return fmt.Sprintf("core.ObjectGet{Req: %d, Ref: %s}", op.Req, formatRef(op.Ref))
	case core.ObjectDelete:
		return fmt.Sprintf("core.ObjectDelete{Req: %d, Key: %q}", op.Req, op.Key)
	case core.ObjectList:
		return fmt.Sprintf("core.ObjectList{Req: %d, Prefix: %q}", op.Req, op.Prefix)
	}
	return fmt.Sprintf("%#v", op)
}

// formatDataField writes the Data field with a leading comma, or nothing when
// Data is nil. It keeps nil and empty data apart.
func formatDataField(data []byte) string {
	switch {
	case data == nil:
		return ""
	case len(data) == 0:
		return ", Data: []byte{}"
	}
	return fmt.Sprintf(", Data: []byte(%q)", string(data))
}

func formatRef(ref core.SegmentRef) string {
	var b strings.Builder
	fmt.Fprintf(&b, "core.SegmentRef{Key: %q, Off: %d, Len: %d}", ref.Key, ref.Off, ref.Len)
	return b.String()
}
