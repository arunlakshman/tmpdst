package generator

import (
	"fmt"
	"mds/internal/core"
	"mds/internal/verify/speccheck/checker"
	"strings"
)

// ObjectOps returns a generator of object store operations for profile p.
// Operation i carries RequestID i+1. The mix includes the empty key, reads past
// the end of an object, Len <= 0 (read to the end), and prefixes with and
// without a slash, because each of these is a place where stores differ.
func ObjectOps(p InputProfile) func(r *checker.Rand, i int) core.IOCall {
	return func(r *checker.Rand, i int) core.IOCall {
		request := core.RequestID(i + 1)
		key := pick(r, p.ObjectKeys)
		if percent(r, p.EmptyKeyPercent) {
			key = ""
		}
		switch chooseByWeight(r, p.PutWeight, p.GetWeight, p.DeleteWeight, p.ListWeight) {
		case 0:
			return core.ObjectPut{Req: request, Key: key, Data: randomData(r, p.MaxDataLen)}
		case 1:
			ref := core.SegmentRef{Key: key, Off: int64(r.Intn(p.RangeSpan) - 1), Len: int64(r.Intn(p.RangeSpan) - 1)}
			return core.ObjectGet{Req: request, Ref: ref}
		case 2:
			return core.ObjectDelete{Req: request, Key: key}
		}
		return core.ObjectList{Req: request, Prefix: pick(r, p.ObjectPrefixes)}
	}
}

// randomData returns between 0 and maxLen letters. An empty result is not nil,
// so a Put of empty data differs from a Put with no data field.
func randomData(r *checker.Rand, maxLen int) []byte {
	data := make([]byte, r.Intn(maxLen+1))
	for j := range data {
		data[j] = byte('a' + r.Intn(26))
	}
	return data
}

// MinimizeObjectOp returns simpler versions of op, simplest first: less data, a
// range that starts at 0 and reads to the end, and a shorter key or prefix.
// It never returns op itself, so a caller that keeps shrinking always ends.
func MinimizeObjectOp(operation core.IOCall) []core.IOCall {
	switch operation := operation.(type) {
	case core.ObjectPut:
		return minimizePut(operation)
	case core.ObjectGet:
		return minimizeGet(operation)
	case core.ObjectDelete:
		var out []core.IOCall
		for _, k := range shortenStrings(operation.Key) {
			out = append(out, core.ObjectDelete{Req: operation.Req, Key: k})
		}
		return out
	case core.ObjectList:
		var out []core.IOCall
		for _, p := range shortenStrings(operation.Prefix) {
			out = append(out, core.ObjectList{Req: operation.Req, Prefix: p})
		}
		return out
	}
	return nil
}

func minimizePut(operation core.ObjectPut) []core.IOCall {
	var out []core.IOCall
	if len(operation.Data) > 0 {
		out = append(out, core.ObjectPut{Req: operation.Req, Key: operation.Key, Data: nil})
	}
	if len(operation.Data) > 1 {
		out = append(out, core.ObjectPut{Req: operation.Req, Key: operation.Key, Data: operation.Data[:len(operation.Data)/2]})
		out = append(out, core.ObjectPut{Req: operation.Req, Key: operation.Key, Data: operation.Data[:len(operation.Data)-1]})
	}
	for _, k := range shortenStrings(operation.Key) {
		out = append(out, core.ObjectPut{Req: operation.Req, Key: k, Data: operation.Data})
	}
	return out
}

func minimizeGet(operation core.ObjectGet) []core.IOCall {
	var out []core.IOCall
	if operation.Ref.Len != 0 {
		ref := operation.Ref
		ref.Len = 0
		out = append(out, core.ObjectGet{Req: operation.Req, Ref: ref})
	}
	if operation.Ref.Off != 0 {
		ref := operation.Ref
		ref.Off = 0
		out = append(out, core.ObjectGet{Req: operation.Req, Ref: ref})
	}
	for _, k := range shortenStrings(operation.Ref.Key) {
		ref := operation.Ref
		ref.Key = k
		out = append(out, core.ObjectGet{Req: operation.Req, Ref: ref})
	}
	return out
}

// shortenStrings returns shorter versions of s: the first byte alone, then s
// without its last byte. The empty string is not shortened, because an empty
// key is a different case and not a smaller one.
func shortenStrings(s string) []string {
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
func FormatObjectOp(operation core.IOCall) string {
	switch operation := operation.(type) {
	case core.ObjectPut:
		return fmt.Sprintf("core.ObjectPut{Req: %d, Key: %q%s}", operation.Req, operation.Key, formatDataField(operation.Data))
	case core.ObjectGet:
		return fmt.Sprintf("core.ObjectGet{Req: %d, Ref: %s}", operation.Req, formatRef(operation.Ref))
	case core.ObjectDelete:
		return fmt.Sprintf("core.ObjectDelete{Req: %d, Key: %q}", operation.Req, operation.Key)
	case core.ObjectList:
		return fmt.Sprintf("core.ObjectList{Req: %d, Prefix: %q}", operation.Req, operation.Prefix)
	}
	return fmt.Sprintf("%#v", operation)
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
