package generator

import (
	"fmt"
	"strings"

	"github.com/manifesterio/mds/broker/internal/verify/speccheck/checker"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/shared/objectstore"
)

// ObjectOps returns a generator of object store operations for profile p.
// Operation i carries RequestID i+1. The mix includes the empty key, reads past
// the end of an object, Len <= 0 (read to the end), and prefixes with and
// without a slash, because each of these is a place where stores differ.
func ObjectOps(p InputProfile) func(r *checker.Rand, i int) objectstore.Call {
	return func(r *checker.Rand, i int) objectstore.Call {
		request := machine.RequestID(i + 1)
		key := pick(r, p.ObjectKeys)
		if percent(r, p.EmptyKeyPercent) {
			key = ""
		}
		switch chooseByWeight(r, p.PutWeight, p.GetWeight, p.DeleteWeight, p.ListWeight) {
		case 0:
			return objectstore.Put{Req: request, Key: key, Data: randomData(r, p.MaxDataLen)}
		case 1:
			ref := objectstore.SegmentRef{Key: key, Off: int64(r.Intn(p.RangeSpan) - 1), Len: int64(r.Intn(p.RangeSpan) - 1)}
			return objectstore.Get{Req: request, Ref: ref}
		case 2:
			return objectstore.Delete{Req: request, Key: key}
		}
		return objectstore.List{Req: request, Prefix: pick(r, p.ObjectPrefixes)}
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
func MinimizeObjectOp(operation objectstore.Call) []objectstore.Call {
	switch operation := operation.(type) {
	case objectstore.Put:
		return minimizePut(operation)
	case objectstore.Get:
		return minimizeGet(operation)
	case objectstore.Delete:
		var out []objectstore.Call
		for _, k := range shortenStrings(operation.Key) {
			out = append(out, objectstore.Delete{Req: operation.Req, Key: k})
		}
		return out
	case objectstore.List:
		var out []objectstore.Call
		for _, p := range shortenStrings(operation.Prefix) {
			out = append(out, objectstore.List{Req: operation.Req, Prefix: p})
		}
		return out
	}
	return nil
}

func minimizePut(operation objectstore.Put) []objectstore.Call {
	var out []objectstore.Call
	if len(operation.Data) > 0 {
		out = append(out, objectstore.Put{Req: operation.Req, Key: operation.Key, Data: nil})
	}
	if len(operation.Data) > 1 {
		out = append(out, objectstore.Put{Req: operation.Req, Key: operation.Key, Data: operation.Data[:len(operation.Data)/2]})
		out = append(out, objectstore.Put{Req: operation.Req, Key: operation.Key, Data: operation.Data[:len(operation.Data)-1]})
	}
	for _, k := range shortenStrings(operation.Key) {
		out = append(out, objectstore.Put{Req: operation.Req, Key: k, Data: operation.Data})
	}
	return out
}

func minimizeGet(operation objectstore.Get) []objectstore.Call {
	var out []objectstore.Call
	if operation.Ref.Len != 0 {
		ref := operation.Ref
		ref.Len = 0
		out = append(out, objectstore.Get{Req: operation.Req, Ref: ref})
	}
	if operation.Ref.Off != 0 {
		ref := operation.Ref
		ref.Off = 0
		out = append(out, objectstore.Get{Req: operation.Req, Ref: ref})
	}
	for _, k := range shortenStrings(operation.Ref.Key) {
		ref := operation.Ref
		ref.Key = k
		out = append(out, objectstore.Get{Req: operation.Req, Ref: ref})
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
func FormatObjectOp(operation objectstore.Call) string {
	switch operation := operation.(type) {
	case objectstore.Put:
		return fmt.Sprintf("objectstore.Put{Req: %d, Key: %q%s}", operation.Req, operation.Key, formatDataField(operation.Data))
	case objectstore.Get:
		return fmt.Sprintf("objectstore.Get{Req: %d, Ref: %s}", operation.Req, formatRef(operation.Ref))
	case objectstore.Delete:
		return fmt.Sprintf("objectstore.Delete{Req: %d, Key: %q}", operation.Req, operation.Key)
	case objectstore.List:
		return fmt.Sprintf("objectstore.List{Req: %d, Prefix: %q}", operation.Req, operation.Prefix)
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

func formatRef(ref objectstore.SegmentRef) string {
	var b strings.Builder
	fmt.Fprintf(&b, "objectstore.SegmentRef{Key: %q, Off: %d, Len: %d}", ref.Key, ref.Off, ref.Len)
	return b.String()
}
