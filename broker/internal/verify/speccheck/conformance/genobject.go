package conformance

import (
	"pgregory.net/rapid"

	"github.com/manifesterio/mds/shared/objectstore"
)

// ObjectOps returns a generator of object store operations for profile p. The
// mix includes the empty key, reads past the end of an object, Len <= 0 (read
// to the end), and prefixes with and without a slash, because each of these is
// a place where stores differ.
//
// An operation has no request id. The run gives each call the id of its
// position (1, 2, 3 and so on) just before it goes to the subject.
//
// The kinds are listed simplest first, because rapid shrinks toward the first
// kind. The weights are the ones in the profile.
func ObjectOps(p InputProfile) *rapid.Generator[objectstore.Call] {
	return Custom(func(r Rand) objectstore.Call {
		key := pick(r, p.ObjectKeys)
		if percent(r, p.EmptyKeyPercent) {
			key = ""
		}
		switch chooseByWeight(r, p.ListWeight, p.GetWeight, p.DeleteWeight, p.PutWeight) {
		case 0:
			return objectstore.List{Prefix: pick(r, p.ObjectPrefixes)}
		case 1:
			return objectstore.Get{Ref: objectstore.SegmentRef{Key: key, Off: int64(r.Intn(p.RangeSpan) - 1), Len: int64(r.Intn(p.RangeSpan) - 1)}}
		case 2:
			return objectstore.Delete{Key: key}
		}
		return objectstore.Put{Key: key, Data: randomData(r, p.MaxDataLen)}
	})
}

// randomData returns between 0 and maxLen letters. An empty result is not nil,
// so a Put of empty data differs from a Put with no data field.
func randomData(r Rand, maxLen int) []byte {
	data := make([]byte, r.Intn(maxLen+1))
	for j := range data {
		data[j] = byte('a' + r.Intn(26))
	}
	return data
}
