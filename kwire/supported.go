package kwire

import (
	"github.com/twmb/franz-go/pkg/kmsg"

	"manifester/core"
)

// RequestHeader is core.RequestHeader.
type RequestHeader = core.RequestHeader

// API keys the broker serves.
const (
	KeyProduce     int16 = 0
	KeyFetch       int16 = 1
	KeyListOffsets int16 = 2
	KeyMetadata    int16 = 3
	KeyApiVersions int16 = 18 //nolint:revive // Kafka spelling (ApiVersions); renaming breaks sim/ callers
)

// ErrCodeUnsupportedVersion is Kafka's UNSUPPORTED_VERSION error code.
const ErrCodeUnsupportedVersion int16 = 35

// APIRange is an API key and its inclusive supported version range.
type APIRange struct{ Key, Min, Max int16 }

var (
	supported = []APIRange{
		{Key: KeyProduce, Min: 3, Max: 9},
		{Key: KeyFetch, Min: 4, Max: 12},
		{Key: KeyListOffsets, Min: 1, Max: 7},
		{Key: KeyMetadata, Min: 1, Max: 12},
		{Key: KeyApiVersions, Min: 0, Max: 3},
	}
	supportedByKey map[int16]APIRange
)

func init() {
	supportedByKey = make(map[int16]APIRange, len(supported))
	for _, r := range supported {
		supportedByKey[r.Key] = r
	}
}

// Supported returns the advertised APIs in key order. Produce >= 3 so batches
// are RecordBatch v2. Produce and Fetch stay below v13, which switches to
// topic IDs. The result aliases the package registry, so callers must not mutate it.
func Supported() []APIRange {
	return supported
}

// Range returns the supported version range of key.
func Range(key int16) (APIRange, bool) {
	r, ok := supportedByKey[key]
	return r, ok
}

// IsSupported reports whether (key, version) is served.
func IsSupported(key, version int16) bool {
	r, ok := Range(key)
	return ok && version >= r.Min && version <= r.Max
}

// ApiVersionsResponse builds the broker's answer to an ApiVersions request
// with header h. When h.APIVersion is unsupported it carries
// UNSUPPORTED_VERSION; EncodeResponse then encodes it as v0, as Kafka does.
// Set the other fields (ThrottleMillis) as needed.
func ApiVersionsResponse(h RequestHeader) *kmsg.ApiVersionsResponse { //nolint:revive // Kafka spelling (ApiVersions); renaming breaks sim/ callers
	resp := kmsg.NewPtrApiVersionsResponse()
	if !IsSupported(KeyApiVersions, h.APIVersion) {
		resp.ErrorCode = ErrCodeUnsupportedVersion
	}
	for _, r := range supported {
		resp.ApiKeys = append(resp.ApiKeys, kmsg.ApiVersionsResponseApiKey{ApiKey: r.Key, MinVersion: r.Min, MaxVersion: r.Max})
	}
	return resp
}
