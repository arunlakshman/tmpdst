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

// Supported returns the advertised APIs in key order. Produce >= 3 so batches
// are RecordBatch v2. Produce and Fetch stay below v13, which switches to
// topic IDs.
func Supported() []APIRange {
	return []APIRange{
		{KeyProduce, 3, 9},
		{KeyFetch, 4, 12},
		{KeyListOffsets, 1, 7},
		{KeyMetadata, 1, 12},
		{KeyApiVersions, 0, 3},
	}
}

// Range returns the supported version range of key.
func Range(key int16) (APIRange, bool) {
	for _, r := range Supported() {
		if r.Key == key {
			return r, true
		}
	}
	return APIRange{}, false
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
	for _, r := range Supported() {
		resp.ApiKeys = append(resp.ApiKeys, kmsg.ApiVersionsResponseApiKey{ApiKey: r.Key, MinVersion: r.Min, MaxVersion: r.Max})
	}
	return resp
}
