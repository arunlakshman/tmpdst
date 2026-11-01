package kafkawire

import (
	"github.com/twmb/franz-go/pkg/kmsg"

	"mds/internal/core"
)

// RequestHeader is core.RequestHeader.
type RequestHeader = core.RequestHeader

// API keys the broker serves.
const (
	KeyProduce         int16 = 0
	KeyFetch           int16 = 1
	KeyListOffsets     int16 = 2
	KeyMetadata        int16 = 3
	KeyOffsetCommit    int16 = 8
	KeyOffsetFetch     int16 = 9
	KeyFindCoordinator int16 = 10
	KeyJoinGroup       int16 = 11
	KeyHeartbeat       int16 = 12
	KeyLeaveGroup      int16 = 13
	KeySyncGroup       int16 = 14
	KeyDescribeGroups  int16 = 15
	KeyListGroups      int16 = 16
	KeyCreateTopics    int16 = 19
	KeyDescribeConfigs int16 = 32
	KeyApiVersions     int16 = 18 //nolint:revive // Kafka spelling (ApiVersions); renaming breaks sim/ callers
)

// ErrCodeUnsupportedVersion is Kafka's UNSUPPORTED_VERSION error code.
const ErrCodeUnsupportedVersion int16 = 35

// APIVersionRange is an API key and its inclusive supported version range.
type APIVersionRange struct{ Key, Min, Max int16 }

//determinismcheck:ok read-only table: no code writes to it after start
var supported = []APIVersionRange{ //nolint:dupl // supported_test.go pins this table with its own copy
	{Key: KeyProduce, Min: 3, Max: 9},
	{Key: KeyFetch, Min: 4, Max: 12},
	{Key: KeyListOffsets, Min: 1, Max: 7},
	{Key: KeyMetadata, Min: 1, Max: 12},
	{Key: KeyCreateTopics, Min: 0, Max: 7},
	{Key: KeyOffsetCommit, Min: 0, Max: 8},
	{Key: KeyOffsetFetch, Min: 0, Max: 8},
	{Key: KeyFindCoordinator, Min: 0, Max: 4},
	{Key: KeyJoinGroup, Min: 0, Max: 7},
	{Key: KeyHeartbeat, Min: 0, Max: 4},
	{Key: KeyLeaveGroup, Min: 0, Max: 5},
	{Key: KeySyncGroup, Min: 0, Max: 5},
	{Key: KeyDescribeGroups, Min: 0, Max: 5},
	{Key: KeyListGroups, Min: 0, Max: 4},
	{Key: KeyDescribeConfigs, Min: 0, Max: 4},
	{Key: KeyApiVersions, Min: 0, Max: 3},
}

// Supported returns the advertised APIs in key order. Produce >= 3 so batches
// are RecordBatch v2. Produce and Fetch stay below v13, which switches to
// topic IDs. The group APIs are the classic protocol only (decision Q9): no
// ConsumerGroupHeartbeat. The result aliases the package registry, so callers
// must not mutate it.
func Supported() []APIVersionRange {
	return supported
}

// Range returns the supported version range of key.
func Range(key int16) (APIVersionRange, bool) {
	for _, r := range supported {
		if r.Key == key {
			return r, true
		}
	}
	return APIVersionRange{}, false
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
	response := kmsg.NewPtrApiVersionsResponse()
	if !IsSupported(KeyApiVersions, h.APIVersion) {
		response.ErrorCode = ErrCodeUnsupportedVersion
	}
	for _, r := range supported {
		response.ApiKeys = append(response.ApiKeys, kmsg.ApiVersionsResponseApiKey{ApiKey: r.Key, MinVersion: r.Min, MaxVersion: r.Max})
	}
	return response
}
