package kafkawire

import (
	"testing"

	"manifester/internal/core"
)

var wantSupported = []APIVersionRange{
	{Key: KeyProduce, Min: 3, Max: 9},
	{Key: KeyFetch, Min: 4, Max: 12},
	{Key: KeyListOffsets, Min: 1, Max: 7},
	{Key: KeyMetadata, Min: 1, Max: 12},
	{Key: KeyApiVersions, Min: 0, Max: 3},
}

func TestSupported(t *testing.T) {
	t.Parallel()
	got := Supported()
	if len(got) != len(wantSupported) {
		t.Fatalf("Supported length %d, want %d", len(got), len(wantSupported))
	}
	for i := range wantSupported {
		if got[i] != wantSupported[i] {
			t.Fatalf("Supported()[%d] = %+v, want %+v", i, got[i], wantSupported[i])
		}
	}
}

func TestRange(t *testing.T) {
	t.Parallel()
	for _, want := range wantSupported {
		got, ok := Range(want.Key)
		if !ok || got != want {
			t.Fatalf("Range(%d) = %+v, %v", want.Key, got, ok)
		}
	}
	if got, ok := Range(22); ok || got != (APIVersionRange{}) {
		t.Fatalf("Range(22) = %+v, %v", got, ok)
	}
}

func TestIsSupported(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		key     int16
		version int16
		want    bool
	}{
		{name: "unsupported key", key: 22, version: 0, want: false},
		{name: "below min", key: KeyProduce, version: 2, want: false},
		{name: "above max", key: KeyFetch, version: 13, want: false},
		{name: "exact min", key: KeyProduce, version: 3, want: true},
		{name: "exact max", key: KeyProduce, version: 9, want: true},
		{name: "api versions min", key: KeyApiVersions, version: 0, want: true},
		{name: "api versions max", key: KeyApiVersions, version: 3, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := IsSupported(tt.key, tt.version); got != tt.want {
				t.Fatalf("IsSupported(%d, %d) = %v, want %v", tt.key, tt.version, got, tt.want)
			}
		})
	}
}

func TestApiVersionsResponse(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		version int16
		err     int16
	}{
		{name: "min version", version: 0, err: 0},
		{name: "max version", version: 3, err: 0},
		{name: "above max", version: 4, err: ErrCodeUnsupportedVersion},
		{name: "far above max", version: 99, err: ErrCodeUnsupportedVersion},
		{name: "below min", version: -1, err: ErrCodeUnsupportedVersion},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			resp := ApiVersionsResponse(core.RequestHeader{APIKey: KeyApiVersions, APIVersion: tt.version})
			if resp.ErrorCode != tt.err {
				t.Fatalf("ErrorCode = %d, want %d", resp.ErrorCode, tt.err)
			}
			if len(resp.ApiKeys) != len(wantSupported) {
				t.Fatalf("ApiKeys length %d, want %d", len(resp.ApiKeys), len(wantSupported))
			}
			for i, want := range wantSupported {
				got := resp.ApiKeys[i]
				if got.ApiKey != want.Key || got.MinVersion != want.Min || got.MaxVersion != want.Max {
					t.Fatalf("ApiKeys[%d] = key %d min %d max %d, want key %d min %d max %d",
						i, got.ApiKey, got.MinVersion, got.MaxVersion, want.Key, want.Min, want.Max)
				}
			}
		})
	}
}
