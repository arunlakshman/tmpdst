package kwire

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kmsg"

	"manifester/core"
)

// ErrMalformed means a frame could not be parsed.
var ErrMalformed = errors.New("kwire: malformed message")

// UnsupportedError is returned by DecodeRequest, together with the parsed
// header, for an API key or version outside Supported.
type UnsupportedError struct {
	Key, Version int16
	UnknownKey   bool // key is not served at all (versus version out of range)
}

func (e *UnsupportedError) Error() string {
	if e.UnknownKey {
		return fmt.Sprintf("kwire: unsupported api key %d", e.Key)
	}
	return fmt.Sprintf("kwire: unsupported version %d of api key %d", e.Version, e.Key)
}

// Codec implements core.Codec on kmsg. It is stateless.
type Codec struct{}

var _ core.Codec = Codec{}

func malformed(format string, a ...any) error {
	return fmt.Errorf("%w: %s", ErrMalformed, fmt.Sprintf(format, a...))
}

// skipTags skips a tagged-field section and returns the bytes after it.
func skipTags(b []byte) ([]byte, error) {
	n, k := binary.Uvarint(b)
	if k <= 0 {
		return nil, malformed("tagged fields")
	}
	b = b[k:]
	for ; n > 0; n-- {
		if _, k = binary.Uvarint(b); k <= 0 {
			return nil, malformed("tag id")
		}
		b = b[k:]
		sz, k := binary.Uvarint(b)
		if k <= 0 || sz > uint64(len(b)-k) {
			return nil, malformed("tag size")
		}
		b = b[k+int(sz):]
	}
	return b, nil
}

// DecodeRequest parses a frame (no length prefix). For an unsupported key or
// version it returns the header and an *UnsupportedError.
func (Codec) DecodeRequest(frame []byte) (core.RequestHeader, core.Request, error) {
	var h core.RequestHeader
	if len(frame) < 10 {
		return h, nil, malformed("short request header")
	}
	h.APIKey = int16(binary.BigEndian.Uint16(frame))
	h.APIVersion = int16(binary.BigEndian.Uint16(frame[2:]))
	h.CorrelationID = int32(binary.BigEndian.Uint32(frame[4:]))
	cl := int16(binary.BigEndian.Uint16(frame[8:]))
	rest := frame[10:]
	switch {
	case cl < -1 || int(cl) > len(rest):
		return h, nil, malformed("client id length")
	case cl > 0:
		h.ClientID = string(rest[:cl])
		rest = rest[cl:]
	}
	if !IsSupported(h.APIKey, h.APIVersion) {
		_, known := Range(h.APIKey)
		return h, nil, &UnsupportedError{Key: h.APIKey, Version: h.APIVersion, UnknownKey: !known}
	}
	req := kmsg.RequestForKey(h.APIKey)
	if req == nil {
		return h, nil, &UnsupportedError{Key: h.APIKey, Version: h.APIVersion, UnknownKey: true}
	}
	req.SetVersion(h.APIVersion)
	if err := readBody(rest, req.IsFlexible(), req.ReadFrom); err != nil {
		return h, nil, err
	}
	return h, req, nil
}

// readBody skips the header tagged fields when flexible, then reads the body.
func readBody(rest []byte, flexible bool, read func([]byte) error) error {
	if flexible {
		var err error
		if rest, err = skipTags(rest); err != nil {
			return err
		}
	}
	if err := read(rest); err != nil {
		return fmt.Errorf("%w: %w", ErrMalformed, err)
	}
	return nil
}

func prefix(dst []byte) []byte { return append(dst, 0, 0, 0, 0) }

// appendFlexibleTag appends an empty header tagged-field section when flexible.
func appendFlexibleTag(b []byte, flexible bool) []byte {
	if flexible {
		return append(b, 0)
	}
	return b
}

func seal(b []byte) []byte {
	binary.BigEndian.PutUint32(b, uint32(len(b)-4))
	return b
}

// respVersion is the version a response is encoded at: ApiVersions replies
// with v0 when the request version is unsupported.
func respVersion(h core.RequestHeader) int16 {
	if h.APIKey == KeyApiVersions && !IsSupported(KeyApiVersions, h.APIVersion) {
		return 0
	}
	return h.APIVersion
}

// respHeaderTags reports whether r's response header carries a tagged-field
// section: flexible versions do, except ApiVersions, which always uses
// response header v0.
func respHeaderTags(h core.RequestHeader, flexible bool) bool {
	return flexible && h.APIKey != KeyApiVersions
}

// EncodeResponse returns length prefix + correlation id + [header tags] + body.
// It sets resp's version (h.APIVersion; v0 for unsupported ApiVersions).
// ApiVersions always uses response header v0 (no tags).
func (Codec) EncodeResponse(h core.RequestHeader, resp core.Response) ([]byte, error) {
	r, ok := resp.(kmsg.Response)
	if !ok || r.Key() != h.APIKey {
		return nil, fmt.Errorf("kwire: response %T does not match api key %d", resp, h.APIKey)
	}
	r.SetVersion(respVersion(h))
	b := prefix(nil)
	b = binary.BigEndian.AppendUint32(b, uint32(h.CorrelationID))
	b = appendFlexibleTag(b, respHeaderTags(h, r.IsFlexible()))
	return seal(r.AppendTo(b)), nil
}

// EncodeRequest returns length prefix + request header + body, setting req's
// version to h.APIVersion. The client id is encoded as a (non-null) string.
func (Codec) EncodeRequest(h core.RequestHeader, req core.Request) ([]byte, error) {
	r, ok := req.(kmsg.Request)
	if !ok || r.Key() != h.APIKey {
		return nil, fmt.Errorf("kwire: request %T does not match api key %d", req, h.APIKey)
	}
	if len(h.ClientID) > 1<<15-1 {
		return nil, errors.New("kwire: client id too long")
	}
	r.SetVersion(h.APIVersion)
	b := prefix(nil)
	b = binary.BigEndian.AppendUint16(b, uint16(h.APIKey))
	b = binary.BigEndian.AppendUint16(b, uint16(h.APIVersion))
	b = binary.BigEndian.AppendUint32(b, uint32(h.CorrelationID))
	b = binary.BigEndian.AppendUint16(b, uint16(len(h.ClientID)))
	b = append(b, h.ClientID...)
	b = appendFlexibleTag(b, r.IsFlexible())
	return seal(r.AppendTo(b)), nil
}

// DecodeResponse parses a response frame (no length prefix) to the request
// described by h, checking the correlation id.
func (Codec) DecodeResponse(h core.RequestHeader, frame []byte) (core.Response, error) {
	if len(frame) < 4 {
		return nil, malformed("short response header")
	}
	if c := int32(binary.BigEndian.Uint32(frame)); c != h.CorrelationID {
		return nil, malformed("correlation id %d, want %d", c, h.CorrelationID)
	}
	resp := kmsg.ResponseForKey(h.APIKey)
	if resp == nil {
		return nil, &UnsupportedError{Key: h.APIKey, Version: h.APIVersion, UnknownKey: true}
	}
	resp.SetVersion(respVersion(h))
	if err := readBody(frame[4:], respHeaderTags(h, resp.IsFlexible()), resp.ReadFrom); err != nil {
		return nil, err
	}
	return resp, nil
}
