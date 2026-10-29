package core

// Kafka wire handling runs inside the deterministic path (decision A). The
// implementations wrap franz-go's pure kmsg package; kgo is never used here.

// Request is satisfied by kmsg.Request.
type Request interface {
	Key() int16
	GetVersion() int16
}

// Response is satisfied by kmsg.Response.
type Response interface {
	Key() int16
	GetVersion() int16
}

// RequestHeader is the Kafka request header needed to route and answer.
type RequestHeader struct {
	APIKey        int16
	APIVersion    int16
	CorrelationID int32
	ClientID      string
}

// FrameSplitter reassembles length-prefixed Kafka frames from a byte stream. One per
// connection; it is part of the owning state machine's state. Pure.
type FrameSplitter interface {
	// Push appends data and returns every complete frame now available, in
	// order. An error means the stream is unrecoverable; close the connection.
	Push(data []byte) (frames [][]byte, err error)
	// Buffered reports bytes held waiting for the rest of a frame.
	Buffered() int
}

// KafkaCodec decodes requests and encodes responses (broker side) and the reverse
// (sim workload side). Pure and deterministic.
type KafkaCodec interface {
	DecodeRequest(frame []byte) (RequestHeader, Request, error)
	EncodeResponse(h RequestHeader, resp Response) ([]byte, error)

	EncodeRequest(h RequestHeader, req Request) ([]byte, error)
	DecodeResponse(h RequestHeader, frame []byte) (Response, error)
}
