package broker

import "manifester/core"

// Bug selects a deliberately broken behaviour for mutant tests. TEST ONLY: the
// zero value is the real broker and production code must never set it.
type Bug uint8

const (
	BugNone Bug = iota
	// BugEarlyAck acks a produce after the S3 PUT, before CommitSegment.
	BugEarlyAck
	// BugNoIdempotence commits batches as core.NoProducer, so client retries
	// are never deduplicated.
	BugNoIdempotence
	// BugWrongOffset patches fetched batches with base offset + 1.
	BugWrongOffset
	// BugServeCorrupt skips the batch CRC check on fetch, so a corrupted GET
	// reaches the client.
	BugServeCorrupt
)

// Config is the broker's tuning. Zero numeric fields take DefaultConfig's value.
type Config struct {
	BrokerID int32  // node id advertised in Metadata
	Host     string // advertised host
	Port     int32  // advertised port

	MaxFrame int32 // largest request frame; larger closes the connection

	BatchWindow     core.Duration // flush timer, armed by the first batch of a segment
	MaxSegmentBytes int           // flush at once when the open segment holds this many batch bytes

	// MaxSegmentPartitions flushes when the open segment spans this many distinct
	// partitions (the DynamoDB control plane rejects ~100 transaction items).
	MaxSegmentPartitions int

	MaxAttempts int           // tries per PUT, commit or read before giving up
	RetryBase   core.Duration // backoff of the first retry; doubles each time
	RetryMax    core.Duration // backoff ceiling

	MaxBufferedBytes    int // pause client reads at or above this many unacked produce bytes
	ResumeBufferedBytes int // resume at or below this (hysteresis)

	DefaultFetchBytes int32 // partition limit when a Fetch asks for none (<= 0)

	// Bug is test-only; see Bug.
	Bug Bug
}

// DefaultConfig is a reasonable single-broker configuration.
func DefaultConfig() Config {
	return Config{
		BrokerID: 1, Host: "localhost", Port: 9092,
		MaxFrame:             16 << 20,
		BatchWindow:          50 * core.Millisecond,
		MaxSegmentBytes:      4 << 20,
		MaxSegmentPartitions: 24,
		MaxAttempts:          6,
		RetryBase:            20 * core.Millisecond,
		RetryMax:             640 * core.Millisecond,
		MaxBufferedBytes:     64 << 20,
		ResumeBufferedBytes:  32 << 20,
		DefaultFetchBytes:    1 << 20,
	}
}

func (c Config) withDefaults() Config {
	d := DefaultConfig()
	if c.Host == "" {
		c.Host = d.Host
	}
	if c.Port == 0 {
		c.Port = d.Port
	}
	if c.MaxFrame <= 0 {
		c.MaxFrame = d.MaxFrame
	}
	if c.BatchWindow <= 0 {
		c.BatchWindow = d.BatchWindow
	}
	if c.MaxSegmentBytes <= 0 {
		c.MaxSegmentBytes = d.MaxSegmentBytes
	}
	if c.MaxSegmentPartitions <= 0 {
		c.MaxSegmentPartitions = d.MaxSegmentPartitions
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = d.MaxAttempts
	}
	if c.RetryBase <= 0 {
		c.RetryBase = d.RetryBase
	}
	if c.RetryMax <= 0 {
		c.RetryMax = d.RetryMax
	}
	if c.MaxBufferedBytes <= 0 {
		c.MaxBufferedBytes = d.MaxBufferedBytes
	}
	if c.ResumeBufferedBytes <= 0 || c.ResumeBufferedBytes > c.MaxBufferedBytes {
		c.ResumeBufferedBytes = c.MaxBufferedBytes / 2
	}
	if c.DefaultFetchBytes <= 0 {
		c.DefaultFetchBytes = d.DefaultFetchBytes
	}
	return c
}

// Kafka error codes the broker returns.
const (
	errNone                 int16 = 0
	errOffsetOutOfRange     int16 = 1
	errCorruptMessage       int16 = 2
	errUnknownTopicOrPart   int16 = 3
	errLeaderNotAvailable   int16 = 5
	errNotLeader            int16 = 6
	errRequestTimedOut      int16 = 7
	errCoordNotAvailable    int16 = 15
	errInvalidRequiredAcks  int16 = 21
	errInvalidRequest       int16 = 42
	errUnsupportedForFormat int16 = 43
	errOutOfOrderSeq        int16 = 45
	errInvalidProducerEpoch int16 = 47
	errInvalidPIDMapping    int16 = 49
	errKafkaStorage         int16 = 56
	errFetchSessionNotFound int16 = 70
	errUnsupportedCompress  int16 = 76
	errUnknownTopicID       int16 = 100
	errUnknownServer        int16 = -1
)
