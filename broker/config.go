package broker

import "manifester/core"

// Bug selects a deliberately broken behaviour for mutant tests. TEST ONLY: the
// zero value is the real broker and production code must never set it.
type Bug uint8

const (
	// BugNone is the real, unbroken broker.
	BugNone Bug = iota
	// BugEarlyAck acks a produce after the S3 PUT, before CommitSegment.
	BugEarlyAck
	// BugWrongOffset patches fetched batches with base offset + 1.
	BugWrongOffset
	// BugServeCorrupt skips the batch CRC check on fetch, so a corrupted GET
	// reaches the client.
	BugServeCorrupt
	// BugNoBatchCap ignores MaxSegmentBatches, so a segment can outgrow the
	// control plane's commit limit.
	BugNoBatchCap
	// BugTimerFromLastBatch restarts the flush timer on every batch, so a steady
	// trickle never flushes by the timer.
	BugTimerFromLastBatch
	// BugSealOffByOne applies the size rule one batch late.
	BugSealOffByOne
	// BugNoPartitionCap ignores MaxSegmentPartitions, so a segment can outgrow
	// the partition cap (and, on the DynamoDB transaction store, its commit
	// item budget 1 + partitions).
	BugNoPartitionCap
	// BugFreshKeyOnRetry retries a CommitSegment with a fresh SegmentKey, so a
	// commit that was applied but lost is applied again and the batches are
	// committed twice.
	BugFreshKeyOnRetry
	// BugSkipProduceCRC admits a produced batch without checking its CRC, so a
	// corrupted (compressed) batch is acked and stored.
	BugSkipProduceCRC
)

// Config is the broker's tuning. Zero numeric fields take DefaultConfig's value.
type Config struct {
	BrokerID int32  // node id advertised in Metadata
	Host     string // advertised host
	Port     int32  // advertised port

	// MaxFrame is the largest request frame; a larger one closes the
	// connection. The default is 100 MiB, Kafka's socket.request.max.bytes, so a
	// client with max.request.size=64000000 (the WarpStream benchmark config) can
	// send a 64 MB Produce request (decision 45).
	MaxFrame int32

	// Segment flush rule (decision 35): the open segment is sealed when it holds
	// MaxSegmentBytes of batch data, or BatchWindow after its first batch
	// arrived (a fixed timer, not extended by later batches), or when it hits
	// one of the count caps below.
	BatchWindow     core.Duration // flush timer, armed by the first batch of a segment
	MaxSegmentBytes int           // flush at once when the open segment holds this many batch bytes

	// MaxSegmentPartitions and MaxSegmentBatches cap the open segment so the
	// control plane can commit it. The cap is a per-store setting (decision 52):
	//
	//	log store (machine/exec) one command of about 20 bytes per partition in a
	//	                         log entry, so the default is 4096 (about 80 KB,
	//	                         far under the 350 KB entry guard). No item limit.
	//
	// MaxSegmentBatches bounds the per-segment bookkeeping and the log entry
	// size. The broker cannot know the CP: keep these in step with it
	// (cp/doc.go).
	MaxSegmentPartitions int // seal at this many distinct partitions
	MaxSegmentBatches    int // seal at this many batches

	MaxAttempts int           // tries per PUT, commit or read before giving up
	RetryBase   core.Duration // backoff of the first retry; doubles each time
	RetryMax    core.Duration // backoff ceiling

	// CommitDeadline bounds how long one segment's commit is retried (audit
	// item 8). A CP outage (restart, host replacement) must not fail every
	// in-flight produce, so a commit is retried without a count limit, with
	// capped exponential backoff and jitter (CommitRetryMax is the cap), until
	// this much time has passed since its first CommitSegment; then the
	// segment's batches fail with REQUEST_TIMED_OUT and its buffer is
	// released. Keep it under the producers' delivery.timeout.ms. Meanwhile
	// the buffer stays held, so backpressure pauses client reads as usual.
	CommitDeadline core.Duration
	CommitRetryMax core.Duration

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
		MaxFrame:             100 << 20,
		BatchWindow:          200 * core.Millisecond,
		MaxSegmentBytes:      16 << 20,
		MaxSegmentPartitions: 4096,
		MaxSegmentBatches:    256,
		MaxAttempts:          6,
		RetryBase:            20 * core.Millisecond,
		RetryMax:             640 * core.Millisecond,
		CommitDeadline:       120 * core.Second,
		CommitRetryMax:       2 * core.Second,
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
	if c.MaxSegmentBatches <= 0 {
		c.MaxSegmentBatches = d.MaxSegmentBatches
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
	if c.CommitDeadline <= 0 {
		c.CommitDeadline = d.CommitDeadline
	}
	if c.CommitRetryMax <= 0 {
		c.CommitRetryMax = d.CommitRetryMax
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

// kafkaCode is a Kafka protocol error code returned on responses.
type kafkaCode int16

const (
	errNone                 kafkaCode = 0
	errOffsetOutOfRange     kafkaCode = 1
	errCorruptMessage       kafkaCode = 2
	errUnknownTopicOrPart   kafkaCode = 3
	errLeaderNotAvailable   kafkaCode = 5
	errNotLeader            kafkaCode = 6
	errRequestTimedOut      kafkaCode = 7
	errInvalidRequiredAcks  kafkaCode = 21
	errInvalidRequest       kafkaCode = 42
	errUnsupportedForFormat kafkaCode = 43
	errKafkaStorage         kafkaCode = 56
	errFetchSessionNotFound kafkaCode = 70
	errUnsupportedCompress  kafkaCode = 76
	errUnknownTopicID       kafkaCode = 100
	errUnknownServer        kafkaCode = -1
)
