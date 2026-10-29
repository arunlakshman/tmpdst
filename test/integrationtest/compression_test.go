package integrationtest

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"manifester/internal/wire/kafkawire"
)

// codecCase is one producer compression setting. id is the codec id expected
// on the stored batches (-1: kgo's default, whatever it picks, only checked to
// round-trip).
type codecCase struct {
	name string
	opt  []kgo.Opt
	id   int
}

func codecCases() []codecCase {
	co := func(c kgo.CompressionCodec) []kgo.Opt { return []kgo.Opt{kgo.ProducerBatchCompression(c)} }
	return []codecCase{
		{"none", co(kgo.NoCompression()), kafkawire.CodecNone},
		{"gzip", co(kgo.GzipCompression()), kafkawire.CodecGzip},
		{"snappy", co(kgo.SnappyCompression()), kafkawire.CodecSnappy},
		{"lz4", co(kgo.Lz4Compression()), kafkawire.CodecLZ4},
		{"zstd", co(kgo.ZstdCompression()), kafkawire.CodecZstd},
		{"kgo-default", nil, -1},
	}
}

// bigValue is compressible, so every real codec shrinks the batch and kgo
// keeps the compressed form.
func bigValue(topic string, p int32, i int) []byte {
	return []byte(string(value(topic, p, i)) + strings.Repeat("compressible-", 150))
}

// runCompression produces with every codec, reads the batches back raw to see
// the codec id the broker stored and served, then consumes (same client) and
// checks every value. One client per codec, closed before the next, so the
// suite does not accumulate dials.
func runCompression(t *testing.T, s *stack) {
	const n, parts = 8, 2 // functional: codec + offsets, not volume
	for _, cc := range codecCases() {
		topic := "cmp-" + cc.name
		s.createTopic(topic, parts)
		cl := newClient(t, s.seeds(), cc.opt...)
		ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
		acked := newSent()
		for i := range n {
			for p := range int32(parts) {
				cl.Produce(ctx, &kgo.Record{Topic: topic, Partition: p, Value: bigValue(topic, p, i)}, acked.promise)
			}
		}
		if err := cl.Flush(ctx); err != nil || len(acked.fails) > 0 {
			t.Fatalf("%s: flush %v, failures %v", cc.name, err, acked.fails)
		}
		if cc.id >= 0 {
			checkStoredCodec(ctx, t, cl, topic, cc)
		}
		cancel()
		got := consumeFrom(t, cl, map[tp]int{{topic, 0}: n, {topic, 1}: n})
		closeClient(cl)
		for k, recs := range got {
			for i, r := range recs {
				if want := bigValue(topic, k.part, i); string(r.Value) != string(want) || r.Offset != int64(i) {
					t.Fatalf("%s %v record %d: offset %d, value mismatch", cc.name, k, i, r.Offset)
				}
			}
		}
	}
}

// checkStoredCodec fetches partition 0 raw and requires every batch to carry
// the producer's codec id, with the broker's patched offsets.
func checkStoredCodec(ctx context.Context, t *testing.T, cl *kgo.Client, topic string, cc codecCase) {
	t.Helper()
	req := kmsg.NewPtrFetchRequest()
	req.MaxBytes, req.MaxWaitMillis = 8<<20, 100
	rt := kmsg.NewFetchRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewFetchRequestTopicPartition()
	rp.PartitionMaxBytes, rp.CurrentLeaderEpoch = 8<<20, -1
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)
	resp, err := cl.Request(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	raw := resp.(*kmsg.FetchResponse).Topics[0].Partitions[0].RecordBatches
	bs, err := kafkawire.SplitBatches(raw)
	if err != nil || len(bs) == 0 {
		t.Fatalf("%s: raw fetch: %v, %d batches", cc.name, err, len(bs))
	}
	next := int64(0)
	for _, b := range bs {
		bt, err := kafkawire.ParseBatch(b)
		if err != nil || bt.Codec() != cc.id || bt.BaseOffset != next {
			t.Fatalf("%s: stored batch codec %d (want %d) base %d (want %d): %v", cc.name, bt.Codec(), cc.id, bt.BaseOffset, next, err)
		}
		next += int64(bt.NumRecords)
	}
}

func TestCompressionRoundTrip(t *testing.T) { runCompression(t, memStack(t, 2)) }

func TestFullStackCompression(t *testing.T) {
	s := fullStack(t)
	s.addBrokers(2)
	runCompression(t, s)
}
