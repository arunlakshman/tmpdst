// Command mdscpinspect prints what the control plane stored in DynamoDB and S3,
// in plain text. It decodes the binary log entries with the real decoder.
// Read only. Throw-away helper, not part of the product.
//
//	mdscpinspect -table mds_cpv1_cp -bucket mds-cpverify-cpv1-478483354994 -region ap-south-2
//	mdscpinspect ... -v            one line per log slot
//	mdscpinspect ... -slot 17      show everything inside one slot
package main

import (
	"cmp"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/manifesterio/mds/internal/controlplane/statemachine"
	"github.com/manifesterio/mds/internal/controlplane/stateupdater"
)

// groupMarker starts a slot with several commands. The envelope (epoch and
// writer name in front of a payload) has no marker here: statemachine.EntryWriter
// checks it, and its marker byte is private to that package.
const groupMarker = 0xC5

var kindNames = []string{"noop", "commit", "create-topic", "group-install", "group-assign", "group-remove", "offset-commit", "create-topic-exclusive"}

func main() {
	table := flag.String("table", "mds_cpv1_cp", "DynamoDB table")
	bucket := flag.String("bucket", "", "S3 bucket (empty: skip S3)")
	region := flag.String("region", "ap-south-2", "AWS region")
	verbose := flag.Bool("v", false, "one line per log slot")
	oneSlot := flag.Int64("slot", -1, "show every command inside this slot")
	head := flag.Int("head", 0, "show the first N log slots, decoded")
	tail := flag.Int("tail", 0, "show the last N log slots, decoded")
	doCheck := flag.Bool("check", false, "verify the stored log and checkpoint; exit 1 if any check fails")
	checkpointPrefix := flag.String("checkpoint-prefix", "cp/checkpoint/", "S3 key prefix of the checkpoints; the newest one is checked")
	ackedPath := flag.String("acked", "", "with -check: the acked file that mdscpverify -acked-out wrote; checks that every acked commit is in the log with the same offsets")
	flag.Parse()

	ctx := context.Background()
	awsConfig, err := config.LoadDefaultConfig(ctx, config.WithRegion(*region))
	check(err)
	db := dynamodb.NewFromConfig(awsConfig)
	rows := readAll(ctx, db, *table)
	meta := readMeta(ctx, db, *table)
	printMeta(meta)
	s3Client := s3.NewFromConfig(awsConfig)
	if *doCheck {
		runChecks(ctx, rows, meta, s3Client, *bucket, *checkpointPrefix, *ackedPath)
		return
	}
	report(rows, *verbose, *oneSlot, *head, *tail)
	if *bucket != "" {
		listBucket(ctx, s3Client, *bucket)
	}
}

type row struct {
	partitionKey, sortKey string
	data                  []byte
	ttl                   int64
}

func readAll(ctx context.Context, db *dynamodb.Client, table string) []row {
	var rows []row
	var start map[string]types.AttributeValue
	for {
		out, err := db.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String(table), ExclusiveStartKey: start})
		check(err)
		for _, item := range out.Items {
			r := row{}
			if v, ok := item["pk"].(*types.AttributeValueMemberS); ok {
				r.partitionKey = v.Value
			}
			if v, ok := item["sk"].(*types.AttributeValueMemberS); ok {
				r.sortKey = v.Value
			}
			if v, ok := item["d"].(*types.AttributeValueMemberB); ok {
				r.data = v.Value
			}
			if v, ok := item["ttl"].(*types.AttributeValueMemberN); ok {
				r.ttl, _ = strconv.ParseInt(v.Value, 10, 64)
			}
			rows = append(rows, r)
		}
		if out.LastEvaluatedKey == nil {
			return rows
		}
		start = out.LastEvaluatedKey
	}
}

// slotInfo is one decoded log slot.
type slotInfo struct {
	slot      int64
	size      int
	epoch     uint64 // writer epoch from the envelope; 0 when there is none
	writer    string // writer name from the envelope
	enveloped bool   // false for entries written without an envelope
	grouped   bool
	entries   []stateupdater.Entry
	bad       string
}

func report(rows []row, verbose bool, oneSlot int64, head, tail int) {
	slots, other := decodeRows(rows)
	fmt.Printf("== DynamoDB: %d items, %d log slots\n", len(rows), len(slots))
	for k, n := range other {
		fmt.Printf("   other rows pk=%s: %d\n", k, n)
	}
	if len(slots) == 0 {
		return
	}
	printMissingSlots(slots)
	printTotals(slots)
	for _, r := range rows {
		if r.partitionKey == "LOG" && r.ttl > 0 {
			fmt.Printf("   log item ttl: expires %s\n", time.Unix(r.ttl, 0).Format(time.RFC1123))
			break
		}
	}
	if verbose {
		fmt.Println("== slots")
		for _, s := range slots {
			printSlot(s)
		}
	}
	if oneSlot >= 0 {
		for _, s := range slots {
			if s.slot == oneSlot {
				printSlotDetail(s)
			}
		}
	}
	printEnds(slots, head, tail)
}

// printEnds shows the first and last few slots with every command decoded.
func printEnds(slots []slotInfo, head, tail int) {
	if head > 0 {
		fmt.Printf("== first %d slots\n", min(head, len(slots)))
		for _, s := range slots[:min(head, len(slots))] {
			printSlotDetail(s)
		}
	}
	if tail > 0 {
		from := max(0, len(slots)-tail)
		fmt.Printf("== last %d slots\n", len(slots)-from)
		for _, s := range slots[from:] {
			printSlotDetail(s)
		}
	}
}

// decodeRows turns LOG rows into sorted slots. Rows of other partitions are only counted.
func decodeRows(rows []row) ([]slotInfo, map[string]int) {
	var slots []slotInfo
	other := map[string]int{}
	for _, r := range rows {
		if r.partitionKey != "LOG" {
			other[r.partitionKey]++
			continue
		}
		n, err := strconv.ParseInt(r.sortKey, 10, 64)
		check(err)
		info := decodeSlot(r.data)
		info.slot, info.size = n, len(r.data)
		slots = append(slots, info)
	}
	slices.SortFunc(slots, func(a, b slotInfo) int { return cmp.Compare(a.slot, b.slot) })
	return slots, other
}

func printMissingSlots(slots []slotInfo) {
	missing := 0
	for i := 1; i < len(slots); i++ {
		if slots[i].slot != slots[i-1].slot+1 {
			missing++
			fmt.Printf("   MISSING slots between %d and %d\n", slots[i-1].slot, slots[i].slot)
		}
	}
	fmt.Printf("   slots %d .. %d, missing: %d\n", slots[0].slot, slots[len(slots)-1].slot, missing)
}

func printTotals(slots []slotInfo) {
	kinds, writers := map[string]int{}, map[string]int{}
	records := map[string]int64{} // "topic/partition" -> records committed
	var bytes, bad, biggest int
	for _, s := range slots {
		bytes += s.size
		biggest = max(biggest, s.size)
		writers[fmt.Sprintf("%s(epoch %d)", s.writer, s.epoch)]++
		if s.bad != "" {
			bad++
		}
		for _, e := range s.entries {
			kinds[kindName(e.Kind)]++
			for _, b := range e.Commit.Batches {
				records[fmt.Sprintf("%s/%d", b.TopicPartition.Topic, b.TopicPartition.Partition)] += b.Records
			}
		}
	}
	fmt.Printf("   bytes in log: %d (biggest slot %d, DynamoDB limit 409600), undecodable slots: %d\n", bytes, biggest, bad)
	printCounts("   commands by kind", kinds)
	printCounts("   slots by writer", writers)
	fmt.Println("   records committed per topic/partition (the end offset must equal this):")
	for _, k := range slices.Sorted(maps.Keys(records)) {
		fmt.Printf("     %-28s %d\n", k, records[k])
	}
}

func printSlotDetail(s slotInfo) {
	printSlot(s)
	for _, e := range s.entries {
		fmt.Printf("     %s id=%s topic=%q parts=%d\n", kindName(e.Kind), e.ID, e.Topic, e.Parts)
		for _, b := range e.Commit.Batches {
			fmt.Printf("       batch %s/%d records=%d at %s[%d:+%d]\n", b.TopicPartition.Topic, b.TopicPartition.Partition, b.Records, b.Loc.Key, b.Loc.Off, b.Loc.Len)
		}
	}
}

func printSlot(s slotInfo) {
	var kinds strings.Builder
	for _, e := range s.entries {
		kinds.WriteString(" " + kindName(e.Kind))
	}
	fmt.Printf("   slot %-6d %5d bytes  writer=%s epoch=%d group=%v commands=%d%s %s\n", s.slot, s.size, s.writer, s.epoch, s.grouped, len(s.entries), kinds.String(), s.bad)
}

// decodeSlot understands the two wrappers the state machine writes:
// 0xC7 [epoch] [writer-length] [writer] then either 0xC5 [id] [count] ([length] [entry])... or one entry.
// The epoch and writer come from statemachine.EntryWriter, the same decoder the node uses.
func decodeSlot(data []byte) slotInfo {
	info := slotInfo{}
	info.epoch, info.writer, info.enveloped = statemachine.EntryWriter(data)
	if info.enveloped {
		// EntryWriter has checked the header, so the payload starts after the writer name.
		_, rest, _ := uvarint(data[1:]) // epoch
		n, rest, _ := uvarint(rest)     // writer name length
		data = rest[n:]
	}
	if len(data) > 0 && data[0] == groupMarker {
		info.grouped = true
		rest := data[1:]
		idLength, rest, ok := uvarint(rest)
		if !ok || idLength > uint64(len(rest)) {
			info.bad = "BAD group header"
			return info
		}
		count, rest, ok := uvarint(rest[idLength:])
		if !ok {
			info.bad = "BAD group count"
			return info
		}
		for range count {
			length, r2, ok := uvarint(rest)
			if !ok || length > uint64(len(r2)) {
				info.bad = "BAD group entry"
				return info
			}
			entry, err := stateupdater.Decode(r2[:length])
			if err != nil {
				info.bad = "BAD entry: " + err.Error()
				return info
			}
			info.entries = append(info.entries, entry)
			rest = r2[length:]
		}
		return info
	}
	entry, err := stateupdater.Decode(data)
	if err != nil {
		info.bad = "BAD entry: " + err.Error()
		return info
	}
	info.entries = []stateupdater.Entry{entry}
	return info
}

func listBucket(ctx context.Context, client *s3.Client, bucket string) {
	out, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	check(err)
	fmt.Printf("== S3 bucket %s: %d objects\n", bucket, len(out.Contents))
	for _, o := range out.Contents {
		fmt.Printf("   %-40s %8d bytes  modified %s\n", aws.ToString(o.Key), aws.ToInt64(o.Size), o.LastModified.Local().Format("15:04:05"))
	}
}

func printCounts(title string, counts map[string]int) {
	fmt.Println(title + ":")
	for _, k := range slices.Sorted(maps.Keys(counts)) {
		fmt.Printf("     %-28s %d\n", k, counts[k])
	}
}

func kindName(k stateupdater.Kind) string {
	if int(k) < len(kindNames) {
		return kindNames[k]
	}
	return fmt.Sprintf("kind-%d", k)
}

func uvarint(b []byte) (uint64, []byte, bool) {
	v, n := binary.Uvarint(b)
	if n <= 0 {
		return 0, b, false
	}
	return v, b[n:], true
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
