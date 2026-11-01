// Command mdscpinspect prints what the control plane stored in DynamoDB and S3,
// in plain text. It decodes the binary log entries with the real decoder.
// Read only. Throw-away helper, not part of the product.
//
//	mdscpinspect -table mds_cpv1_cp -bucket mds-cpverify-cpv1-478483354994 -region ap-south-2
//	mdscpinspect ... -v            one line per log slot
//	mdscpinspect ... -slot 17      show everything inside one slot
package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"mds/internal/controlplane/stateupdater"
)

const (
	envelopeMarker = 0xC7 // rank and writer name in front of a payload
	groupMarker    = 0xC5 // several commands in one slot
)

var kindNames = []string{"noop", "commit", "create-topic", "group-install", "group-assign", "group-remove", "offset-commit", "create-topic-exclusive"}

func main() {
	table := flag.String("table", "mds_cpv1_cp", "DynamoDB table")
	bucket := flag.String("bucket", "", "S3 bucket (empty: skip S3)")
	region := flag.String("region", "ap-south-2", "AWS region")
	verbose := flag.Bool("v", false, "one line per log slot")
	oneSlot := flag.Int64("slot", -1, "show every command inside this slot")
	flag.Parse()

	ctx := context.Background()
	awsConfig, err := config.LoadDefaultConfig(ctx, config.WithRegion(*region))
	check(err)
	rows := readAll(ctx, dynamodb.NewFromConfig(awsConfig), *table)
	report(rows, *verbose, *oneSlot)
	if *bucket != "" {
		listBucket(ctx, s3.NewFromConfig(awsConfig), *bucket)
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
	slot    int64
	size    int
	rank    uint64
	writer  string
	grouped bool
	entries []stateupdater.Entry
	bad     string
}

func report(rows []row, verbose bool, oneSlot int64) {
	slots, other := decodeRows(rows)
	fmt.Printf("== DynamoDB: %d items, %d log slots\n", len(rows), len(slots))
	for k, n := range other {
		fmt.Printf("   other rows pk=%s: %d\n", k, n)
	}
	if len(slots) == 0 {
		return
	}
	printGaps(slots)
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
	sort.Slice(slots, func(i, j int) bool { return slots[i].slot < slots[j].slot })
	return slots, other
}

func printGaps(slots []slotInfo) {
	gaps := 0
	for i := 1; i < len(slots); i++ {
		if slots[i].slot != slots[i-1].slot+1 {
			gaps++
			fmt.Printf("   GAP between slot %d and %d\n", slots[i-1].slot, slots[i].slot)
		}
	}
	fmt.Printf("   slots %d .. %d, gaps: %d\n", slots[0].slot, slots[len(slots)-1].slot, gaps)
}

func printTotals(slots []slotInfo) {
	kinds, writers := map[string]int{}, map[string]int{}
	records := map[string]int64{} // "topic/partition" -> records committed
	var bytes, bad, biggest int
	for _, s := range slots {
		bytes += s.size
		biggest = max(biggest, s.size)
		writers[fmt.Sprintf("%s(rank %d)", s.writer, s.rank)]++
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
	names := make([]string, 0, len(records))
	for k := range records {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
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
	fmt.Printf("   slot %-6d %5d bytes  writer=%s rank=%d group=%v commands=%d%s %s\n", s.slot, s.size, s.writer, s.rank, s.grouped, len(s.entries), kinds.String(), s.bad)
}

// decodeSlot understands the two wrappers the state machine writes:
// 0xC7 [rank] [writer-length] [writer] then either 0xC5 [id] [count] ([length] [entry])... or one entry.
func decodeSlot(data []byte) slotInfo {
	info := slotInfo{}
	if len(data) > 0 && data[0] == envelopeMarker {
		rank, rest, ok := uvarint(data[1:])
		n, rest, ok2 := uvarint(rest)
		if !ok || !ok2 || n > uint64(len(rest)) {
			info.bad = "BAD envelope"
			return info
		}
		info.rank, info.writer = rank, string(rest[:n])
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
	names := make([]string, 0, len(counts))
	for k := range counts {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
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
