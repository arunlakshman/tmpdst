package logcp_test

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"manifester/core"
	"manifester/cp/fakedb"
	"manifester/cp/logcp"
	"manifester/cp/logcp/logcptest"
	"manifester/cp/logsm"
)

// TestDumpSampleRows prints every item of the DynamoDB table of a small
// dynamolog control plane in readable form (markdown on stdout). It is skipped
// unless DUMP_ROWS=1:
//
//	DUMP_ROWS=1 go test ./cp/logcp -run TestDumpSampleRows -v
func TestDumpSampleRows(t *testing.T) {
	if os.Getenv("DUMP_ROWS") != "1" {
		t.Skip("set DUMP_ROWS=1 to dump sample rows")
	}
	db, objs := newDB(t), logcptest.NewObjects()
	log := logcp.NewDynLog(db, "cp", logcp.DynLogOptions{TTL: 7 * 24 * time.Hour})
	s := node(t, "n0", 0, log, db, objs, nil)
	if code, err := s.CreateTopic(bg, "orders", 4); err != nil || code != core.OK {
		t.Fatal(code, err)
	}
	shapes := [][]int32{{0}, {0, 1, 0}, {2, 3}, {1, 1, 1, 1}, {0, 1, 2, 3}, {3}, {2, 2, 0}, {1, 3, 0, 2, 1}, {0, 0}, {2}}
	for i, parts := range shapes {
		key := fmt.Sprintf("seg/broker-%d/%06d.seg", i%3+1, i+1)
		c := core.CommitSegment{SegmentKey: key}
		off := int64(0)
		for j, p := range parts {
			n, ln := int64(10+3*j+i), int64(2048+100*i)
			c.Batches = append(c.Batches, core.PendingBatch{TP: tp("orders", p), Records: n, Loc: core.SegmentRef{Key: key, Off: off, Len: ln}})
			off += ln
		}
		commit(t, s, c)
	}
	if _, err := s.Checkpoint(bg); err != nil {
		t.Fatal(err)
	}

	var out strings.Builder
	w := func(f string, a ...any) { fmt.Fprintf(&out, f+"\n", a...) }
	sizes := map[string][]int{}
	w("### Log slots (pk=LOG)")
	for _, spec := range []struct {
		pk, typ string
		keep    []int // indexes shown; negative counts from the end
	}{{"LOG", "LOG", []int{0, 1, 2, -1}}} {
		items := queryAll(t, db, spec.pk)
		shown := map[int]bool{}
		for _, i := range spec.keep {
			shown[(i+len(items))%len(items)] = true
		}
		for i, it := range items {
			sizes[spec.typ] = append(sizes[spec.typ], itemSize(it))
			if shown[i] {
				w("%s", describe(t, it))
			} else if shown[i-1] || i == 0 {
				w("... (%d more %s rows)", len(items)-len(shown), spec.pk)
			}
		}
	}
	w("\n### Pointer and bookkeeping rows")
	show := func(typ string, it map[string]types.AttributeValue, body string) {
		sizes[typ] = append(sizes[typ], itemSize(it))
		w("%s", body)
	}
	for _, f := range []struct{ pk, sk, typ string }{{"CKPT", "-", "CKPT"}} {
		it := db.Item("cp", map[string]types.AttributeValue{"pk": sv(f.pk), "sk": sv(f.sk)})
		if it == nil {
			t.Fatalf("missing %s", f.pk)
		}
		show(f.typ, it, describe(t, it))
	}
	w("\n### Item sizes (bytes, DynamoDB accounting: attribute names + values)\n")
	w("| item type | count | min | max |\n|---|---|---|---|")
	var typeNames []string
	for k := range sizes {
		typeNames = append(typeNames, k)
	}
	sort.Strings(typeNames)
	for _, k := range typeNames {
		mn, mx := sizes[k][0], sizes[k][0]
		for _, v := range sizes[k] {
			mn, mx = min(mn, v), max(mx, v)
		}
		w("| %s | %d | %d | %d |", k, len(sizes[k]), mn, mx)
	}
	fmt.Print(out.String())
}

func sv(v string) types.AttributeValue { return &types.AttributeValueMemberS{Value: v} }

func queryAll(t testing.TB, db *fakedb.Fake, pk string) []map[string]types.AttributeValue {
	t.Helper()
	var all []map[string]types.AttributeValue
	var start map[string]types.AttributeValue
	for {
		o, err := db.Query(bg, &dynamodb.QueryInput{
			TableName:                 aws.String("cp"),
			KeyConditionExpression:    aws.String("pk = :p AND sk BETWEEN :lo AND :hi"),
			ExpressionAttributeValues: map[string]types.AttributeValue{":p": sv(pk), ":lo": sv("0"), ":hi": sv("99999999999999999999")},
			ExclusiveStartKey:         start,
		})
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, o.Items...)
		if start = o.LastEvaluatedKey; start == nil {
			return all
		}
	}
}

// itemSize is the DynamoDB item size: attribute names plus value bytes.
func itemSize(it map[string]types.AttributeValue) int {
	n := 0
	for k, v := range it {
		n += len(k)
		switch x := v.(type) {
		case *types.AttributeValueMemberS:
			n += len(x.Value)
		case *types.AttributeValueMemberN:
			n += len(x.Value)/2 + 1
		case *types.AttributeValueMemberB:
			n += len(x.Value)
		}
	}
	return n
}

func attr(it map[string]types.AttributeValue, k string) string {
	switch x := it[k].(type) {
	case *types.AttributeValueMemberS:
		return x.Value
	case *types.AttributeValueMemberN:
		return x.Value
	case *types.AttributeValueMemberB:
		return fmt.Sprintf("<%d bytes>", len(x.Value))
	}
	return ""
}

func describe(t testing.TB, it map[string]types.AttributeValue) string {
	var b strings.Builder
	pk, sk := attr(it, "pk"), attr(it, "sk")
	var attrs []string
	for _, k := range []string{"w", "id", "ts", "n", "v", "ttl", "base", "cnt", "runs", "d", "ix_slot", "ix_ts", "parts", "ck_slot", "ck_key", "ck_size", "ck_sum", "ck_ts"} {
		if _, ok := it[k]; ok {
			attrs = append(attrs, k+"="+attr(it, k))
		}
	}
	fmt.Fprintf(&b, "\n**pk=`%s` sk=`%s`** (%d bytes): %s", pk, sk, itemSize(it), strings.Join(attrs, " "))
	if pk == "LOG" {
		raw := it["d"].(*types.AttributeValueMemberB).Value
		e, err := logcp.UnmarshalEntry(raw)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(&b, "\n- d: version byte %d, rank %d, writer %q", raw[0], e.Rank, e.Writer)
		for _, c := range e.Cmds {
			if c.Kind == logsm.KindCreateTopic {
				fmt.Fprintf(&b, "\n- cmd CreateTopic topic=%q parts=%d", c.Topic, c.Parts)
				continue
			}
			fmt.Fprintf(&b, "\n- cmd Commit segment=%q:", c.Commit.SegmentKey)
			for _, pb := range c.Commit.Batches {
				fmt.Fprintf(&b, " [%s/%d rec=%d off=%d len=%d]", pb.TP.Topic, pb.TP.Partition, pb.Records, pb.Loc.Off, pb.Loc.Len)
			}
		}
	}
	return b.String()
}
