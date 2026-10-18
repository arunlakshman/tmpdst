package dynamo

import (
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"manifester/core"
)

func TestCancelReasons(t *testing.T) {
	tc := &types.TransactionCanceledException{CancellationReasons: []types.CancellationReason{
		{Code: aws.String("None")},
		{Code: aws.String("ConditionalCheckFailed")},
		{Code: aws.String("ThrottlingError")},
	}}
	items := []types.TransactWriteItem{
		{Put: &types.Put{Item: key("G#s", "-")}},
		{Put: &types.Put{Item: runKey(core.TopicPartition{Topic: "t", Partition: 0}, 5)}},
		{ConditionCheck: &types.ConditionCheck{Key: topicKey("t")}},
	}
	codes, detail := cancelReasons(tc, items)
	if !slices.Equal(codes, []string{"ConditionalCheckFailed", "ThrottlingError"}) {
		t.Fatalf("codes %v", codes)
	}
	if detail[0] != "1:ConditionalCheckFailed:R#t#0/00000000000000000005" || detail[1] != "2:ThrottlingError:TOPICS/t" {
		t.Fatalf("detail %v", detail)
	}
}
