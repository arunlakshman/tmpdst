// Command mdscpreset empties the control plane test table and the checkpoint
// folder in the bucket, so an experiment starts from nothing. It keeps the table
// and the bucket. It does not touch other folders of the bucket (bin/, scripts/,
// results/). Throw-away helper for the EC2 experiments, not part of the product.
//
//	mdscpreset -region ap-south-2 -table mds_cpv1_cp -bucket BUCKET [-prefix cp/]
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	batchSize = 25 // DynamoDB allows 25 deletes in one BatchWriteItem
	workers   = 8  // the log is one partition key: about 1,000 writes per second in all
)

func main() {
	region := flag.String("region", "ap-south-2", "AWS region")
	table := flag.String("table", "", "DynamoDB table to empty")
	bucket := flag.String("bucket", "", "S3 bucket (empty: skip S3)")
	prefix := flag.String("prefix", "cp/", "S3 folder to empty")
	flag.Parse()
	if *table == "" || (*bucket != "" && *prefix == "") {
		fmt.Fprintln(os.Stderr, "need -table, and a non-empty -prefix when -bucket is given")
		os.Exit(2)
	}
	ctx := context.Background()
	awsConfig, err := config.LoadDefaultConfig(ctx, config.WithRegion(*region))
	fail(err)

	deleted := emptyTable(ctx, dynamodb.NewFromConfig(awsConfig), *table)
	fmt.Printf("table %s: deleted %d items\n", *table, deleted)
	if *bucket != "" {
		objects := emptyFolder(ctx, s3.NewFromConfig(awsConfig), *bucket, *prefix)
		fmt.Printf("s3://%s/%s: deleted %d objects\n", *bucket, *prefix, objects)
	}
}

// emptyTable scans the keys page by page and deletes them with parallel batches.
func emptyTable(ctx context.Context, db *dynamodb.Client, table string) int64 {
	batches := make(chan []map[string]types.AttributeValue)
	var total atomic.Int64
	var wait sync.WaitGroup
	for range workers {
		wait.Go(func() {
			for keys := range batches {
				deleteBatch(ctx, db, table, keys)
				total.Add(int64(len(keys)))
			}
		})
	}
	var start map[string]types.AttributeValue
	for {
		page, err := db.Scan(ctx, &dynamodb.ScanInput{
			TableName: aws.String(table), ProjectionExpression: aws.String("pk, sk"),
			ExclusiveStartKey: start, ConsistentRead: aws.Bool(true),
		})
		fail(err)
		for i := 0; i < len(page.Items); i += batchSize {
			batches <- page.Items[i:min(i+batchSize, len(page.Items))]
		}
		if page.LastEvaluatedKey == nil {
			break
		}
		start = page.LastEvaluatedKey
	}
	close(batches)
	wait.Wait()
	return total.Load()
}

// deleteBatch deletes the keys. DynamoDB throttles a single partition key
// (the whole log is one), so a throttled or unprocessed batch is tried again
// after a short wait, for up to two minutes.
func deleteBatch(ctx context.Context, db *dynamodb.Client, table string, keys []map[string]types.AttributeValue) {
	requests := make([]types.WriteRequest, len(keys))
	for i, key := range keys {
		requests[i] = types.WriteRequest{DeleteRequest: &types.DeleteRequest{Key: key}}
	}
	deadline := time.Now().Add(2 * time.Minute)
	wait := 50 * time.Millisecond
	for len(requests) > 0 {
		out, err := db.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{RequestItems: map[string][]types.WriteRequest{table: requests}})
		if err == nil {
			requests = out.UnprocessedItems[table]
			if len(requests) == 0 {
				return
			}
		}
		if time.Now().After(deadline) {
			fail(fmt.Errorf("could not delete %d items before the deadline (last error: %w)", len(requests), err))
		}
		time.Sleep(wait)
		wait = min(wait*2, 2*time.Second)
	}
}

func emptyFolder(ctx context.Context, client *s3.Client, bucket, prefix string) int {
	count := 0
	pager := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix)})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		fail(err)
		if len(page.Contents) == 0 {
			continue
		}
		ids := make([]s3types.ObjectIdentifier, len(page.Contents))
		for i, object := range page.Contents {
			ids[i] = s3types.ObjectIdentifier{Key: object.Key}
		}
		_, err = client.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(bucket), Delete: &s3types.Delete{Objects: ids, Quiet: aws.Bool(true)}})
		fail(err)
		count += len(ids)
	}
	return count
}

func fail(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
