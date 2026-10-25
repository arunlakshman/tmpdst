package it

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"manifester/cp/logcp"
	"manifester/cp/logcp/logcptest"
	"manifester/sys/s3store"
)

// fullStack is the real thing: the log control plane on ExtendDB and segments
// in SeaweedFS. Skipped unless both endpoints are set (scripts/it-up.sh prints
// them).
func fullStack(t *testing.T) *stack {
	t.Helper()
	dy, s3ep := os.Getenv("MANIFESTER_DYNAMO_ENDPOINT"), os.Getenv("MANIFESTER_S3_ENDPOINT")
	if dy == "" || s3ep == "" {
		t.Skip("MANIFESTER_DYNAMO_ENDPOINT and MANIFESTER_S3_ENDPOINT not set (scripts/it-up.sh)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := dynamodb.New(dynamodb.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(dy),
		Credentials:  credentials.NewStaticCredentialsProvider("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", ""),
	})
	table := fmt.Sprintf("it%d_cp", time.Now().UnixNano())
	if err := logcp.CreateTable(ctx, db, table); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = db.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{TableName: &table}) })
	objs, err := s3store.New(ctx, s3store.Config{Endpoint: s3ep, Bucket: s3store.UniqueBucket(), PathStyle: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := objs.EnsureBucket(ctx); err != nil {
		t.Fatal(err)
	}
	cfg := logcptest.FastConfig("it0", 0, logcp.NewDynLog(db, table, logcp.DynLogOptions{}))
	cfg.DB, cfg.Table, cfg.Objects = db, table, logcptest.NewObjects()
	return newStack(t, logcptest.Start(t, cfg), objs, s3store.Classifier{})
}

func TestFullStackRoundTrip(t *testing.T) {
	s := fullStack(t)
	s.addBrokers(2)
	create(s)
	runRoundTrip(t, s)
}

func TestFullStackKillBroker(t *testing.T) {
	s := fullStack(t)
	s.addBrokers(3)
	create(s)
	runKill(t, s)
}
