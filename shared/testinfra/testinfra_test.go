//go:build integration

package testinfra

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestMain(m *testing.M) { Main(m) }

func TestDynamoDBRoundTrip(t *testing.T) {
	ctx := context.Background()
	for range 2 { // a second call must give a different, empty table
		d := DynamoDB(t)
		key := map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "a"},
			"sk": &types.AttributeValueMemberS{Value: "b"},
		}
		got, err := d.Client.GetItem(ctx, &dynamodb.GetItemInput{TableName: &d.Table, Key: key})
		if err != nil || len(got.Item) != 0 {
			t.Fatalf("fresh table not empty: %v %v", got, err)
		}
		if _, err := d.Client.PutItem(ctx, &dynamodb.PutItemInput{TableName: &d.Table, Item: key}); err != nil {
			t.Fatal(err)
		}
		got, err = d.Client.GetItem(ctx, &dynamodb.GetItemInput{TableName: &d.Table, Key: key})
		if err != nil || len(got.Item) != 2 {
			t.Fatalf("read back: %v %v", got, err)
		}
	}
}

func TestPostgresRoundTrip(t *testing.T) {
	for range 2 { // the table has no schema name: it must land in the fresh schema
		p := Postgres(t)
		db, err := sql.Open("pgx", p.DSN)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		if _, err := db.Exec("CREATE TABLE smoke (n INT)"); err != nil {
			t.Fatalf("the table exists already, so the schema is not fresh: %v", err)
		}
		if _, err := db.Exec("INSERT INTO smoke VALUES (7)"); err != nil {
			t.Fatal(err)
		}
		var n int
		if err := db.QueryRow("SELECT n FROM smoke").Scan(&n); err != nil || n != 7 {
			t.Fatalf("read back: %d %v", n, err)
		}
	}
}

func TestS3RoundTrip(t *testing.T) {
	ctx := context.Background()
	for range 2 {
		s := S3(t)
		list, err := s.Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: &s.Bucket})
		if err != nil || len(list.Contents) != 0 {
			t.Fatalf("fresh bucket not empty: %v %v", list, err)
		}
		if _, err := s.Client.PutObject(ctx, &s3.PutObjectInput{Bucket: &s.Bucket, Key: aws.String("k"), Body: bytes.NewReader([]byte("v"))}); err != nil {
			t.Fatal(err)
		}
		out, err := s.Client.GetObject(ctx, &s3.GetObjectInput{Bucket: &s.Bucket, Key: aws.String("k")})
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(out.Body)
		out.Body.Close()
		if string(b) != "v" {
			t.Fatalf("read back %q", b)
		}
	}
}
