//go:build integration

package testinfra

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var dynamoSvc service

// DynamoDBInfo says how to reach one fresh DynamoDB table.
type DynamoDBInfo struct {
	Endpoint string           // http://host:port, for -dynamo-endpoint
	Client   *dynamodb.Client // ready to use, against Endpoint
	Table    string           // empty table with string keys pk and sk, pay-per-request
}

// DynamoDB returns a client for the shared DynamoDB Local container and a new
// empty table (keys pk and sk, both strings: the same shape as
// dynamolog.CreateTable makes). The table is deleted when the test ends.
// The container is in memory only. It fails the test if Docker is not running.
func DynamoDB(t testing.TB) DynamoDBInfo {
	t.Helper()
	addr, err := dynamoSvc.start(testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{
		Image:        DynamoDBImage,
		ExposedPorts: []string{"8000/tcp"},
		Cmd:          []string{"-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb"},
		WaitingFor:   wait.ForListeningPort("8000/tcp"),
	}}, "8000/tcp")
	if err != nil {
		t.Fatal(err)
	}
	endpoint := "http://" + addr
	client := dynamodb.New(dynamodb.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(endpoint),
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
	})
	table := unique("table")
	_, err = client.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		TableName: &table,
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatalf("testinfra: create table %s: %v", table, err)
	}
	t.Cleanup(func() { _, _ = client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{TableName: &table}) })
	return DynamoDBInfo{Endpoint: endpoint, Client: client, Table: table}
}
