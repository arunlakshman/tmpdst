//go:build integration

package testinfra

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var seaweedSvc service

// weed runs master, volume server, filer and S3 gateway in one process. With no
// S3 config file, the gateway does not check credentials.
var weedCmd = []string{"server", "-dir=/data", "-s3", "-s3.port=8333", "-volume.max=50"}

const (
	// The gateway ignores them; the SDK needs some.
	s3User = "mds"
	s3Pass = "mds"
)

// S3Info says how to reach one fresh SeaweedFS bucket.
type S3Info struct {
	Endpoint string     // http://host:port, for s3store.Config.Endpoint and -s3-endpoint
	Region   string     // us-east-1
	Bucket   string     // new, empty
	Client   *s3.Client // ready to use, path-style, against Endpoint
}

// S3 starts (once per package) a SeaweedFS container (S3 gateway on) and
// returns a new empty bucket. SeaweedFS runs without authentication, so the
// dummy key of s3store.New is enough. Use PathStyle: true. The bucket is
// deleted when the test ends. It fails the test if Docker is not running.
func S3(t testing.TB) S3Info {
	t.Helper()
	addr, err := seaweedSvc.start(testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{
		Image:        SeaweedFSImage,
		Cmd:          weedCmd,
		ExposedPorts: []string{"8333/tcp"},
		WaitingFor:   wait.ForHTTP("/").WithPort("8333/tcp").WithStartupTimeout(2 * time.Minute),
	}}, "8333/tcp")
	if err != nil {
		t.Fatal(err)
	}
	endpoint := "http://" + addr
	client := s3.New(s3.Options{
		Region:                     "us-east-1",
		BaseEndpoint:               aws.String(endpoint),
		UsePathStyle:               true,
		Credentials:                credentials.NewStaticCredentialsProvider(s3User, s3Pass, ""),
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	})
	// Bucket names allow lower case letters, digits and "-", not "_".
	bucket := strings.ReplaceAll(unique("bucket"), "_", "-")
	// The port answers a little before the filer and a volume are ready.
	var err2 error
	for range 60 {
		if _, err2 = client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: &bucket}); err2 == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if err2 != nil {
		t.Fatalf("testinfra: create bucket %s: %v", bucket, err2)
	}
	t.Cleanup(func() { emptyAndDelete(client, bucket) })
	return S3Info{Endpoint: endpoint, Region: "us-east-1", Bucket: bucket, Client: client}
}

func emptyAndDelete(client *s3.Client, bucket string) {
	ctx := context.Background()
	p := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{Bucket: &bucket})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return
		}
		for _, o := range page.Contents {
			_, _ = client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &bucket, Key: o.Key})
		}
	}
	_, _ = client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: &bucket})
}
