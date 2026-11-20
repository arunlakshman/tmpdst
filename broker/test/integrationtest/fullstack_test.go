//go:build integration

package integrationtest

import (
	"context"
	"testing"
	"time"

	s3store "github.com/manifesterio/mds/shared/objectstore/s3"
	"github.com/manifesterio/mds/shared/testinfra"
)

// fullStack starts mdscontrolplane against DynamoDB Local and SeaweedFS from
// shared/testinfra, and brokers with segment objects in that S3. Never skips.
func fullStack(t *testing.T) *stack {
	t.Helper()
	d := testinfra.DynamoDB(t)
	s3 := testinfra.S3(t)
	// Table that does not exist yet so -create-tables creates it (same as
	// controlplane/test/integration).
	table := d.Table + "_cp"
	cp := startCP(t, []string{
		"-store", "dynamolog",
		"-dynamo-endpoint", d.Endpoint,
		"-region", "us-east-1",
		"-table", table,
		"-create-tables",
		"-checkpoint-bucket", s3.Bucket,
		"-s3-endpoint", s3.Endpoint,
		"-s3-path-style",
		"-rank", "0",
	})
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	objs, err := s3store.New(ctx, s3store.Config{
		Endpoint: s3.Endpoint, Bucket: s3.Bucket, PathStyle: true, Region: s3.Region,
	})
	if err != nil {
		t.Fatal(err)
	}
	return stackAgainst(t, cp.addr, objs, s3store.Classifier{})
}
