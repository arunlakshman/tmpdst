// Package s3store is the S3 adapter: a system.ObjectStore over the AWS SDK and
// the system.ErrorClassifier for Obj* actions. It behaves like speccheck.ObjectStore
// (conformance C3), so a range outside the object is ErrInvalid and deleting
// an absent key is ErrNotFound.
package s3store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"mds/internal/core"
	"mds/internal/system"
	"mds/internal/system/awsmetrics"
	"mds/internal/system/httpconnections"
)

// Config selects the bucket and endpoint. An empty Endpoint is AWS itself.
type Config struct {
	Endpoint  string // e.g. http://127.0.0.1:8333
	Region    string // default us-east-1
	Bucket    string
	Prefix    string // prepended to every key; List strips it
	PathStyle bool   // required by SeaweedFS and most S3 clones
	// HTTP overrides the HTTP client (tests).
	HTTP *http.Client
	// Obs, if set, measures every HTTP attempt of every SDK call
	// (mdsbroker_s3_*) and logs failed attempts.
	Obs *awsmetrics.Recorder
}

// Error is a truthful failure of the store, as plain data.
type Error struct{ Code core.ErrCode }

func (e *Error) Error() string { return "s3store: " + e.Code.String() }

func fail(c core.ErrCode) error { return &Error{c} }

// Store implements system.ObjectStore.
type Store struct {
	c      *s3.Client
	bucket string
	prefix string
}

var _ system.ObjectStore = (*Store)(nil)

// New builds a store. With a custom endpoint and no AWS credentials in the
// environment it uses a static dummy key (SeaweedFS runs without auth).
func New(ctx context.Context, storeConfig Config) (*Store, error) {
	if storeConfig.Region == "" {
		storeConfig.Region = "us-east-1"
	}
	opts := loadOptions(storeConfig)
	if storeConfig.HTTP == nil {
		opts = append(opts, config.WithHTTPClient(httpconnections.ConfigureAWSClient()))
	}
	ac, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return &Store{c: s3.NewFromConfig(ac, clientOptions(storeConfig)), bucket: storeConfig.Bucket, prefix: storeConfig.Prefix}, nil
}

func loadOptions(storeConfig Config) []func(*config.LoadOptions) error {
	opts := []func(*config.LoadOptions) error{config.WithRegion(storeConfig.Region)}
	if storeConfig.Endpoint != "" && os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("mds", "mds", "")))
	}
	if storeConfig.HTTP != nil {
		opts = append(opts, config.WithHTTPClient(storeConfig.HTTP))
	}
	return opts
}

func clientOptions(storeConfig Config) func(*s3.Options) {
	return func(o *s3.Options) {
		o.UsePathStyle = storeConfig.PathStyle
		o.Retryer = aws.NopRetryer{} // the broker owns retries
		o.APIOptions = append(o.APIOptions, storeConfig.Obs.APIOptions()...)
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
		if storeConfig.Endpoint != "" {
			o.BaseEndpoint = aws.String(storeConfig.Endpoint)
		}
	}
}

// EnsureBucket creates the bucket if it does not exist (dev and tests).
func (s *Store) EnsureBucket(ctx context.Context) error {
	_, err := s.c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &s.bucket})
	var own *s3types.BucketAlreadyOwnedByYou
	var exists *s3types.BucketAlreadyExists
	if errors.As(err, &own) || errors.As(err, &exists) {
		return nil
	}
	return err
}

func (s *Store) key(k string) *string { return aws.String(s.prefix + k) }

// Put writes data under key.
func (s *Store) Put(ctx context.Context, key string, data []byte) error {
	if key == "" {
		return fail(core.ErrInvalid)
	}
	_, err := s.c.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &s.bucket, Key: s.key(key),
		Body: bytes.NewReader(data), ContentLength: aws.Int64(int64(len(data))),
	})
	return err
}

// Get reads the bytes referenced by ref.
func (s *Store) Get(ctx context.Context, ref core.SegmentRef) ([]byte, error) {
	if ref.Key == "" {
		return nil, fail(core.ErrNotFound) // never stored: Put refuses it
	}
	if ref.Off < 0 {
		return nil, s.badRange(ctx, ref)
	}
	in := &s3.GetObjectInput{Bucket: &s.bucket, Key: s.key(ref.Key), Range: rangeHeader(ref)}
	out, err := s.c.GetObject(ctx, in)
	if err != nil {
		return s.getFailed(ctx, ref, in.Range != nil, err)
	}
	defer func() { _ = out.Body.Close() }()
	return readBody(out.Body, ref.Len)
}

func rangeHeader(ref core.SegmentRef) *string {
	switch {
	case ref.Len > 0:
		return aws.String(fmt.Sprintf("bytes=%d-%d", ref.Off, ref.Off+ref.Len-1))
	case ref.Off > 0:
		return aws.String(fmt.Sprintf("bytes=%d-", ref.Off))
	}
	return nil
}

// getFailed maps a failed GetObject; reading exactly at the end of the
// object is an empty result, not an error.
func (s *Store) getFailed(ctx context.Context, ref core.SegmentRef, ranged bool, err error) ([]byte, error) {
	if notFound(err) && !ranged {
		return nil, fail(core.ErrNotFound)
	}
	// Some servers answer a range past the end of an empty object with 404.
	if apiCode(err) == "InvalidRange" || notFound(err) {
		err = s.badRange(ctx, ref)
		if errors.Is(err, errEmpty) {
			return []byte{}, nil
		}
	}
	return nil, err
}

func readBody(body io.Reader, want int64) ([]byte, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}
	if want > 0 && int64(len(data)) != want {
		return nil, fail(core.ErrInvalid) // the range ran past the end
	}
	return data, nil
}

// badRange resolves a 416: reading from exactly the end is empty, anything
// else past the end is invalid.
func (s *Store) badRange(ctx context.Context, ref core.SegmentRef) error {
	h, err := s.c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &s.bucket, Key: s.key(ref.Key)})
	if err != nil {
		if notFound(err) {
			return fail(core.ErrNotFound)
		}
		return err
	}
	if ref.Len <= 0 && aws.ToInt64(h.ContentLength) == ref.Off {
		return errEmpty
	}
	return fail(core.ErrInvalid)
}

var errEmpty = errors.New("empty range")

// Delete removes the object under key.
func (s *Store) Delete(ctx context.Context, key string) error {
	if key == "" {
		return fail(core.ErrNotFound)
	}
	if _, err := s.c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &s.bucket, Key: s.key(key)}); err != nil {
		if notFound(err) {
			return fail(core.ErrNotFound)
		}
		return err
	}
	_, err := s.c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &s.bucket, Key: s.key(key)})
	return err
}

// List returns the keys that start with prefix.
func (s *Store) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	p := s3.NewListObjectsV2Paginator(s.c, &s3.ListObjectsV2Input{Bucket: &s.bucket, Prefix: aws.String(s.prefix + prefix)})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, o := range page.Contents {
			keys = append(keys, aws.ToString(o.Key)[len(s.prefix):])
		}
	}
	sort.Strings(keys)
	return keys, nil
}

func apiCode(err error) string {
	if ae, ok := errors.AsType[smithy.APIError](err); ok {
		return ae.ErrorCode()
	}
	return ""
}

func notFound(err error) bool {
	switch apiCode(err) {
	case "NoSuchKey", "NotFound":
		return true
	}
	return false
}

// UniqueBucket returns a fresh bucket name for a test run.
func UniqueBucket() string {
	return fmt.Sprintf("mds-%d", time.Now().UnixNano())
}
