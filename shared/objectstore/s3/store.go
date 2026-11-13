// Package s3 is the S3 adapter: an objectstore.Store over the AWS SDK and the
// objectstore.Classifier for the object store calls. It follows the rules of
// objectstore.Spec, so a range outside the object is ErrInvalid and deleting
// an absent key is ErrNotFound. The broker and the control plane both use it.
package s3

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/manifesterio/mds/shared/awsmetrics"
	"github.com/manifesterio/mds/shared/httpconnections"
	"github.com/manifesterio/mds/shared/objectstore"
	"github.com/manifesterio/mds/shared/status"
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
type Error struct{ Code status.ErrCode }

func (e *Error) Error() string { return "s3store: " + e.Code.String() }

func fail(c status.ErrCode) error { return &Error{c} }

// Store implements objectstore.Store.
type Store struct {
	c      *awss3.Client
	bucket string
	prefix string
}

var _ objectstore.Store = (*Store)(nil)

// New builds a store. With a custom endpoint and no AWS credentials in the
// environment it uses a static dummy key (SeaweedFS runs without auth).
func New(ctx context.Context, storeConfig Config) (*Store, error) {
	storeConfig.Region = cmp.Or(storeConfig.Region, "us-east-1")
	opts := loadOptions(storeConfig)
	if storeConfig.HTTP == nil {
		opts = append(opts, config.WithHTTPClient(httpconnections.ConfigureAWSClient()))
	}
	ac, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return &Store{c: awss3.NewFromConfig(ac, clientOptions(storeConfig)), bucket: storeConfig.Bucket, prefix: storeConfig.Prefix}, nil
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

func clientOptions(storeConfig Config) func(*awss3.Options) {
	return func(o *awss3.Options) {
		o.UsePathStyle = storeConfig.PathStyle
		o.Retryer = aws.NopRetryer{} // the broker owns retries
		o.APIOptions = append(o.APIOptions, storeConfig.Obs.APIOptions()...)
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
		if storeConfig.Endpoint != "" {
			o.BaseEndpoint = new(storeConfig.Endpoint)
		}
	}
}

// EnsureBucket creates the bucket if it does not exist (dev and tests).
func (s *Store) EnsureBucket(ctx context.Context) error {
	_, err := s.c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: &s.bucket})
	_, own := errors.AsType[*s3types.BucketAlreadyOwnedByYou](err)
	_, exists := errors.AsType[*s3types.BucketAlreadyExists](err)
	if own || exists {
		return nil
	}
	return err
}

func (s *Store) key(k string) *string { return new(s.prefix + k) }

// Put writes data under key.
func (s *Store) Put(ctx context.Context, key string, data []byte) error {
	if key == "" {
		return fail(status.ErrInvalid)
	}
	_, err := s.c.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: &s.bucket, Key: s.key(key),
		Body: bytes.NewReader(data), ContentLength: new(int64(len(data))),
	})
	return err
}

// Get reads the bytes referenced by ref.
func (s *Store) Get(ctx context.Context, ref objectstore.SegmentRef) ([]byte, error) {
	if ref.Key == "" {
		return nil, fail(status.ErrNotFound) // never stored: Put refuses it
	}
	if ref.Off < 0 {
		return nil, s.badRange(ctx, ref)
	}
	in := &awss3.GetObjectInput{Bucket: &s.bucket, Key: s.key(ref.Key), Range: rangeHeader(ref)}
	out, err := s.c.GetObject(ctx, in)
	if err != nil {
		return s.getFailed(ctx, ref, in.Range != nil, err)
	}
	defer func() { _ = out.Body.Close() }()
	return readBody(out.Body, ref.Len)
}

func rangeHeader(ref objectstore.SegmentRef) *string {
	switch {
	case ref.Len > 0:
		return new(fmt.Sprintf("bytes=%d-%d", ref.Off, ref.Off+ref.Len-1))
	case ref.Off > 0:
		return new(fmt.Sprintf("bytes=%d-", ref.Off))
	}
	return nil
}

// getFailed maps a failed GetObject; reading exactly at the end of the
// object is an empty result, not an error.
func (s *Store) getFailed(ctx context.Context, ref objectstore.SegmentRef, ranged bool, err error) ([]byte, error) {
	if notFound(err) && !ranged {
		return nil, fail(status.ErrNotFound)
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
		return nil, fail(status.ErrInvalid) // the range ran past the end
	}
	return data, nil
}

// badRange resolves a 416: reading from exactly the end is empty, anything
// else past the end is invalid.
func (s *Store) badRange(ctx context.Context, ref objectstore.SegmentRef) error {
	h, err := s.c.HeadObject(ctx, &awss3.HeadObjectInput{Bucket: &s.bucket, Key: s.key(ref.Key)})
	if err != nil {
		if notFound(err) {
			return fail(status.ErrNotFound)
		}
		return err
	}
	if ref.Len <= 0 && aws.ToInt64(h.ContentLength) == ref.Off {
		return errEmpty
	}
	return fail(status.ErrInvalid)
}

var errEmpty = errors.New("empty range")

// Delete removes the object under key.
func (s *Store) Delete(ctx context.Context, key string) error {
	if key == "" {
		return fail(status.ErrNotFound)
	}
	if _, err := s.c.HeadObject(ctx, &awss3.HeadObjectInput{Bucket: &s.bucket, Key: s.key(key)}); err != nil {
		if notFound(err) {
			return fail(status.ErrNotFound)
		}
		return err
	}
	_, err := s.c.DeleteObject(ctx, &awss3.DeleteObjectInput{Bucket: &s.bucket, Key: s.key(key)})
	return err
}

// List returns the keys that start with prefix.
func (s *Store) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	p := awss3.NewListObjectsV2Paginator(s.c, &awss3.ListObjectsV2Input{Bucket: &s.bucket, Prefix: new(s.prefix + prefix)})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, o := range page.Contents {
			keys = append(keys, aws.ToString(o.Key)[len(s.prefix):])
		}
	}
	slices.Sort(keys)
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
