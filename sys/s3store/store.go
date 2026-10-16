// Package s3store is the S3 adapter: a sys.ObjectStore over the AWS SDK and
// the sys.Classifier for Obj* actions. It behaves like veritas.ObjectStore
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

	"manifester/core"
	"manifester/sys"
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
}

// Error is a truthful failure of the store, as plain data.
type Error struct{ Code core.ErrCode }

func (e *Error) Error() string { return "s3store: " + e.Code.String() }

func fail(c core.ErrCode) error { return &Error{c} }

// Store implements sys.ObjectStore.
type Store struct {
	c      *s3.Client
	bucket string
	prefix string
}

var _ sys.ObjectStore = (*Store)(nil)

// New builds a store. With a custom endpoint and no AWS credentials in the
// environment it uses a static dummy key (SeaweedFS runs without auth).
func New(ctx context.Context, cfg Config) (*Store, error) {
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	opts := []func(*config.LoadOptions) error{config.WithRegion(cfg.Region)}
	if cfg.Endpoint != "" && os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("manifester", "manifester", "")))
	}
	if cfg.HTTP != nil {
		opts = append(opts, config.WithHTTPClient(cfg.HTTP))
	}
	ac, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}
	c := s3.NewFromConfig(ac, func(o *s3.Options) {
		o.UsePathStyle = cfg.PathStyle
		o.Retryer = aws.NopRetryer{} // the broker owns retries
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
	})
	return &Store{c: c, bucket: cfg.Bucket, prefix: cfg.Prefix}, nil
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

func (s *Store) Get(ctx context.Context, ref core.SegmentRef) ([]byte, error) {
	if ref.Key == "" {
		return nil, fail(core.ErrNotFound) // never stored: Put refuses it
	}
	if ref.Off < 0 {
		return nil, s.badRange(ctx, ref)
	}
	in := &s3.GetObjectInput{Bucket: &s.bucket, Key: s.key(ref.Key)}
	switch {
	case ref.Len > 0:
		in.Range = aws.String(fmt.Sprintf("bytes=%d-%d", ref.Off, ref.Off+ref.Len-1))
	case ref.Off > 0:
		in.Range = aws.String(fmt.Sprintf("bytes=%d-", ref.Off))
	}
	out, err := s.c.GetObject(ctx, in)
	if err != nil {
		if notFound(err) && in.Range == nil {
			return nil, fail(core.ErrNotFound)
		}
		// Some servers answer a range past the end of an empty object with 404.
		if apiCode(err) == "InvalidRange" || notFound(err) {
			err = s.badRange(ctx, ref)
			if err == errEmpty {
				return []byte{}, nil
			}
			return nil, err
		}
		return nil, err
	}
	defer out.Body.Close()
	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, err
	}
	if ref.Len > 0 && int64(len(data)) != ref.Len {
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
	var ae smithy.APIError
	if errors.As(err, &ae) {
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
	return fmt.Sprintf("manifester-%d", time.Now().UnixNano())
}
