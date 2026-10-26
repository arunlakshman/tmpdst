package runinfo

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Manifest is the record of what a process was started with. It is written
// once at startup (-manifest-out) so a run can be reconstructed from logs,
// metrics and this file alone.
type Manifest struct {
	Info
	Region      string            `json:"region,omitempty"`
	Flags       map[string]string `json:"flags"`     // every flag, effective value
	FlagsSet    []string          `json:"flags_set"` // flags given explicitly
	Tuning      map[string]any    `json:"tuning,omitempty"`
	Env         map[string]string `json:"env,omitempty"` // MANIFESTER_*, AWS_REGION, AWS_DEFAULT_REGION, AWS_ENDPOINT*
	PID         int               `json:"pid"`
	Args        []string          `json:"args"`
	GOOS        string            `json:"goos"`
	GOARCH      string            `json:"goarch"`
	NumCPU      int               `json:"num_cpu"`
	GOMAXPROCS  int               `json:"gomaxprocs"`
	Instance    map[string]string `json:"instance,omitempty"` // EC2 IMDSv2, absent elsewhere
	ManifestURI string            `json:"manifest_uri,omitempty"`
}

// NewManifest captures fs (usually flag.CommandLine, after Parse). tuning is
// the service's resolved settings that are not plain flags (sizes parsed from
// "auto", memory limit, ...).
func NewManifest(i Info, fs *flag.FlagSet, region string, tuning map[string]any) Manifest {
	m := Manifest{Info: i, Region: region, Flags: map[string]string{}, Tuning: tuning, Env: map[string]string{},
		PID: os.Getpid(), Args: os.Args[1:], GOOS: runtime.GOOS, GOARCH: runtime.GOARCH,
		NumCPU: runtime.NumCPU(), GOMAXPROCS: runtime.GOMAXPROCS(0)}
	fs.VisitAll(func(f *flag.Flag) { m.Flags[f.Name] = f.Value.String() })
	fs.Visit(func(f *flag.Flag) { m.FlagsSet = append(m.FlagsSet, f.Name) })
	sort.Strings(m.FlagsSet)
	for _, kv := range os.Environ() {
		k, v, _ := strings.Cut(kv, "=")
		if strings.HasPrefix(k, "MANIFESTER_") || k == "AWS_REGION" || k == "AWS_DEFAULT_REGION" || strings.HasPrefix(k, "AWS_ENDPOINT") {
			m.Env[k] = v
		}
	}
	return m
}

// Log emits the manifest's flags and tuning as one INFO line.
func (m Manifest) Log(log *slog.Logger) {
	log.Info("run manifest", "event", "manifest", "region", m.Region, "flags", m.Flags, "flags_set", m.FlagsSet,
		"tuning", m.Tuning, "instance", m.Instance, "hostname", m.Hostname, "go_version", m.GoVersion,
		"num_cpu", m.NumCPU, "gomaxprocs", m.GOMAXPROCS)
}

// WriteOptions say where and how to write.
type WriteOptions struct {
	Dest         string // file path, or s3://bucket/key; "{service}" and "{run_id}" are substituted; a trailing "/" gets <service>-<run_id>.json
	Region       string
	S3Endpoint   string
	S3PathStyle  bool
	IMDS         bool          // try EC2 instance metadata
	IMDSEndpoint string        // default http://169.254.169.254
	IMDSTimeout  time.Duration // per request, default 300ms
}

// Write fills in instance metadata (optional), then writes the manifest to
// o.Dest. It returns the final destination. Callers treat errors as non-fatal.
func Write(ctx context.Context, m Manifest, o WriteOptions) (string, error) {
	if o.Dest == "" {
		return "", nil
	}
	if o.IMDS {
		m.Instance = FetchIMDS(ctx, o.IMDSEndpoint, o.IMDSTimeout)
	}
	dest := ExpandDest(o.Dest, m.Info)
	m.ManifestURI = dest
	body, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return dest, err
	}
	body = append(body, '\n')
	if strings.HasPrefix(dest, "s3://") {
		return dest, putS3(ctx, dest, body, o)
	}
	return dest, writeFile(dest, body)
}

// writeFile writes body to path, creating parent directories.
func writeFile(path string, body []byte) error {
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return err
		}
	}
	return os.WriteFile(path, body, 0o600)
}

// putS3 uploads body to the s3://bucket/key destination.
func putS3(ctx context.Context, dest string, body []byte, o WriteOptions) error {
	u, err := url.Parse(dest)
	if err != nil || u.Host == "" || strings.TrimPrefix(u.Path, "/") == "" {
		return fmt.Errorf("manifest: bad S3 destination %q", dest)
	}
	opts := []func(*config.LoadOptions) error{}
	if o.Region != "" {
		opts = append(opts, config.WithRegion(o.Region))
	}
	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return err
	}
	c := s3.NewFromConfig(cfg, func(so *s3.Options) {
		so.UsePathStyle = o.S3PathStyle
		if o.S3Endpoint != "" {
			so.BaseEndpoint = aws.String(o.S3Endpoint)
		}
		so.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
	})
	_, err = c.PutObject(ctx, &s3.PutObjectInput{Bucket: &u.Host, Key: aws.String(strings.TrimPrefix(u.Path, "/")),
		Body: bytes.NewReader(body), ContentType: aws.String("application/json")})
	return err
}

// ExpandDest substitutes {service} and {run_id} and completes a trailing "/".
func ExpandDest(d string, i Info) string {
	d = strings.NewReplacer("{service}", i.Service, "{run_id}", i.RunID).Replace(d)
	if strings.HasSuffix(d, "/") {
		d += i.Service + "-" + i.RunID + ".json"
	}
	return d
}

// FetchIMDS reads a few IMDSv2 fields. It returns nil when the endpoint is not
// reachable (not on EC2), after at most one short timeout.
func FetchIMDS(ctx context.Context, endpoint string, timeout time.Duration) map[string]string {
	if endpoint == "" {
		endpoint = "http://169.254.169.254"
	}
	if timeout <= 0 {
		timeout = 300 * time.Millisecond
	}
	hc := &http.Client{Timeout: timeout}
	do := func(method, path string, hdr map[string]string) (string, bool) {
		req, err := http.NewRequestWithContext(ctx, method, endpoint+path, nil)
		if err != nil {
			return "", false
		}
		for k, v := range hdr {
			req.Header.Set(k, v)
		}
		resp, err := hc.Do(req)
		if err != nil {
			return "", false
		}
		defer func() { _ = resp.Body.Close() }() // read-only response
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return strings.TrimSpace(string(b)), resp.StatusCode == http.StatusOK
	}
	tok, ok := do(http.MethodPut, "/latest/api/token", map[string]string{"X-aws-ec2-metadata-token-ttl-seconds": "60"})
	if !ok {
		return nil
	}
	out := map[string]string{}
	for k, p := range map[string]string{
		"instance_id": "instance-id", "instance_type": "instance-type", "availability_zone": "placement/availability-zone",
		"region": "placement/region", "ami_id": "ami-id", "local_ipv4": "local-ipv4",
	} {
		if v, ok := do("GET", "/latest/meta-data/"+p, map[string]string{"X-aws-ec2-metadata-token": tok}); ok {
			out[k] = v
		}
	}
	return out
}
