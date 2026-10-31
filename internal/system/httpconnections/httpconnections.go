// Package httpconnections sizes the HTTP connection pools of the broker and the
// control plane for the real concurrency (audit item 4).
//
// Go's default keeps 2 idle connections per host, the AWS SDK's 10 and the old
// controlplaneclient's 16. A caught-up consumer fleet makes tens of thousands of CP
// requests per second and a broker has hundreds of S3 requests in flight, so
// with those defaults every burst dials fresh connections and leaves them in
// TIME_WAIT (16k sockets in six seconds were measured), which exhausts the
// ephemeral ports. The pools below keep up to PerHost idle connections per
// host, so a steady load reuses connections and dials nothing.
package httpconnections

import (
	"net"
	"net/http"
	"time"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
)

const (
	// PerHost is MaxIdleConnsPerHost: at least the peak concurrency to one
	// host (S3 PUT/GET fan-out per broker, CP Lookup storm).
	PerHost = 256
	// MaxPerHost caps open connections (idle and busy) to one host; more
	// requests queue instead of dialing without bound.
	MaxPerHost = 1024
	// IdleTimeout closes a connection idle this long. S3 closes idle
	// connections itself after about 20 s; staying below the server's own
	// timeouts avoids reusing a dead one.
	IdleTimeout = 60 * time.Second
)

// Configure sets the pool fields of t.
func Configure(t *http.Transport) {
	t.MaxIdleConns = 2 * PerHost
	t.MaxIdleConnsPerHost = PerHost
	t.MaxConnsPerHost = MaxPerHost
	t.IdleConnTimeout = IdleTimeout
	t.ForceAttemptHTTP2 = false
}

// Transport returns a new pooled transport with the standard dialer.
func Transport() *http.Transport {
	t := &http.Transport{
		DialContext: (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
	}
	Configure(t)
	return t
}

// ConfigureAWSClient returns an ConfigureAWSClient SDK HTTP client with the pool of Configure.
func ConfigureAWSClient() *awshttp.BuildableClient {
	return awshttp.NewBuildableClient().WithTransportOptions(Configure)
}
