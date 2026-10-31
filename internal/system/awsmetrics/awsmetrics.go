// Package awsmetrics measures AWS SDK v2 calls with smithy middleware: one
// observation per HTTP attempt (so retries the SDK hides are visible), one per
// operation, retries, request bytes and throttling. It lives at the
// non-deterministic edge and is shared by system/s3store (mdsbroker_s3_*) and
// mdscontrolplane's DynamoDB client (mdscontrolplane_dynamo_*).
//
// Every method is safe on a nil *Recorder.
package awsmetrics

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"time"

	awsmw "github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/prometheus/client_golang/prometheus"
)

// Recorder holds the collectors and the logger of one AWS client.
type Recorder struct {
	log *slog.Logger
	svc string

	attempt   *prometheus.HistogramVec // op, result
	operation *prometheus.HistogramVec // op, outcome
	retries   *prometheus.CounterVec   // op
	reqBytes  *prometheus.HistogramVec // op
	apiErrs   *prometheus.CounterVec   // op, code
	throttles *prometheus.CounterVec   // op, code

	// QuietCodes are API error codes whose failed attempts log at DEBUG, not
	// WARN, because the caller reports them with more context.
	QuietCodes map[string]bool

	lim limiter
}

// New registers <namespace>_<sub>_* on reg. log may be nil (no log lines).
func New(reg prometheus.Registerer, namespace, sub string, log *slog.Logger) *Recorder {
	if log == nil {
		log = slog.New(slog.NewTextHandler(discard{}, nil))
	}
	secs := prometheus.ExponentialBuckets(0.001, 2, 15) // 1 ms .. 16 s
	r := &Recorder{log: log.With("aws_service", sub), svc: sub, lim: limiter{perSec: 20}}
	r.attempt = prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: namespace, Subsystem: sub, Name: "attempt_seconds",
		Help: "One HTTP attempt of an SDK call (SDK retries are separate attempts), by operation and result: ok, http_<status>, timeout, canceled, network, error.", Buckets: secs},
		[]string{"op", "result"})
	r.operation = prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: namespace, Subsystem: sub, Name: "operation_seconds",
		Help: "A whole SDK call including SDK retries and backoff, by operation and outcome (ok, error).", Buckets: secs},
		[]string{"op", "outcome"})
	r.retries = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: namespace, Subsystem: sub, Name: "retries_total",
		Help: "Attempts beyond the first of an SDK call, by operation."}, []string{"op"})
	r.reqBytes = prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: namespace, Subsystem: sub, Name: "request_bytes",
		Help: "HTTP request body size per attempt, by operation.", Buckets: prometheus.ExponentialBuckets(64, 4, 12)}, []string{"op"})
	r.apiErrs = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: namespace, Subsystem: sub, Name: "api_errors_total",
		Help: "Attempts that ended in an AWS API error, by operation and error code."}, []string{"op", "code"})
	r.throttles = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: namespace, Subsystem: sub, Name: "throttles_total",
		Help: "Attempts rejected for throttling (ProvisionedThroughputExceededException, ThrottlingException, RequestLimitExceeded, SlowDown, HTTP 429/503 throttle codes), by operation and code."},
		[]string{"op", "code"})
	reg.MustRegister(r.attempt, r.operation, r.retries, r.reqBytes, r.apiErrs, r.throttles)
	return r
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

// APIOptions returns the options to append to a client's APIOptions.
func (r *Recorder) APIOptions() []func(*middleware.Stack) error {
	if r == nil {
		return nil
	}
	return []func(*middleware.Stack) error{func(st *middleware.Stack) error {
		// Whole operation: before Retry, so it sees the attempt results.
		if err := st.Finalize.Insert(operationMiddleware{r}, "Retry", middleware.Before); err != nil {
			return err
		}
		// Per attempt: after Retry (and signing), closest to the transport.
		return st.Finalize.Add(attemptMiddleware{r}, middleware.After)
	}}
}

// IsTooManyRequests reports whether an AWS error code means "slow down".
func IsTooManyRequests(code string) bool {
	switch code {
	case "ProvisionedThroughputExceededException", "ThrottlingException", "RequestLimitExceeded",
		"SlowDown", "Throttling", "ThrottledException", "RequestThrottled", "TooManyRequestsException",
		"RequestThrottledException", "EC2ThrottledException":
		return true
	}
	return false
}

// Result classifies an attempt's outcome for the "result" label and returns the
// HTTP status (0 if none) and API error code ("" if none).
func Result(ctx context.Context, status int, err error) (string, string) {
	var code string
	if ae, ok := errors.AsType[smithy.APIError](err); ok {
		code = ae.ErrorCode()
	}
	if err == nil {
		if status >= 200 && status < 300 || status == 0 {
			return "ok", ""
		}
		return "http_" + strconv.Itoa(status), ""
	}
	var re *awshttp.ResponseError
	if status == 0 && errors.As(err, &re) {
		status = re.HTTPStatusCode()
	}
	var ne net.Error
	switch {
	case status != 0:
		return "http_" + strconv.Itoa(status), code
	case errors.Is(err, context.DeadlineExceeded) || ctx.Err() == context.DeadlineExceeded || (errors.As(err, &ne) && ne.Timeout()):
		return "timeout", code
	case errors.Is(err, context.Canceled):
		return "canceled", code
	case errors.As(err, &ne):
		return "network", code
	}
	return "error", code
}

type attemptMiddleware struct{ r *Recorder }

func (attemptMiddleware) ID() string { return "mdsAwsObsAttempt" }

func (m attemptMiddleware) HandleFinalize(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (
	middleware.FinalizeOutput, middleware.Metadata, error) {
	r := m.r
	operation := awsmw.GetOperationName(ctx)
	var bytes int64 = -1
	target := ""
	if request, ok := in.Request.(*smithyhttp.Request); ok {
		bytes = request.ContentLength
		if request.URL != nil {
			target = request.URL.Path
		}
	}
	t0 := time.Now()
	out, md, err := next.HandleFinalize(ctx, in)
	d := time.Since(t0)

	status := 0
	if response, ok := awsmw.GetRawResponse(md).(*smithyhttp.Response); ok && response != nil {
		status = response.StatusCode
	}
	result, code := Result(ctx, status, err)
	var reqID string
	if re, ok := errors.AsType[*awshttp.ResponseError](err); ok {
		if status == 0 {
			status = re.HTTPStatusCode()
		}
		reqID = re.ServiceRequestID()
	}
	r.attempt.WithLabelValues(operation, result).Observe(d.Seconds())
	if bytes >= 0 {
		r.reqBytes.WithLabelValues(operation).Observe(float64(bytes))
	}
	if result == "ok" {
		return out, md, err
	}
	if code != "" {
		r.apiErrs.WithLabelValues(operation, code).Inc()
	}
	throttled := IsTooManyRequests(code)
	if throttled {
		r.throttles.WithLabelValues(operation, code).Inc()
	}
	lvl := slog.LevelWarn
	if r.QuietCodes[code] || result == "canceled" {
		lvl = slog.LevelDebug
	}
	if lvl == slog.LevelWarn {
		if n, ok := r.lim.allow(time.Now()); !ok {
			return out, md, err
		} else if n > 0 {
			r.log.Warn("aws attempt warnings suppressed", "event", "aws_warn_suppressed", "suppressed", n)
		}
	}
	r.log.Log(ctx, lvl, "aws attempt failed", "event", "aws_attempt_failed", "op", operation, "result", result,
		"http_status", status, "code", code, "throttled", throttled, "duration_ms", ms(d), "request_bytes", bytes,
		"target", target, "request_id", reqID, "err", err.Error())
	return out, md, err
}

type operationMiddleware struct{ r *Recorder }

func (operationMiddleware) ID() string { return "mdsAwsObsOperation" }

func (m operationMiddleware) HandleFinalize(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (
	middleware.FinalizeOutput, middleware.Metadata, error) {
	t0 := time.Now()
	out, md, err := next.HandleFinalize(ctx, in)
	operation := awsmw.GetOperationName(ctx)
	outcome := "ok"
	if err != nil {
		outcome = "error"
	}
	m.r.operation.WithLabelValues(operation, outcome).Observe(time.Since(t0).Seconds())
	if ar, ok := retry.GetAttemptResults(md); ok && len(ar.Results) > 1 {
		m.r.retries.WithLabelValues(operation).Add(float64(len(ar.Results) - 1))
	}
	return out, md, err
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

// limiter admits perSec events per second and counts what it drops.
type limiter struct {
	mu      sync.Mutex
	perSec  int
	sec     int64
	n       int
	dropped int
}

// allow reports whether an event may be logged and how many were dropped since
// the last admitted one (to report once).
func (l *limiter) allow(now time.Time) (int, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	var dropped int
	if s := now.Unix(); s != l.sec {
		l.sec, l.n = s, 0
	}
	if l.n >= l.perSec {
		l.dropped++
		return 0, false
	}
	l.n++
	dropped, l.dropped = l.dropped, 0
	return dropped, true
}
