package cp

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"manifester/core"
	"manifester/cp/cpobs"
)

const maxBody = 32 << 20

// NewHandler serves st over JSON: POST /v1/{commit-segment,lookup,describe-topics,create-topic} and GET /healthz.
// Business errors are HTTP 200 with {"err":"ErrConflict"} etc.; ErrUnavailable
// and context errors are 503; any other store error is 500.
func NewHandler(st Store) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
	post(mux, "/v1/commit-segment", commitHandler(st))
	post(mux, "/v1/lookup", lookupHandler(st))
	post(mux, "/v1/describe-topics", describeHandler(st))
	post(mux, "/v1/create-topic", createTopicHandler(st))
	return mux
}

func commitHandler(st Store) func(context.Context, CommitReq) (any, error) {
	return func(ctx context.Context, q CommitReq) (any, error) {
		cpobs.FromContext(ctx).SetSegment(q.SegmentKey)
		op := core.CommitSegment{SegmentKey: q.SegmentKey}
		for _, b := range q.Batches {
			op.Batches = append(op.Batches, BatchFromWire(b))
		}
		r, code, err := st.CommitSegment(ctx, op)
		if code != core.OK || err != nil {
			return CommitResp{Err: ErrString(code)}, err
		}
		return CommitResp{Assigned: rangesToWire(r.Assigned)}, nil
	}
}

func lookupHandler(st Store) func(context.Context, LookupReq) (any, error) {
	return func(ctx context.Context, q LookupReq) (any, error) {
		op := core.Lookup{TP: core.TopicPartition{Topic: q.Topic, Partition: q.Partition}, From: core.Offset(q.From), MaxBytes: q.MaxBytes}
		r, code, err := st.Lookup(ctx, op)
		if code != core.OK || err != nil {
			return LookupResp{Err: ErrString(code)}, err
		}
		return LookupResp{End: int64(r.End), Ranges: rangesToWire(r.Ranges)}, nil
	}
}

func rangesToWire(in []core.BatchRange) []Range {
	var out []Range
	for _, a := range in {
		out = append(out, RangeToWire(a))
	}
	return out
}

func describeHandler(st Store) func(context.Context, DescribeReq) (any, error) {
	return func(ctx context.Context, q DescribeReq) (any, error) {
		r, err := st.DescribeTopics(ctx, core.DescribeTopics{Topics: q.Topics})
		out := DescribeResp{}
		for _, t := range r.Topics {
			out.Topics = append(out.Topics, Topic{t.Name, t.Partitions})
		}
		return out, err
	}
}

func createTopicHandler(st Store) func(context.Context, CreateTopicReq) (any, error) {
	return func(ctx context.Context, q CreateTopicReq) (any, error) {
		code, err := st.CreateTopic(ctx, q.Topic, q.Partitions)
		return CreateTopicResp{Err: ErrString(code)}, err
	}
}

// post registers a JSON POST endpoint. A malformed body is ErrInvalid (200).
func post[Q any](mux *http.ServeMux, path string, f func(context.Context, Q) (any, error)) {
	mux.HandleFunc("POST "+path, func(w http.ResponseWriter, r *http.Request) {
		var q Q
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody)).Decode(&q); err != nil {
			reply(w, http.StatusOK, map[string]string{"err": core.ErrInvalid.String()})
			return
		}
		resp, err := f(r.Context(), q)
		switch {
		case err == nil:
			reply(w, http.StatusOK, resp)
		case errors.Is(err, ErrUnavailable), errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			slog.Warn("cp: unavailable", "path", path, "err", err)
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
		default:
			slog.Error("cp: store error", "path", path, "err", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
		}
	})
}

func reply(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
