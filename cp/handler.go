package cp

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"manifester/core"
)

const maxBody = 32 << 20

// NewHandler serves st over JSON: POST /v1/{register,init-producer,
// commit-segment,lookup,describe-topics,create-topic} and GET /healthz.
// Business errors are HTTP 200 with {"err":"ErrFenced"} etc.; ErrUnavailable
// and context errors are 503; any other store error is 500.
func NewHandler(st Store) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
	post(mux, "/v1/register", func(ctx context.Context, q RegisterReq) (any, error) {
		self, err := NodeFromWire(q.Self)
		if err != nil {
			return RegisterResp{Err: ErrString(core.ErrInvalid)}, nil
		}
		r, err := st.Register(ctx, core.Register{Self: self})
		return RegisterResp{Epoch: uint64(r.Epoch)}, err
	})
	post(mux, "/v1/init-producer", func(ctx context.Context, _ struct{}) (any, error) {
		r, err := st.InitProducer(ctx)
		return InitProducerResp{Producer: int64(r.Producer), PEpoch: r.PEpoch}, err
	})
	post(mux, "/v1/commit-segment", func(ctx context.Context, q CommitReq) (any, error) {
		owner, err := NodeFromWire(q.Owner)
		if err != nil {
			return CommitResp{Err: ErrString(core.ErrInvalid)}, nil
		}
		op := core.CommitSegment{SegmentKey: q.SegmentKey, Owner: owner, Epoch: core.Epoch(q.Epoch)}
		for _, b := range q.Batches {
			op.Batches = append(op.Batches, BatchFromWire(b))
		}
		r, code, err := st.CommitSegment(ctx, op)
		if code != core.OK || err != nil {
			return CommitResp{Err: ErrString(code)}, err
		}
		out := CommitResp{}
		for _, a := range r.Assigned {
			out.Assigned = append(out.Assigned, RangeToWire(a))
		}
		return out, nil
	})
	post(mux, "/v1/lookup", func(ctx context.Context, q LookupReq) (any, error) {
		op := core.Lookup{TP: core.TopicPartition{Topic: q.Topic, Partition: q.Partition}, From: core.Offset(q.From), MaxBytes: q.MaxBytes}
		r, code, err := st.Lookup(ctx, op)
		if code != core.OK || err != nil {
			return LookupResp{Err: ErrString(code)}, err
		}
		out := LookupResp{HighWatermark: int64(r.HighWatermark)}
		for _, a := range r.Ranges {
			out.Ranges = append(out.Ranges, RangeToWire(a))
		}
		return out, nil
	})
	post(mux, "/v1/describe-topics", func(ctx context.Context, q DescribeReq) (any, error) {
		r, err := st.DescribeTopics(ctx, core.DescribeTopics{Topics: q.Topics})
		out := DescribeResp{}
		for _, t := range r.Topics {
			out.Topics = append(out.Topics, Topic{t.Name, t.Partitions})
		}
		return out, err
	})
	post(mux, "/v1/create-topic", func(ctx context.Context, q CreateTopicReq) (any, error) {
		code, err := st.CreateTopic(ctx, q.Topic, q.Partitions)
		return CreateTopicResp{Err: ErrString(code)}, err
	})
	return mux
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
