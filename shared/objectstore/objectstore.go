// Package objectstore is the contract of the object store (S3) for both products.
// It has:
//
//   - the four calls (Put, Get, Delete, List) as one sealed sum, Call;
//   - the four answers as one sealed sum, Result;
//   - Store, the plain Go interface that an adapter (such as the S3 adapter in
//     objectstore/s3) implements, and Classifier, which turns an adapter error into
//     plain data;
//   - Spec (spec.go), a short, fault-free model that shows the correct behavior.
//
// The broker core wraps a Call in its own action (core.ObjectCall) and a Result in
// its own event (core.ObjectDone). The wrapper writes the same bytes into the trace
// hash as the old separate cases did.
//
// This package has no I/O and no time, so a deterministic package may import it.
package objectstore

import (
	"context"

	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/shared/status"
)

// SegmentRef is a byte range inside an object in the object store.
type SegmentRef struct {
	Key string
	Off int64
	Len int64 // <= 0 means to the end of the object
}

// Call is one request to the object store. Only the four cases in this file
// exist. Every Call has a RequestID, and its Result carries the same one.
//
//sumtype:decl
type Call interface {
	isCall()
	RequestID() machine.RequestID
}

// Put uploads Data under Key.
type Put struct {
	Req  machine.RequestID
	Key  string // unique per upload, so a MaybeApplied PUT is safe to retry
	Data []byte
}

// Get reads the byte range Ref.
type Get struct {
	Req machine.RequestID
	Ref SegmentRef
}

// Delete removes the object at Key.
type Delete struct {
	Req machine.RequestID
	Key string
}

// List lists the keys under Prefix.
type List struct {
	Req    machine.RequestID
	Prefix string
}

func (Put) isCall()    {}
func (Get) isCall()    {}
func (Delete) isCall() {}
func (List) isCall()   {}

// RequestID returns the request this call carries.
func (c Put) RequestID() machine.RequestID { return c.Req }

// RequestID returns the request this call carries.
func (c Get) RequestID() machine.RequestID { return c.Req }

// RequestID returns the request this call carries.
func (c Delete) RequestID() machine.RequestID { return c.Req }

// RequestID returns the request this call carries.
func (c List) RequestID() machine.RequestID { return c.Req }

// Result is the answer to one Call. Only the four cases in this file exist.
//
//sumtype:decl
type Result interface {
	isResult()
	RequestID() machine.RequestID
}

// PutDone reports that a Put finished.
type PutDone struct {
	Req     machine.RequestID
	Key     string
	Err     status.ErrCode
	Outcome status.Outcome
}

// GetDone reports that a Get finished.
type GetDone struct {
	Req  machine.RequestID
	Data []byte
	Err  status.ErrCode
}

// DeleteDone reports that a Delete finished.
type DeleteDone struct {
	Req     machine.RequestID
	Err     status.ErrCode
	Outcome status.Outcome
}

// ListDone reports that a List finished.
type ListDone struct {
	Req  machine.RequestID
	Keys []string // sorted
	Err  status.ErrCode
}

func (PutDone) isResult()    {}
func (GetDone) isResult()    {}
func (DeleteDone) isResult() {}
func (ListDone) isResult()   {}

// RequestID returns the request this result answers.
func (r PutDone) RequestID() machine.RequestID { return r.Req }

// RequestID returns the request this result answers.
func (r GetDone) RequestID() machine.RequestID { return r.Req }

// RequestID returns the request this result answers.
func (r DeleteDone) RequestID() machine.RequestID { return r.Req }

// RequestID returns the request this result answers.
func (r ListDone) RequestID() machine.RequestID { return r.Req }

// Store is the object store as a Go interface. An adapter over real I/O (S3) and
// an in-memory fake both implement it. A failure that the store knows has a
// status.ErrCode inside its error value (the S3 adapter has the type s3.Error for
// this). Any other error is classified by a Classifier.
type Store interface {
	Put(ctx context.Context, key string, data []byte) error
	Get(ctx context.Context, ref SegmentRef) ([]byte, error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string) ([]string, error)
}

// Classifier turns an error of a Store into plain data (the single place where
// SDK, HTTP and net errors become an ErrCode). Getting the Outcome right here is
// what makes acknowledgments honest. The Outcome depends on which call failed,
// not only on the error.
type Classifier interface {
	Classify(call Call, err error) (status.ErrCode, status.Outcome)
}
