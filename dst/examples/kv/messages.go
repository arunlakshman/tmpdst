package kv

import "github.com/manifesterio/mds/dst/trace"

// Every message has a fixed tag number and a Fold method that writes the tag
// first, then the fields. A test checks that no two tags are equal. The trace
// hash never uses Go type names, so renaming a type moves no golden hash.

// Tag returns the tag number of the message.
func (begin) Tag() uint64       { return 1 }
func (nextOp) Tag() uint64      { return 2 }
func (getResponse) Tag() uint64 { return 3 }
func (sendPut) Tag() uint64     { return 11 }
func (sendGet) Tag() uint64     { return 12 }
func (waitNext) Tag() uint64    { return 13 }
func (putRequest) Tag() uint64  { return 21 }
func (getRequest) Tag() uint64  { return 22 }
func (getDone) Tag() uint64     { return 23 }
func (storePut) Tag() uint64    { return 31 }
func (storeGet) Tag() uint64    { return 32 }
func (reply) Tag() uint64       { return 33 }

// Fold writes the message into the trace hash.
func (m begin) Fold(h *trace.Hash)  { h.Uint64(m.Tag()) }
func (m nextOp) Fold(h *trace.Hash) { h.Uint64(m.Tag()) }
func (m getResponse) Fold(h *trace.Hash) {
	fold3(h, m.Tag(), m.Key, m.Value, m.Want)
}
func (m sendPut) Fold(h *trace.Hash) { fold2(h, m.Tag(), m.Key, m.Value) }
func (m sendGet) Fold(h *trace.Hash) { fold2(h, m.Tag(), m.Key, m.Want) }
func (m waitNext) Fold(h *trace.Hash) {
	h.Uint64(m.Tag())
	h.Uint64(uint64(m.ID))
	h.Int64(int64(m.After))
}
func (m putRequest) Fold(h *trace.Hash) { fold2(h, m.Tag(), m.Key, m.Value) }
func (m getRequest) Fold(h *trace.Hash) { fold2(h, m.Tag(), m.Key, m.Want) }
func (m getDone) Fold(h *trace.Hash)    { fold3(h, m.Tag(), m.Key, m.Value, m.Want) }
func (m storePut) Fold(h *trace.Hash)   { fold2(h, m.Tag(), m.Key, m.Value) }
func (m storeGet) Fold(h *trace.Hash)   { fold2(h, m.Tag(), m.Key, m.Want) }
func (m reply) Fold(h *trace.Hash)      { fold3(h, m.Tag(), m.Key, m.Value, m.Want) }

func fold2(h *trace.Hash, tag uint64, a, b string) {
	h.Uint64(tag)
	h.String(a)
	h.String(b)
}

func fold3(h *trace.Hash, tag uint64, a, b, c string) {
	fold2(h, tag, a, b)
	h.String(c)
}
