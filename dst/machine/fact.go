package machine

import "github.com/manifesterio/mds/dst/trace"

// Tag numbers of the trace hash for facts and field values. They are frozen:
// changing one moves every golden hash.
const (
	tagInt64Value  = 401
	tagUint64Value = 402
	tagStringValue = 403
	tagBool        = 404
	tagBytes       = 405
	tagFact        = 501
)

// Fact is a named, structured observation for the timeline. Rules (the
// checks of a run) read facts by name.
type Fact struct {
	Name   string
	Fields []Field
}

// Field returns the value of the first field with this key, or nil when the
// fact has no such field.
func (f Fact) Field(key string) FieldValue {
	for _, fl := range f.Fields {
		if fl.Key == key {
			return fl.Value
		}
	}
	return nil
}

// Fold writes the fact into the trace hash: tag 501, the name, the number of
// fields, then each key and value.
func (f Fact) Fold(h *trace.Hash) {
	h.Uint64(tagFact)
	h.String(f.Name)
	h.Uint64(uint64(len(f.Fields)))
	for _, fl := range f.Fields {
		h.String(fl.Key)
		fl.Value.Fold(h)
	}
}

// Field is one key and value of a Fact.
type Field struct {
	Key   string
	Value FieldValue
}

// FieldValue is the value of one Field. The cases are Int64Value,
// Uint64Value, StringValue, Bool and Bytes.
//
//sumtype:decl
type FieldValue interface {
	isFieldValue()
	Fold(h *trace.Hash)
}

// Int64Value is a signed integer field value.
type Int64Value int64

// Uint64Value is an unsigned integer field value.
type Uint64Value uint64

// StringValue is a text field value.
type StringValue string

// Bool is a true or false field value.
type Bool bool

// Bytes is a byte slice field value.
type Bytes []byte

func (Int64Value) isFieldValue()  {}
func (Uint64Value) isFieldValue() {}
func (StringValue) isFieldValue() {}
func (Bool) isFieldValue()        {}
func (Bytes) isFieldValue()       {}

// Fold writes tag 401 and the value.
func (v Int64Value) Fold(h *trace.Hash) {
	h.Uint64(tagInt64Value)
	h.Int64(int64(v))
}

// Fold writes tag 402 and the value.
func (v Uint64Value) Fold(h *trace.Hash) {
	h.Uint64(tagUint64Value)
	h.Uint64(uint64(v))
}

// Fold writes tag 403 and the text.
func (v StringValue) Fold(h *trace.Hash) {
	h.Uint64(tagStringValue)
	h.String(string(v))
}

// Fold writes tag 404 and the value.
func (v Bool) Fold(h *trace.Hash) {
	h.Uint64(tagBool)
	h.Bool(bool(v))
}

// Fold writes tag 405 and the bytes.
func (v Bytes) Fold(h *trace.Hash) {
	h.Uint64(tagBytes)
	h.Bytes(v)
}
