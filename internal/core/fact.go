package core

// Fact is a structured, append-only observation (lesson 10). Name is a stable
// constant ("segment_committed", "produce_acked"). Fields are ordered.
//
// Name and the field encoding are trace-hash inputs: frozen, past-tense
// constants, never derived from Go type names. Renaming one re-baselines the
// golden hash. An ack fact is emitted in the same Step, immediately before the
// Send that acks, so invariants can check "acked implies committed" on that Step.
type Fact struct {
	Name   string
	Fields []Field
}

// Field returns the value for key, or nil if absent.
func (f Fact) Field(key string) FieldValue {
	for _, x := range f.Fields {
		if x.Key == key {
			return x.Value
		}
	}
	return nil
}

// Field is one named value of a Fact.
type Field struct {
	Key   string
	Value FieldValue
}

// FieldValue is restricted to comparable plain data.
type FieldValue interface{ isFieldValue() }

type (
	// Int64Value is a signed integer field value.
	Int64Value int64
	// Uint64Value is an unsigned integer field value.
	Uint64Value uint64
	// StringValue is a string field value.
	StringValue string
	// Bool is a boolean field value.
	Bool bool
	// Bytes is a byte-slice field value.
	Bytes []byte
)

func (Int64Value) isFieldValue()  {}
func (Uint64Value) isFieldValue() {}
func (StringValue) isFieldValue() {}
func (Bool) isFieldValue()        {}
func (Bytes) isFieldValue()       {}
