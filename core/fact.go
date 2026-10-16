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

type Field struct {
	Key   string
	Value FieldValue
}

// FieldValue is restricted to comparable plain data.
type FieldValue interface{ isFieldValue() }

type (
	I64   int64
	U64   uint64
	Str   string
	Bool  bool
	Bytes []byte
)

func (I64) isFieldValue()   {}
func (U64) isFieldValue()   {}
func (Str) isFieldValue()   {}
func (Bool) isFieldValue()  {}
func (Bytes) isFieldValue() {}
