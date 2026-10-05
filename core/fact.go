package core

// Fact is a structured, append-only observation (lesson 10). Name is a stable
// constant ("segment_committed", "produce_acked"). Fields are ordered.
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
