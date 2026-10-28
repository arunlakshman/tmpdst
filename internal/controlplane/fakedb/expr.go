package fakedb

import (
	"bytes"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// The expression subset the store uses:
//
//	condition  atom {" OR " atom}; atom: attribute_not_exists(n) | n = :v | n < :v
//	update     "ADD n :v {, n :v}" | "SET n = :v {, n = :v}"
//	key cond   "pk = :p" | "pk = :p AND sk BETWEEN :lo AND :hi"
//
// Anything else is a ValidationException, so a new expression in the store
// fails loudly here instead of being silently mis-evaluated.

func stringOf(it item, name string) string {
	v, _ := it[name].(*types.AttributeValueMemberS)
	if v == nil {
		return ""
	}
	return v.Value
}

func itemIDOf(it item) string { return stringOf(it, "pk") + "\x00" + stringOf(it, "sk") }

func cloneValue(v types.AttributeValue) types.AttributeValue {
	if b, ok := v.(*types.AttributeValueMemberB); ok {
		return &types.AttributeValueMemberB{Value: bytes.Clone(b.Value)}
	}
	return v // S and N values are immutable
}

func cloneItem(it item) item {
	if it == nil {
		return nil
	}
	c := make(item, len(it))
	for k, v := range it {
		c[k] = cloneValue(v)
	}
	return c
}

// compare orders two values of one scalar type.
func compare(a, b types.AttributeValue) (int, error) {
	switch x := a.(type) {
	case *types.AttributeValueMemberN:
		y, ok := b.(*types.AttributeValueMemberN)
		if !ok {
			return 0, validation("type mismatch")
		}
		return compareNumbers(x.Value, y.Value)
	case *types.AttributeValueMemberS:
		y, ok := b.(*types.AttributeValueMemberS)
		if !ok {
			return 0, validation("type mismatch")
		}
		return strings.Compare(x.Value, y.Value), nil
	}
	return 0, validation("unsupported type %T in a comparison", a)
}

func compareNumbers(a, b string) (int, error) {
	x, err := strconv.ParseInt(a, 10, 64)
	if err != nil {
		return 0, validation("bad number %q", a)
	}
	y, err := strconv.ParseInt(b, 10, 64)
	if err != nil {
		return 0, validation("bad number %q", b)
	}
	switch {
	case x < y:
		return -1, nil
	case x > y:
		return 1, nil
	}
	return 0, nil
}

// evalCondition evaluates a ConditionExpression against cur (nil: no such item).
// A nil expression is true.
func evalCondition(expr *string, vals item, cur item) (bool, error) {
	if expr == nil {
		return true, nil
	}
	for alt := range strings.SplitSeq(*expr, " OR ") {
		ok, err := evalSimpleCondition(strings.TrimSpace(alt), vals, cur)
		if ok || err != nil {
			return ok, err
		}
	}
	return false, nil
}

func evalSimpleCondition(atom string, vals, cur item) (bool, error) {
	if name, ok := strings.CutPrefix(atom, "attribute_not_exists("); ok {
		_, has := cur[strings.TrimSuffix(name, ")")]
		return !has, nil
	}
	f := strings.Fields(atom)
	if len(f) != 3 || (f[1] != "=" && f[1] != "<") {
		return false, validation("unsupported condition %q", atom)
	}
	want, ok := vals[f[2]]
	if !ok {
		return false, validation("value %s not provided", f[2])
	}
	have, ok := cur[f[0]]
	if !ok {
		return false, nil
	}
	c, err := compare(have, want)
	return (f[1] == "=" && c == 0) || (f[1] == "<" && c < 0), err
}

// applyUpdate applies an UpdateExpression to a copy of cur (nil: a new item
// with key) and returns it with the names of the attributes it set.
func applyUpdate(expr string, vals item, key, cur item) (item, []string, error) {
	out := cloneItem(cur)
	var set []string
	if out == nil {
		out = cloneItem(key)
	}
	verb, rest, _ := strings.Cut(expr, " ")
	if verb != "SET" && verb != "ADD" {
		return nil, nil, validation("unsupported update %q", expr)
	}
	for clause := range strings.SplitSeq(rest, ",") {
		name, err := applyClause(verb, strings.Fields(clause), vals, out)
		if err != nil {
			return nil, nil, err
		}
		set = append(set, name)
	}
	return out, set, nil
}

// applyClause applies "n = :v" (SET) or "n :v" (ADD) to it and returns n.
func applyClause(verb string, f []string, vals, it item) (string, error) {
	name, v, err := clauseValue(verb, f, vals)
	if err != nil {
		return "", err
	}
	if verb == "SET" {
		it[name] = cloneValue(v)
		return name, nil
	}
	sum, err := addNumbers(it[name], v)
	it[name] = sum
	return name, err
}

func clauseValue(verb string, f []string, vals item) (string, types.AttributeValue, error) {
	if verb == "SET" && len(f) == 3 && f[1] == "=" {
		f = []string{f[0], f[2]}
	} else if verb == "SET" || len(f) != 2 {
		return "", nil, validation("unsupported %s clause %q", verb, strings.Join(f, " "))
	}
	v, ok := vals[f[1]]
	if !ok {
		return "", nil, validation("value %s not provided", f[1])
	}
	return f[0], v, nil
}

// addNumbers is ADD for numbers: an absent attribute counts as 0.
func addNumbers(cur, delta types.AttributeValue) (types.AttributeValue, error) {
	d, ok := delta.(*types.AttributeValueMemberN)
	if !ok {
		return nil, validation("ADD needs a number")
	}
	base := "0"
	if c, ok := cur.(*types.AttributeValueMemberN); ok {
		base = c.Value
	}
	x, err1 := strconv.ParseInt(base, 10, 64)
	y, err2 := strconv.ParseInt(d.Value, 10, 64)
	if err1 != nil || err2 != nil {
		return nil, validation("ADD needs integers")
	}
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(x+y, 10)}, nil
}

// keyCondition is a parsed key condition: partition pk, optionally sk in [lo, hi].
type keyCondition struct {
	pk       string
	lo, hi   string
	hasRange bool
}

func parseKeyCond(expr string, vals item) (keyCondition, error) {
	parts := strings.Split(expr, " AND ")
	var kc keyCondition
	pk, ok := valueOf(strings.Fields(parts[0]), "pk", "=", vals)
	if !ok || (len(parts) != 1 && len(parts) != 3) {
		return kc, validation("unsupported key condition %q", expr)
	}
	kc.pk = pk
	if len(parts) == 1 {
		return kc, nil
	}
	return parseKeyRange(kc, parts, vals, expr)
}

func parseKeyRange(kc keyCondition, parts []string, vals item, expr string) (keyCondition, error) {
	lo, ok1 := valueOf(strings.Fields(parts[1]), "sk", "BETWEEN", vals)
	hi, ok2 := valueOf([]string{"", "", strings.TrimSpace(parts[2])}, "", "", vals)
	if !ok1 || !ok2 {
		return kc, validation("unsupported key condition %q", expr)
	}
	kc.lo, kc.hi, kc.hasRange = lo, hi, true
	return kc, nil
}

// valueOf matches "<name> <op> :v" and resolves :v to a string.
func valueOf(f []string, name, op string, vals item) (string, bool) {
	if len(f) != 3 || (name != "" && (f[0] != name || f[1] != op)) {
		return "", false
	}
	s, ok := vals[f[2]].(*types.AttributeValueMemberS)
	if !ok {
		return "", false
	}
	return s.Value, true
}

func (kc keyCondition) matches(it item) bool {
	if stringOf(it, "pk") != kc.pk {
		return false
	}
	sk := stringOf(it, "sk")
	return !kc.hasRange || (sk >= kc.lo && sk <= kc.hi)
}
