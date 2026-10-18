// Package json2yaml renders a JSON document as block-style YAML, keeping the
// key order. It supports only what JSON has; every string that is not a
// plain multi-line text is double-quoted, so the output is unambiguous.
package json2yaml

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

type object struct {
	keys []string
	vals []any
}

// Convert returns the YAML for the JSON document in src.
func Convert(src []byte) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(src))
	dec.UseNumber()
	v, err := parse(dec)
	if err != nil {
		return nil, err
	}
	var out []string
	if text, ok := inline(v); ok {
		out = []string{text}
	} else {
		out = block(v, 0)
	}
	return []byte(strings.Join(out, "\n") + "\n"), nil
}

func parse(dec *json.Decoder) (any, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	d, ok := tok.(json.Delim)
	if !ok {
		return tok, nil
	}
	if d == '[' {
		return parseArray(dec)
	}
	return parseObject(dec)
}

func parseArray(dec *json.Decoder) (any, error) {
	arr := []any{}
	for dec.More() {
		v, err := parse(dec)
		if err != nil {
			return nil, err
		}
		arr = append(arr, v)
	}
	_, err := dec.Token()
	return arr, err
}

func parseObject(dec *json.Decoder) (any, error) {
	o := &object{}
	for dec.More() {
		k, err := dec.Token()
		if err != nil {
			return nil, err
		}
		v, err := parse(dec)
		if err != nil {
			return nil, err
		}
		o.keys = append(o.keys, k.(string))
		o.vals = append(o.vals, v)
	}
	_, err := dec.Token()
	return o, err
}

var plainKey = regexp.MustCompile(`^[A-Za-z_$][A-Za-z0-9_$]*$`)

func quote(s string) string {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(s)
	return strings.TrimRight(b.String(), "\n")
}

func key(k string) string {
	if plainKey.MatchString(k) {
		return k
	}
	return quote(k)
}

// literal reports whether s can be written as a |- block scalar.
func literal(s string) bool {
	if !strings.Contains(s, "\n") || strings.HasPrefix(s, " ") || strings.HasPrefix(s, "\n") || strings.HasSuffix(s, "\n") {
		return false
	}
	for _, r := range s {
		if r != '\n' && (r < ' ' || r == 0x7f) {
			return false
		}
	}
	for _, ln := range strings.Split(s, "\n") {
		if strings.HasSuffix(ln, " ") {
			return false
		}
	}
	return true
}

// inline renders scalars and empty containers on one line.
func inline(v any) (string, bool) {
	switch x := v.(type) {
	case nil:
		return "null", true
	case bool:
		return fmt.Sprint(x), true
	case json.Number:
		return x.String(), true
	case string:
		return quote(x), !literal(x)
	case []any:
		return "[]", len(x) == 0
	case *object:
		return "{}", len(x.keys) == 0
	}
	return "", false
}

// block renders v (not inline) with its lines indented by ind spaces.
func block(v any, ind int) []string {
	pad := strings.Repeat(" ", ind)
	switch x := v.(type) {
	case string:
		return text(x, pad)
	case []any:
		var out []string
		for _, it := range x {
			out = append(out, item(it, pad)...)
		}
		return out
	}
	o := v.(*object)
	var out []string
	for i, k := range o.keys {
		out = append(out, entry(pad+key(k)+":", o.vals[i], ind)...)
	}
	return out
}

// entry renders head followed by v: inline after it, or a nested block.
func entry(head string, v any, ind int) []string {
	if t, ok := inline(v); ok {
		return []string{head + " " + t}
	}
	if s, ok := v.(string); ok {
		return append([]string{head + " |-"}, text(s, strings.Repeat(" ", ind+2))...)
	}
	return append([]string{head}, block(v, ind+2)...)
}

func item(v any, pad string) []string {
	if t, ok := inline(v); ok {
		return []string{pad + "- " + t}
	}
	if s, ok := v.(string); ok {
		return append([]string{pad + "- |-"}, text(s, pad+"  ")...)
	}
	lines := block(v, len(pad)+2)
	lines[0] = pad + "- " + strings.TrimPrefix(lines[0], pad+"  ")
	return lines
}

func text(s, pad string) []string {
	var out []string
	for _, ln := range strings.Split(s, "\n") {
		if ln == "" {
			out = append(out, "")
		} else {
			out = append(out, pad+ln)
		}
	}
	return out
}
