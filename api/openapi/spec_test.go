package openapi_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"

	"manifester/api/openapi"
	"manifester/api/openapi/json2yaml"
	"manifester/core"
	"manifester/cp"
	"manifester/cp/memstore"
)

type doc = map[string]any

func load(t *testing.T) doc {
	t.Helper()
	var d doc
	if err := json.Unmarshal(openapi.JSON, &d); err != nil {
		t.Fatalf("controlplane.json does not parse: %v", err)
	}
	if v, _ := d["openapi"].(string); !strings.HasPrefix(v, "3.1.") {
		t.Fatalf("openapi version %q, want 3.1.x", v)
	}
	return d
}

func sub(m doc, keys ...string) doc {
	for _, k := range keys {
		next, _ := m[k].(doc)
		if next == nil {
			return doc{}
		}
		m = next
	}
	return m
}

func keysOf(m doc) []string {
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

// TestYAMLInSync: controlplane.yaml is exactly the rendering of the JSON source.
func TestYAMLInSync(t *testing.T) {
	want, err := json2yaml.Convert(openapi.JSON)
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile("controlplane.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("api/openapi/controlplane.yaml is out of date with controlplane.json: run `go run ./api/openapi/cmd/specgen`")
	}
}

var (
	postRoute = regexp.MustCompile(`post\(mux, "(/[^"]+)"`)
	getRoute  = regexp.MustCompile(`mux\.HandleFunc\("GET (/[^"]+)"`)
)

// handlerRoutes lists "METHOD path" for every route cp.NewHandler registers,
// read from the source so a new route cannot be forgotten.
func handlerRoutes(t *testing.T) []string {
	t.Helper()
	src, err := os.ReadFile("../../cp/handler.go")
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, m := range postRoute.FindAllStringSubmatch(string(src), -1) {
		if strings.HasPrefix(m[1], "/v1/") {
			out = append(out, "POST "+m[1])
		}
	}
	for _, m := range getRoute.FindAllStringSubmatch(string(src), -1) {
		out = append(out, "GET "+m[1])
	}
	sort.Strings(out)
	return out
}

func specRoutes(d doc) []string {
	var out []string
	for p, item := range sub(d, "paths") {
		for method := range item.(doc) {
			out = append(out, strings.ToUpper(method)+" "+p)
		}
	}
	sort.Strings(out)
	return out
}

func TestRoutesMatchHandler(t *testing.T) {
	code, spec := handlerRoutes(t), specRoutes(load(t))
	if len(code) < 5 {
		t.Fatalf("found only %v in cp/handler.go; has the registration style changed?", code)
	}
	for _, r := range code {
		if !slices.Contains(spec, r) {
			t.Errorf("route %s is registered in cp/handler.go but missing from the spec", r)
		}
	}
	for _, r := range spec {
		if !slices.Contains(code, r) {
			t.Errorf("route %s is in the spec but not registered in cp/handler.go", r)
		}
	}
}

// wireTypes maps each component schema to its Go wire type. Schemas listed in
// noGoType are spec-only.
var wireTypes = map[string]reflect.Type{
	"Batch": reflect.TypeOf(cp.Batch{}), "Range": reflect.TypeOf(cp.Range{}),
	"CommitReq": reflect.TypeOf(cp.CommitReq{}), "CommitResp": reflect.TypeOf(cp.CommitResp{}),
	"LookupReq": reflect.TypeOf(cp.LookupReq{}), "LookupResp": reflect.TypeOf(cp.LookupResp{}),
	"DescribeReq": reflect.TypeOf(cp.DescribeReq{}), "Topic": reflect.TypeOf(cp.Topic{}), "DescribeResp": reflect.TypeOf(cp.DescribeResp{}),
	"CreateTopicReq": reflect.TypeOf(cp.CreateTopicReq{}), "CreateTopicResp": reflect.TypeOf(cp.CreateTopicResp{}),
}

var noGoType = []string{"ErrCode", "ErrorBody"}

type field struct {
	name     string
	required bool
	typ      reflect.Type
}

func jsonFields(t reflect.Type) []field {
	var out []field
	for i := 0; i < t.NumField(); i++ {
		tag := t.Field(i).Tag.Get("json")
		name, opts, _ := strings.Cut(tag, ",")
		if name == "" || name == "-" {
			continue
		}
		out = append(out, field{name, !strings.Contains(opts, "omitempty"), t.Field(i).Type})
	}
	return out
}

// kindFormat is the (type, format) a Go kind has in the spec.
func kindFormat(k reflect.Kind) (string, string) {
	switch k {
	case reflect.String:
		return "string", ""
	case reflect.Bool:
		return "boolean", ""
	case reflect.Int64:
		return "integer", "int64"
	case reflect.Int32:
		return "integer", "int32"
	case reflect.Uint64:
		return "integer", "uint64"
	case reflect.Int16:
		return "integer", ""
	case reflect.Slice:
		return "array", ""
	}
	return "object", ""
}

func checkField(schema string, f field, p doc, schemas doc) []string {
	var errs []string
	if f.typ.Kind() == reflect.Struct {
		if r, _ := p["$ref"].(string); r != "#/components/schemas/"+f.typ.Name() {
			errs = append(errs, fmt.Sprintf("%s.%s: Go struct %s, spec has %v", schema, f.name, f.typ.Name(), p))
		}
		return errs
	}
	if f.name == "err" {
		if !strings.Contains(fmt.Sprint(p), "#/components/schemas/ErrCode") {
			errs = append(errs, schema+".err must reference ErrCode")
		}
		return errs
	}
	typ, format := kindFormat(f.typ.Kind())
	if p["type"] != typ {
		errs = append(errs, fmt.Sprintf("%s.%s: Go %s => type %q, spec has %v", schema, f.name, f.typ.Kind(), typ, p["type"]))
	}
	if f.typ.Kind() != reflect.Int16 && f.typ.Kind() != reflect.Slice && p["format"] != nilIfEmpty(format) {
		errs = append(errs, fmt.Sprintf("%s.%s: Go %s => format %q, spec has %v", schema, f.name, f.typ.Kind(), format, p["format"]))
	}
	if f.typ.Kind() == reflect.Int16 && (p["minimum"] != float64(-32768) || p["maximum"] != float64(32767)) {
		errs = append(errs, fmt.Sprintf("%s.%s: int16 needs minimum -32768 and maximum 32767", schema, f.name))
	}
	if f.typ.Kind() == reflect.Slice {
		errs = append(errs, checkItems(schema, f, p)...)
	}
	return errs
}

func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func checkItems(schema string, f field, p doc) []string {
	items := sub(p, "items")
	elem := f.typ.Elem()
	if elem.Kind() == reflect.Struct {
		if r, _ := items["$ref"].(string); r != "#/components/schemas/"+elem.Name() {
			return []string{fmt.Sprintf("%s.%s: items should reference %s, spec has %v", schema, f.name, elem.Name(), items)}
		}
	} else if items["type"] != "string" {
		return []string{fmt.Sprintf("%s.%s: items type %v, want string", schema, f.name, items["type"])}
	}
	return nil
}

func requiredSet(s doc) []string {
	var out []string
	rs, _ := s["required"].([]any)
	for _, r := range rs {
		out = append(out, r.(string))
	}
	sort.Strings(out)
	return out
}

func checkSchema(name string, typ reflect.Type, s doc, schemas doc) []string {
	var errs []string
	props := sub(s, "properties")
	var wantReq []string
	seen := map[string]bool{}
	for _, f := range jsonFields(typ) {
		seen[f.name] = true
		p, ok := props[f.name].(doc)
		if !ok {
			errs = append(errs, fmt.Sprintf("%s: Go field %q (%s) is missing from the schema", name, f.name, typ))
			continue
		}
		errs = append(errs, checkField(name, f, p, schemas)...)
		if f.required {
			wantReq = append(wantReq, f.name)
		}
	}
	for _, k := range keysOf(props) {
		if !seen[k] {
			errs = append(errs, fmt.Sprintf("%s: schema property %q has no JSON field in Go type %s", name, k, typ))
		}
	}
	sort.Strings(wantReq)
	if got := requiredSet(s); !slices.Equal(got, wantReq) {
		errs = append(errs, fmt.Sprintf("%s: required is %v, want %v (fields without omitempty)", name, got, wantReq))
	}
	return errs
}

func TestSchemasMatchWireTypes(t *testing.T) {
	schemas := sub(load(t), "components", "schemas")
	for _, name := range keysOf(schemas) {
		if _, ok := wireTypes[name]; !ok && !slices.Contains(noGoType, name) {
			t.Errorf("schema %s has no Go wire type: add it to wireTypes (or noGoType if spec-only)", name)
		}
	}
	for name, typ := range wireTypes {
		s, ok := schemas[name].(doc)
		if !ok {
			t.Errorf("Go wire type %s (%s) has no schema %s", typ, name, name)
			continue
		}
		for _, e := range checkSchema(name, typ, s, schemas) {
			t.Error(e)
		}
	}
}

// TestRoutesUseWireTypes: each operation's request and response schemas are the
// ones its Go handler decodes and encodes.
func TestOperationSchemas(t *testing.T) {
	want := map[string][2]string{
		"/v1/commit-segment": {"CommitReq", "CommitResp"}, "/v1/lookup": {"LookupReq", "LookupResp"},
		"/v1/describe-topics": {"DescribeReq", "DescribeResp"}, "/v1/create-topic": {"CreateTopicReq", "CreateTopicResp"},
	}
	d := load(t)
	for path, w := range want {
		op := sub(d, "paths", path, "post")
		req := sub(op, "requestBody", "content", "application/json", "schema")
		if req["$ref"] != "#/components/schemas/"+w[0] {
			t.Errorf("%s request schema is %v, want %s", path, req, w[0])
		}
		resp := fmt.Sprint(sub(op, "responses", "200", "content", "application/json", "schema"))
		if !strings.Contains(resp, "#/components/schemas/"+w[1]) {
			t.Errorf("%s 200 schema is %s, want a reference to %s", path, resp, w[1])
		}
		for _, code := range []string{"200", "500", "503"} {
			if _, ok := sub(op, "responses")[code]; !ok {
				t.Errorf("%s lacks a %s response", path, code)
			}
		}
	}
}

// TestErrCodeEnum: the enum is exactly the wire spellings cp.ParseErrCode accepts.
func TestErrCodeEnum(t *testing.T) {
	enum, _ := sub(load(t), "components", "schemas", "ErrCode")["enum"].([]any)
	var got, want []string
	for _, e := range enum {
		got = append(got, e.(string))
	}
	for c := core.ErrCode(1); c <= core.ErrIO; c++ { // the retired slot has no spelling
		if _, ok := cp.ParseErrCode(c.String()); ok {
			want = append(want, c.String())
		}
	}
	sort.Strings(got)
	sort.Strings(want)
	if !slices.Equal(got, want) {
		t.Fatalf("ErrCode enum %v, want %v (core.ErrCode spellings)", got, want)
	}
}

type replayStep struct{ path, name string }

func replaySteps(d doc) []replayStep {
	var out []replayStep
	steps, _ := d["x-replay"].([]any)
	for _, s := range steps {
		m := s.(doc)
		out = append(out, replayStep{m["path"].(string), m["example"].(string)})
	}
	return out
}

func exampleValue(op doc, side, name string) (any, bool) {
	var ex doc
	if side == "req" {
		ex = sub(op, "requestBody", "content", "application/json", "examples", name)
	} else {
		ex = sub(op, "responses", "200", "content", "application/json", "examples", name)
	}
	v, ok := ex["value"]
	return v, ok
}

func post(t *testing.T, h http.Handler, path string, body any) (int, []byte) {
	t.Helper()
	b, _ := json.Marshal(body)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, bytes.NewReader(b)))
	out, _ := io.ReadAll(rec.Body)
	return rec.Code, out
}

func sameJSON(got []byte, want any) error {
	var g any
	if err := json.Unmarshal(got, &g); err != nil {
		return fmt.Errorf("response is not JSON: %q", got)
	}
	if !reflect.DeepEqual(g, want) {
		w, _ := json.Marshal(want)
		return fmt.Errorf("got %s\n want %s", bytes.TrimSpace(got), w)
	}
	return nil
}

// TestExamplesReplay sends every worked example, in x-replay order, to one
// fresh cp.NewHandler over the memstore and compares the answer.
func TestExamplesReplay(t *testing.T) {
	d := load(t)
	steps := replaySteps(d)
	if len(steps) == 0 {
		t.Fatal("x-replay is empty")
	}
	h := cp.NewHandler(memstore.New())
	used := map[string]bool{}
	for _, s := range steps {
		op := sub(d, "paths", s.path, "post")
		req, ok1 := exampleValue(op, "req", s.name)
		resp, ok2 := exampleValue(op, "resp", s.name)
		if !ok1 || !ok2 {
			t.Errorf("%s example %q needs both a request and a response example", s.path, s.name)
			continue
		}
		used[s.path+" "+s.name] = true
		code, body := post(t, h, s.path, req)
		if code != http.StatusOK {
			t.Errorf("%s example %q: status %d, want 200", s.path, s.name, code)
		}
		if err := sameJSON(body, resp); err != nil {
			t.Errorf("%s example %q: %v", s.path, s.name, err)
		}
	}
	for path, item := range sub(d, "paths") {
		op := item.(doc)["post"]
		if op == nil {
			continue
		}
		for name := range sub(op.(doc), "requestBody", "content", "application/json", "examples") {
			if !used[path+" "+name] {
				t.Errorf("%s example %q is not in x-replay, so nothing checks it", path, name)
			}
		}
	}
}

func TestHealthzAndErrors(t *testing.T) {
	h := cp.NewHandler(memstore.New())
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/healthz", nil))
	want := sub(load(t), "paths", "/healthz", "get", "responses", "200", "content", "text/plain")["example"]
	if rec.Code != 200 || rec.Body.String() != want {
		t.Fatalf("healthz %d %q, spec example %q", rec.Code, rec.Body.String(), want)
	}
	// malformed body: 200 {"err":"ErrInvalid"}, as the spec's ErrorBody says.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/lookup", strings.NewReader("{")))
	if err := sameJSON(rec.Body.Bytes(), doc{"err": "ErrInvalid"}); err != nil || rec.Code != 200 {
		t.Fatalf("malformed body: %d %v", rec.Code, err)
	}
}
