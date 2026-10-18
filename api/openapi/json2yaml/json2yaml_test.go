package json2yaml

import "testing"

func TestConvert(t *testing.T) {
	src := `{"a":1,"b/c":[true,null,{"x":"y"},[1,2],"l1\nl2"],"d":{},"e":[],"f":"q\"\n","g":"two\nlines","h":{"i":{"j":"k"}}}`
	want := `a: 1
"b/c":
  - true
  - null
  - x: "y"
  - - 1
    - 2
  - |-
    l1
    l2
d: {}
e: []
f: "q\"\n"
g: |-
  two
  lines
h:
  i:
    j: "k"
`
	got, err := Convert([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Fatalf("got:\n%s\nwant:\n%s", got, want)
	}
}

func TestConvertScalarAndErrors(t *testing.T) {
	if got, err := Convert([]byte(`"x"`)); err != nil || string(got) != "\"x\"\n" {
		t.Fatalf("%q %v", got, err)
	}
	for _, bad := range []string{``, `{`, `[1,`, `{"a":}`} {
		if _, err := Convert([]byte(bad)); err == nil {
			t.Errorf("%q: want error", bad)
		}
	}
}

func TestLiteralRules(t *testing.T) {
	for s, want := range map[string]bool{"a\nb": true, "a": false, " a\nb": false, "a \nb": false, "a\n": false, "a\tb\nc": false} {
		if literal(s) != want {
			t.Errorf("literal(%q) != %v", s, want)
		}
	}
}
