package spectest

import "testing"

func TestRerunLine(t *testing.T) {
	cases := []struct {
		r    Rerun
		want string
	}{
		{Rerun{EnvPrefix: "P"}, "P_SEED=17 go test -run '^TestA$/^b\\.c$' -count=1 -v ."},
		{Rerun{}, "go test -run '^TestA$/^b\\.c$' -count=1 -v . -rapid.seed=17 -rapid.checks=1"},
		{Rerun{EnvPrefix: "P", Command: func(seed uint64, test string) string { return test }}, "TestA/b.c"},
	}
	for _, c := range cases {
		if got := c.r.Line(17, "TestA/b.c"); got != c.want {
			t.Errorf("Line = %q, want %q", got, c.want)
		}
	}
}

func TestSeedVar(t *testing.T) {
	if got := (Rerun{EnvPrefix: "X"}).SeedVar(); got != "X_SEED" {
		t.Fatalf("got %q", got)
	}
	if got := (Rerun{}).SeedVar(); got != "" {
		t.Fatalf("got %q", got)
	}
}

func TestRunPatternQuotesSpecialCharacters(t *testing.T) {
	got := RunPattern(`TestX/a+b(c)[d]{e}|f*g?h^i$j\k`)
	want := `^TestX$/^a\+b\(c\)\[d\]\{e\}\|f\*g\?h\^i\$j\\k$`
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestSeedFromEnv(t *testing.T) {
	r := Rerun{EnvPrefix: "QQ"}
	if _, ok, err := r.seedFromEnv(); ok || err != nil {
		t.Fatalf("no variable: ok=%v err=%v", ok, err)
	}
	t.Setenv("QQ_SEED", "")
	if _, ok, err := r.seedFromEnv(); ok || err != nil {
		t.Fatalf("empty variable: ok=%v err=%v", ok, err)
	}
	t.Setenv("QQ_SEED", "42")
	if seed, ok, err := r.seedFromEnv(); !ok || err != nil || seed != 42 {
		t.Fatalf("seed=%d ok=%v err=%v", seed, ok, err)
	}
	for _, bad := range []string{"0", "-1", "x"} {
		t.Setenv("QQ_SEED", bad)
		if _, _, err := r.seedFromEnv(); err == nil {
			t.Fatalf("value %q must be an error", bad)
		}
	}
	if _, ok, err := (Rerun{}).seedFromEnv(); ok || err != nil {
		t.Fatalf("no prefix: ok=%v err=%v", ok, err)
	}
}
