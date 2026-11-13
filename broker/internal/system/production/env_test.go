package production

import "testing"

func TestEnvIntnAndBetweenStayInRange(t *testing.T) {
	e, err := NewEnv(nil)
	if err != nil {
		t.Fatal(err)
	}
	if e.Intn(0) != 0 || e.Intn(-3) != 0 {
		t.Fatal("Intn with n <= 0 must give 0")
	}
	seen := map[int64]bool{}
	for range 500 {
		if n := e.Intn(5); n < 0 || n >= 5 {
			t.Fatalf("Intn(5) = %d", n)
		}
		v := e.Between(-2, 2)
		if v < -2 || v > 2 {
			t.Fatalf("Between(-2, 2) = %d", v)
		}
		seen[v] = true
	}
	if len(seen) != 5 {
		t.Fatalf("Between(-2, 2) gave %d different values, want both ends included", len(seen))
	}
	if e.Between(4, 4) != 4 || e.Between(9, 3) != 9 {
		t.Fatal("Between with hi <= lo must give lo")
	}
	_ = e.Between(-1<<63, 1<<63-1) // the whole range must not divide by zero
}

func TestEnvChance(t *testing.T) {
	e, err := NewEnv(nil)
	if err != nil {
		t.Fatal(err)
	}
	if e.Chance(0) || e.Chance(-1) {
		t.Fatal("Chance(p <= 0) must be false")
	}
	for range 100 {
		if !e.Chance(1) {
			t.Fatal("Chance(1) must be true")
		}
	}
	hits := 0
	for range 2000 {
		if e.Chance(0.5) {
			hits++
		}
	}
	if hits < 800 || hits > 1200 {
		t.Fatalf("Chance(0.5) hit %d of 2000", hits)
	}
}
