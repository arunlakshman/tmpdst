package spectest

import (
	"fmt"
	"strings"
)

// Rerun builds the command that runs one failing seed again.
//
// EnvPrefix is set by the product (dst has no product words). With the prefix
// "P", the command sets the variable P_SEED. Lockstep.Check reads P_SEED and
// then runs only that seed. With an empty prefix, the command uses the rapid
// flags -rapid.seed and -rapid.checks instead.
//
// Command, when it is not nil, replaces the default command completely.
type Rerun struct {
	EnvPrefix string
	Command   func(seed uint64, test string) string
}

// SeedVar returns the name of the seed variable, for example "P_SEED", or ""
// when EnvPrefix is empty.
func (r Rerun) SeedVar() string {
	if r.EnvPrefix == "" {
		return ""
	}
	return r.EnvPrefix + "_SEED"
}

// Line returns the command that runs seed of the test named test again. test
// is the full test name (t.Name()). Run the command in the package folder.
func (r Rerun) Line(seed uint64, test string) string {
	if r.Command != nil {
		return r.Command(seed, test)
	}
	if r.EnvPrefix == "" {
		return fmt.Sprintf("go test -run '%s' -count=1 -v . -rapid.seed=%d -rapid.checks=1", RunPattern(test), seed)
	}
	return fmt.Sprintf("%s=%d go test -run '%s' -count=1 -v .", r.SeedVar(), seed, RunPattern(test))
}

// RunPattern returns a -run pattern for go test that matches exactly the test
// named test. A subtest name has one part per level, split by "/", and each
// part is matched on its own.
func RunPattern(test string) string {
	parts := strings.Split(test, "/")
	for i, p := range parts {
		parts[i] = "^" + quoteRegexp(p) + "$"
	}
	return strings.Join(parts, "/")
}

// quoteRegexp puts a backslash before each regular expression special
// character, like regexp.QuoteMeta.
func quoteRegexp(s string) string {
	var b strings.Builder
	for _, c := range s {
		if strings.ContainsRune(`\.+*?()|[]{}^$`, c) {
			b.WriteByte('\\')
		}
		b.WriteRune(c)
	}
	return b.String()
}
