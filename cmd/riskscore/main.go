// Command riskscore computes the CRAP (Change Risk Anti-Patterns) score of
// every Go function from cyclomatic complexity and a coverprofile, and fails
// when a function is too crappy. See docs/crap-gate.md.
//
// Standard library only (dependency-audit policy, decision I).
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

const rebuildHint = "go run ./cmd/riskscore -profile <cover.out> -update-baseline   (or: make crap-baseline); review and commit the diff of ops/crap-baseline.json"

// Baseline is the checked-in ratchet file: the offenders that existed when it
// was generated, with their CRAP at that time.
type Baseline struct {
	Threshold float64            `json:"threshold"`
	Note      string             `json:"note"`
	Functions map[string]float64 `json:"functions"`
}

// Result is the outcome of comparing the offenders with the baseline.
type Result struct {
	Offenders []*Func  // all functions over the threshold
	New       []*Func  // not in baseline
	Worse     []*Func  // in baseline, CRAP grew beyond tolerance
	Improved  []string // baselined, now fine or better (baseline could be tightened)
}

func (v Result) Failed() bool { return len(v.New)+len(v.Worse) > 0 }

// Evaluate applies the ratchet. With base == nil (strict) every offender fails.
func Evaluate(funcs []*Func, threshold float64, base *Baseline, tol float64) Result {
	var v Result
	seen := map[string]bool{}
	for _, f := range funcs {
		seen[f.Key] = true
		if f.RiskScore <= threshold {
			continue
		}
		v.Offenders = append(v.Offenders, f)
		if base == nil {
			v.New = append(v.New, f)
			continue
		}
		old, ok := base.Functions[f.Key]
		switch {
		case !ok:
			v.New = append(v.New, f)
		case f.RiskScore > old+tol:
			v.Worse = append(v.Worse, f)
		}
	}
	if base != nil {
		for k, old := range base.Functions {
			if !seen[k] {
				v.Improved = append(v.Improved, fmt.Sprintf("%s: gone (was %.1f)", k, old))
			}
		}
		for _, f := range funcs {
			if old, ok := base.Functions[f.Key]; ok && f.RiskScore <= threshold {
				v.Improved = append(v.Improved, fmt.Sprintf("%s: %.1f -> %.1f (now within threshold)", f.Key, old, f.RiskScore))
			} else if ok && f.RiskScore < old-tol {
				v.Improved = append(v.Improved, fmt.Sprintf("%s: %.1f -> %.1f", f.Key, old, f.RiskScore))
			}
		}
		sort.Strings(v.Improved)
	}
	return v
}

var reportThresholds = []float64{30, 15, 10, 6}

func envFloat(name string, def float64) float64 {
	if s := os.Getenv(name); s != "" {
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return f
		}
	}
	return def
}

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

type config struct {
	root, profile, merge, basePath, exclude string
	threshold, tol                          float64
	update, strict, asJSON                  bool
	top                                     int
}

func parseFlags(args []string, stderr io.Writer) (*config, bool) {
	fs := flag.NewFlagSet("riskscore", flag.ContinueOnError)
	fs.SetOutput(stderr)
	c := &config{}
	fs.StringVar(&c.root, "root", ".", "module root (directory with go.mod)")
	fs.StringVar(&c.profile, "profile", "", "coverprofile from go test -coverprofile (required)")
	fs.StringVar(&c.merge, "merge-profile", "", "comma-separated extra coverprofiles to merge (e.g. from Docker-backed integration tests)")
	fs.Float64Var(&c.threshold, "threshold", envFloat("CRAP_THRESHOLD", 20), "fail above this CRAP score (env CRAP_THRESHOLD)")
	fs.StringVar(&c.basePath, "baseline", "ops/crap-baseline.json", "ratchet baseline file")
	fs.BoolVar(&c.update, "update-baseline", false, "write the baseline from the current offenders and exit 0")
	fs.BoolVar(&c.strict, "strict", false, "ignore the baseline: fail on every offender")
	fs.BoolVar(&c.asJSON, "json", false, "machine-readable output")
	fs.IntVar(&c.top, "top", 25, "rows in the table (0 = all functions)")
	fs.Float64Var(&c.tol, "tolerance", 0.5, "CRAP increase tolerated for a baselined function")
	fs.StringVar(&c.exclude, "exclude", "", "regexp on the function key (dir.Recv.Name) to leave out of analysis")
	if err := fs.Parse(args); err != nil {
		return nil, false
	}
	if c.profile == "" {
		_, _ = fmt.Fprintln(stderr, "riskscore: -profile is required")
		return nil, false
	}
	return c, true
}

// analyse parses the tree, applies the exclude filter and the merged coverage,
// and returns the functions sorted by CRAP, worst first.
func analyse(c *config) ([]*Func, error) {
	funcs, err := ParseTree(c.root)
	if err != nil {
		return nil, err
	}
	funcs, err = removeExcluded(funcs, c.exclude)
	if err != nil {
		return nil, err
	}
	if err := applyCoverage(c, funcs); err != nil {
		return nil, err
	}
	sort.SliceStable(funcs, func(i, j int) bool { return funcs[i].RiskScore > funcs[j].RiskScore })
	return funcs, nil
}

func removeExcluded(funcs []*Func, exclude string) ([]*Func, error) {
	if exclude == "" {
		return funcs, nil
	}
	re, err := regexp.Compile(exclude)
	if err != nil {
		return nil, fmt.Errorf("-exclude: %w", err)
	}
	kept := funcs[:0]
	for _, f := range funcs {
		if !re.MatchString(f.Key) {
			kept = append(kept, f)
		}
	}
	return kept, nil
}

func applyCoverage(c *config, funcs []*Func) error {
	module, err := ModulePath(c.root)
	if err != nil {
		return err
	}
	prof := Profile{}
	paths := []string{c.profile}
	for m := range strings.SplitSeq(c.merge, ",") {
		if m = strings.TrimSpace(m); m != "" {
			paths = append(paths, m)
		}
	}
	for _, p := range paths {
		if err := prof.ReadProfileFile(p); err != nil {
			return err
		}
	}
	prof.Apply(module, funcs)
	return nil
}

func writeBaseline(c *config, funcs []*Func) (int, error) {
	b := Baseline{
		Threshold: c.threshold,
		Note:      "Ratchet baseline for cmd/riskscore. Regenerate: " + rebuildHint,
		Functions: map[string]float64{},
	}
	for _, f := range funcs {
		if f.RiskScore > c.threshold {
			b.Functions[f.Key] = round2(f.RiskScore)
		}
	}
	data, _ := json.MarshalIndent(b, "", "  ")
	return len(b.Functions), os.WriteFile(c.basePath, append(data, '\n'), 0o600)
}

// loadBaseline returns the baseline (nil in strict mode) and a warning text.
func loadBaseline(c *config) (*Baseline, string, error) {
	if c.strict {
		return nil, "", nil
	}
	data, err := os.ReadFile(c.basePath)
	if os.IsNotExist(err) {
		return &Baseline{Functions: map[string]float64{}}, "no baseline file " + c.basePath + ": every offender fails", nil
	}
	if err != nil {
		return nil, "", err
	}
	base := &Baseline{}
	if err := json.Unmarshal(data, base); err != nil {
		return nil, "", fmt.Errorf("%s: %w", c.basePath, err)
	}
	warn := ""
	if base.Threshold != c.threshold {
		warn = fmt.Sprintf("baseline was generated at threshold %.0f but the gate runs at %.0f; regenerate it", base.Threshold, c.threshold)
	}
	return base, warn, nil
}

func run(args []string, stdout, stderr io.Writer) int {
	c, ok := parseFlags(args, stderr)
	if !ok {
		return 2
	}
	funcs, err := analyse(c)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "riskscore:", err)
		return 2
	}
	if c.update {
		n, err := writeBaseline(c, funcs)
		if err != nil {
			_, _ = fmt.Fprintln(stderr, "riskscore:", err)
			return 2
		}
		_, _ = fmt.Fprintf(stdout, "riskscore: wrote %s with %d offenders over %.0f\n", c.basePath, n, c.threshold)
		return 0
	}
	base, warn, err := loadBaseline(c)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "riskscore:", err)
		return 2
	}
	v := Evaluate(funcs, c.threshold, base, c.tol)
	if c.asJSON {
		writeJSON(stdout, funcs, v, c.threshold, c.top, c.strict, warn)
	} else {
		writeText(stdout, funcs, v, c.threshold, c.top, c.strict, warn)
	}
	if v.Failed() {
		return 1
	}
	return 0
}

func round2(f float64) float64 { return float64(int(f*100+0.5)) / 100 }

func counts(funcs []*Func) map[float64]int {
	m := map[float64]int{}
	for _, t := range reportThresholds {
		for _, f := range funcs {
			if f.RiskScore > t {
				m[t]++
			}
		}
	}
	return m
}

func writeText(w io.Writer, funcs []*Func, v Result, threshold float64, top int, strict bool, warn string) {
	n := len(funcs)
	if top > 0 && top < n {
		n = top
	}
	_, _ = fmt.Fprintf(w, "%8s %5s %6s  %-52s %s\n", "CRAP", "CC", "COV%", "FUNCTION", "LOCATION")
	for _, f := range funcs[:n] {
		_, _ = fmt.Fprintf(w, "%8.1f %5d %5.1f%%  %-52s %s:%d\n", f.RiskScore, f.Complexity, f.Coverage*100, f.Key, f.File, f.Line)
	}
	c := counts(funcs)
	_, _ = fmt.Fprintf(w, "\n%d functions analysed. Functions with CRAP over:", len(funcs))
	for _, t := range reportThresholds {
		_, _ = fmt.Fprintf(w, "  %.0f: %d", t, c[t])
	}
	_, _ = fmt.Fprintln(w)
	if warn != "" {
		_, _ = fmt.Fprintln(w, "WARNING:", warn)
	}
	if len(v.Improved) > 0 {
		_, _ = fmt.Fprintf(w, "%d baselined functions improved or vanished (baseline can be tightened).\n", len(v.Improved))
	}
	mode := "ratchet"
	if strict {
		mode = "strict"
	}
	if !v.Failed() {
		_, _ = fmt.Fprintf(w, "riskscore: OK (%s, threshold %.0f, %d offenders tolerated)\n", mode, threshold, len(v.Offenders))
		return
	}
	_, _ = fmt.Fprintf(w, "\nriskscore: FAIL (%s, threshold %.0f)\n", mode, threshold)
	for _, f := range v.New {
		_, _ = fmt.Fprintf(w, "  over threshold:  %-50s CRAP %.1f (CC %d, cov %.0f%%) %s:%d\n", f.Key, f.RiskScore, f.Complexity, f.Coverage*100, f.File, f.Line)
	}
	for _, f := range v.Worse {
		_, _ = fmt.Fprintf(w, "  got worse:       %-50s CRAP %.1f (CC %d, cov %.0f%%) %s:%d\n", f.Key, f.RiskScore, f.Complexity, f.Coverage*100, f.File, f.Line)
	}
	_, _ = fmt.Fprintln(w, "Fix: add tests or split the function. Only if the offender is accepted, regenerate the baseline deliberately:")
	_, _ = fmt.Fprintln(w, "  "+rebuildHint)
}

func writeJSON(w io.Writer, funcs []*Func, v Result, threshold float64, top int, strict bool, warn string) {
	n := len(funcs)
	if top > 0 && top < n {
		n = top
	}
	c := map[string]int{}
	for t, k := range counts(funcs) {
		c[strconv.FormatFloat(t, 'f', -1, 64)] = k
	}
	out := map[string]any{
		"threshold": threshold, "strict": strict, "ok": !v.Failed(), "warning": warn,
		"analysed": len(funcs), "over": c,
		"new": keys(v.New), "worse": keys(v.Worse), "improved": v.Improved,
		"functions": funcs[:n],
	}
	data, _ := json.MarshalIndent(out, "", "  ")
	_, _ = fmt.Fprintln(w, string(data))
}

func keys(fs []*Func) []string {
	out := []string{}
	for _, f := range fs {
		out = append(out, f.Key)
	}
	return out
}
