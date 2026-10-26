package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

// Block is one coverprofile block.
type Block struct {
	File      string // path as written in the profile (import path form)
	StartLine int
	StartCol  int
	EndLine   int
	EndCol    int
	Stmts     int
	Count     int
}

type blockKey struct {
	file                  string
	sl, sc, el, ec, nstmt int
}

// Profile maps a profile file name to its merged blocks.
type Profile map[string][]Block

// ReadProfile parses a go coverprofile (mode set, count or atomic) and merges
// it into p. Blocks present in several profiles are merged by summing counts,
// so a block is covered if any contributing run covered it.
func (p Profile) ReadProfile(r io.Reader) error {
	idx := map[blockKey]int{}
	for _, bl := range p {
		for i, b := range bl {
			idx[keyOf(b)] = i
		}
	}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	ln := 0
	for sc.Scan() {
		ln++
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "mode:") {
			continue
		}
		b, err := parseBlock(line)
		if err != nil {
			return fmt.Errorf("line %d: %w", ln, err)
		}
		if i, ok := idx[keyOf(b)]; ok {
			p[b.File][i].Count += b.Count
			continue
		}
		idx[keyOf(b)] = len(p[b.File])
		p[b.File] = append(p[b.File], b)
	}
	return sc.Err()
}

func keyOf(b Block) blockKey {
	return blockKey{b.File, b.StartLine, b.StartCol, b.EndLine, b.EndCol, b.Stmts}
}

// parseBlock parses "file:sl.sc,el.ec nstmt count".
func parseBlock(line string) (Block, error) {
	var b Block
	colon := strings.LastIndex(line, ":")
	if colon < 0 {
		return b, fmt.Errorf("bad block %q", line)
	}
	b.File = line[:colon]
	var sl, sc, el, ec, n, c int
	if _, err := fmt.Sscanf(line[colon+1:], "%d.%d,%d.%d %d %d", &sl, &sc, &el, &ec, &n, &c); err != nil {
		return b, fmt.Errorf("bad block %q: %w", line, err)
	}
	b.StartLine, b.StartCol, b.EndLine, b.EndCol, b.Stmts, b.Count = sl, sc, el, ec, n, c
	return b, nil
}

// ReadProfileFile reads one profile file into p.
func (p Profile) ReadProfileFile(path string) error {
	f, err := os.Open(path) //nolint:gosec // G304: path is an explicit CLI argument
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	if err := p.ReadProfile(f); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}

// ModulePath returns the module path from root/go.mod.
func ModulePath(root string) (string, error) {
	b, err := os.ReadFile(root + "/go.mod") //nolint:gosec // G304: path is an explicit CLI argument
	if err != nil {
		return "", err
	}
	for l := range strings.SplitSeq(string(b), "\n") {
		if f := strings.Fields(l); len(f) == 2 && f[0] == "module" {
			return strings.Trim(f[1], `"`), nil
		}
	}
	return "", errors.New("no module line in go.mod")
}

// Apply fills Coverage and CRAP on every function. A block belongs to a
// function when it starts inside the function's line range (closures are part
// of their function). Coverage is covered statements / total statements. A
// function with no statements in a profiled file counts as fully covered;
// a function in a file absent from the profile counts as 0% covered.
func (p Profile) Apply(module string, funcs []*Func) {
	byFile := map[string][]Block{}
	for name, bl := range p {
		byFile[strings.TrimPrefix(name, module+"/")] = bl
	}
	for _, f := range funcs {
		bl, ok := byFile[f.File]
		total, covered := 0, 0
		for _, b := range bl {
			if b.StartLine >= f.Line && b.StartLine <= f.EndLine {
				total += b.Stmts
				if b.Count > 0 {
					covered += b.Stmts
				}
			}
		}
		switch {
		case total > 0:
			f.Coverage = float64(covered) / float64(total)
		case ok:
			f.Coverage = 1
		default:
			f.Coverage = 0
		}
		f.CRAP = CRAP(f.Complexity, f.Coverage)
	}
}
