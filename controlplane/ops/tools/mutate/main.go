// Command mutate checks that the tests can find bugs. A mutant is one exact text
// replacement in one source file: the old text, the new text. For each mutant the
// tool copies the tree to a temp folder, applies the replacement, and runs the
// test command that the mutant names. The mutant is killed when that command
// fails. A mutant that survives is a bug that the tests cannot see.
//
// The tool fails loudly, and before it runs anything, when the old text of a
// mutant is not in its file exactly once. So a mutant cannot rot without notice
// when the code around it changes. It also runs every test command on the
// unchanged tree first, so a command that always fails cannot "kill" a mutant,
// and it refuses a mutant that does not compile.
//
// Run it from the repository root: make mutants. The file format is described
// at the top of ops/mutants.txt.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// skipDirs are the top-level folders that are not copied: git data, build
// output and agent folders.
var skipDirs = map[string]bool{".git": true, "build": true, ".claude": true}

type mutant struct {
	name, file, run string
	old, new        string
	line            int // line of the "mutant" line in the mutants file
}

func main() {
	list := flag.String("file", "ops/mutants.txt", "the mutants file")
	only := flag.String("only", "", "run only the mutant with this name")
	timeout := flag.Duration("timeout", 10*time.Minute, "time limit for one test command")
	flag.Parse()
	if err := run(*list, *only, *timeout); err != nil {
		fmt.Fprintln(os.Stderr, "mutate: FAIL:", err)
		os.Exit(1)
	}
}

func run(list, only string, timeout time.Duration) error {
	mutants, err := parse(list)
	if err != nil {
		return err
	}
	if only != "" {
		var keep []mutant
		for _, m := range mutants {
			if m.name == only {
				keep = append(keep, m)
			}
		}
		if len(keep) == 0 {
			return fmt.Errorf("no mutant named %q in %s", only, list)
		}
		mutants = keep
	}
	// Check every mutant against the source before anything runs.
	for _, m := range mutants {
		if err := check(m); err != nil {
			return err
		}
	}
	// Each distinct test command must pass on the unchanged tree.
	start := time.Now()
	passed := map[string]bool{}
	for _, m := range mutants {
		if passed[m.run] {
			continue
		}
		if out, err := runCommand(".", m.run, timeout); err != nil {
			return fmt.Errorf("the test command of mutant %q fails on the unchanged tree, so it proves nothing: %s\n%s", m.name, m.run, tail(out, 20))
		}
		passed[m.run] = true
	}
	var survived []string
	for _, m := range mutants {
		t0 := time.Now()
		killer, err := try(m, timeout)
		if err != nil {
			return err
		}
		if killer == "" {
			survived = append(survived, m.name)
			fmt.Printf("SURVIVED  %-28s %5.1fs  the tests did not notice: %s\n", m.name, time.Since(t0).Seconds(), m.run)
			continue
		}
		fmt.Printf("killed    %-28s %5.1fs  by %s\n", m.name, time.Since(t0).Seconds(), killer)
	}
	fmt.Printf("%d mutants, %d killed, %d survived, %.1fs\n", len(mutants), len(mutants)-len(survived), len(survived), time.Since(start).Seconds())
	if len(survived) > 0 {
		return fmt.Errorf("surviving mutants: %s", strings.Join(survived, ", "))
	}
	return nil
}

// check fails when the old text is not in the file exactly once.
func check(m mutant) error {
	src, err := os.ReadFile(m.file)
	if err != nil {
		return fmt.Errorf("mutant %q (line %d): %w", m.name, m.line, err)
	}
	if n := strings.Count(string(src), m.old); n != 1 {
		return fmt.Errorf("mutant %q (line %d): the old text must be in %s exactly once, but it is there %d times:\n%s", m.name, m.line, m.file, n, m.old)
	}
	return nil
}

// try runs one mutant. It returns the line that shows the kill, or "" when the
// mutant survived.
func try(m mutant, timeout time.Duration) (string, error) {
	root, err := os.MkdirTemp("", "mutate-")
	if err != nil {
		return "", err
	}
	defer func() { _ = os.RemoveAll(root) }()
	dir, err := setupWorkTree(root)
	if err != nil {
		return "", err
	}
	path := filepath.Join(dir, m.file)
	src, err := os.ReadFile(path) //nolint:gosec // G304: the path is from the mutants file, which is in git
	if err != nil {
		return "", err
	}
	out := strings.Replace(string(src), m.old, m.new, 1)
	if err := os.WriteFile(path, []byte(out), 0o600); err != nil { //nolint:gosec // G703: the path is in our own temp folder
		return "", err
	}
	text, err := runCommand(dir, m.run, timeout)
	if err == nil {
		return "", nil
	}
	if strings.Contains(text, "[build failed]") || strings.Contains(text, "[setup failed]") {
		return "", fmt.Errorf("mutant %q (line %d) does not compile, so it proves nothing:\n%s", m.name, m.line, tail(text, 20))
	}
	if _, ok := errors.AsType[*exec.ExitError](err); !ok {
		return "", fmt.Errorf("mutant %q: %w", m.name, err)
	}
	for line := range strings.SplitSeq(text, "\n") {
		if strings.HasPrefix(line, "--- FAIL") || strings.HasPrefix(line, "panic:") {
			return strings.TrimSpace(line), nil
		}
	}
	return "a failing command", nil
}

// setupWorkTree copies this module into root, in a folder with the same name.
// The go.mod file of this module has "replace" lines with paths like ../dst. So
// every sibling module of the real repo is linked next to the copy, and those
// paths still work. It returns the folder of the copy.
func setupWorkTree(root string) (string, error) {
	here, err := filepath.Abs(".")
	if err != nil {
		return "", err
	}
	dir := filepath.Join(root, filepath.Base(here))
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return "", err
	}
	if err := copyTree(".", dir); err != nil {
		return "", err
	}
	parent := filepath.Dir(here)
	entries, err := os.ReadDir(parent)
	if err != nil {
		return "", err
	}
	for _, e := range entries {
		sibling := filepath.Join(parent, e.Name())
		if sibling == here || !e.IsDir() {
			continue
		}
		if _, err := os.Stat(filepath.Join(sibling, "go.mod")); err != nil {
			continue
		}
		if err := os.Symlink(sibling, filepath.Join(root, e.Name())); err != nil {
			return "", err
		}
	}
	return dir, nil
}

// runCommand runs the command in dir without a shell and returns its output.
func runCommand(dir, command string, timeout time.Duration) (string, error) {
	words := strings.Fields(command)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, words[0], words[1:]...) //nolint:gosec // G204: the command is from the mutants file, which is in git
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		return string(out), fmt.Errorf("%s: %w", command, ctx.Err())
	}
	return string(out), err
}

// copyTree copies the files under src to dst, without the folders in skipDirs.
func copyTree(src, dst string) error {
	return filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		if d.IsDir() {
			if skipDirs[rel] {
				return filepath.SkipDir
			}
			return os.MkdirAll(filepath.Join(dst, rel), 0o750)
		}
		if !d.Type().IsRegular() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		data, err := os.ReadFile(path) //nolint:gosec // G304: the path comes from walking the tree
		if err != nil {
			return err
		}
		// The same mode, so scripts stay executable.
		return os.WriteFile(filepath.Join(dst, rel), data, info.Mode().Perm()) //nolint:gosec // G703: the path is in our own temp folder
	})
}

// parse reads the mutants file. Lines that start with # are comments, except
// inside an old or new text. A mutant is:
//
//	mutant NAME
//	file PATH
//	run COMMAND
//	@@ old
//	...the old text, exactly...
//	@@ new
//	...the new text...
//	@@ end
func parse(path string) ([]mutant, error) {
	f, err := os.Open(path) //nolint:gosec // G304: the path is the -file flag
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	var (
		mutants []mutant
		cur     *mutant
		part    string // "", "old" or "new": which text is being read
		oldText []string
		newText []string
		names   = map[string]bool{}
	)
	scanner := bufio.NewScanner(f)
	n := 0
	fail := func(format string, args ...any) error {
		return fmt.Errorf("%s:%d: %s", path, n, fmt.Sprintf(format, args...))
	}
	for scanner.Scan() {
		n++
		line := scanner.Text()
		switch {
		case line == "@@ old" && cur != nil && part == "":
			part = "old"
		case line == "@@ new" && part == "old":
			part = "new"
		case line == "@@ end" && part == "new":
			cur.old, cur.new = strings.Join(oldText, "\n"), strings.Join(newText, "\n")
			if cur.old == "" || cur.old == cur.new || cur.file == "" || cur.run == "" {
				return nil, fail("mutant %q needs file, run, a non-empty old text, and a new text that differs", cur.name)
			}
			mutants = append(mutants, *cur)
			cur, part, oldText, newText = nil, "", nil, nil
		case part == "old":
			oldText = append(oldText, line)
		case part == "new":
			newText = append(newText, line)
		case line == "" || strings.HasPrefix(line, "#"):
		case strings.HasPrefix(line, "mutant ") && cur == nil:
			name := strings.TrimSpace(strings.TrimPrefix(line, "mutant "))
			if name == "" || names[name] {
				return nil, fail("empty or repeated mutant name %q", name)
			}
			names[name] = true
			cur = &mutant{name: name, line: n}
		case strings.HasPrefix(line, "file ") && cur != nil:
			cur.file = strings.TrimSpace(strings.TrimPrefix(line, "file "))
		case strings.HasPrefix(line, "run ") && cur != nil:
			cur.run = strings.TrimSpace(strings.TrimPrefix(line, "run "))
		default:
			return nil, fail("unexpected line %q", line)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if cur != nil {
		return nil, fail("mutant %q has no @@ end", cur.name)
	}
	return mutants, nil
}

// tail is the last n lines of text.
func tail(text string, n int) string {
	lines := strings.Split(strings.TrimRight(text, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}
