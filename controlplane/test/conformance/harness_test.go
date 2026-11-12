// Package conformance checks that the control plane answers like the model in
// internal/spec. It follows Bornholt et al., "Using Lightweight Formal Methods
// to Validate a Key-Value Storage Node in Amazon S3" (SOSP 2021), sections 4
// and 5: rapid draws a list of operations (ops_test.go), each one goes to the
// model and to the target, and the answers must be equal. After a restart the
// whole state is compared and every earlier commit is sent again.
package conformance

import (
	"context"
	"flag"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/controlplane/internal/spec"
)

// Target is a system under test. Exec is needed. Each other function is an
// operation of the target. A nil function means the target has no such
// operation, and it is never drawn.
type Target struct {
	// Exec sends one operation and returns the answer. The error is for a
	// call that did not get an answer.
	Exec func(context.Context, runtime.ControlPlaneOperation) (runtime.ControlPlaneDone, error)
	// Reboot is a clean restart.
	Reboot func()
	// DirtyReboot is a crash. keep says what the crash leaves of the
	// checkpoints: 0 all, 1 all but the newest, 2 none. A target may keep more
	// when it cannot rebuild its state without them.
	DirtyReboot func(keep int)
	// Checkpoint waits until a checkpoint covers everything written so far.
	Checkpoint func()
	// Inject makes the next storage call of one kind fail (see failAppend).
	// -1 stops all failures. A target with Inject gets only operations that are
	// safe to send twice.
	Inject func(kind int)
	// Lacks names the operations (the name of an op in ops_test.go) that the
	// target cannot send, for example because its API has no such call.
	Lacks []string
}

// Kinds of injected failure, the argument of Target.Inject.
const (
	failAppend     = iota // an append fails and nothing is stored
	failAppendLate        // an append is stored, then it fails: the answer is lost
	failRead              // a log read fails
	failPut               // a checkpoint upload fails
	failureKinds          // the number of kinds
)

// callTimeout bounds one call. A call that takes longer is a failure.
const callTimeout = 3 * time.Second

// maxExtraRounds bounds how many more rounds of checks Run makes when an
// answer that the operations aim at was not seen yet. Each round judges the
// full property, so no check gets weaker.
const maxExtraRounds = 20

// world is the state of one check.
type world struct {
	t      *rapid.T
	target Target
	model  *spec.ControlPlane
	step   int
	seen   map[string]int // answers by operation and code, over all checks

	commits   []contract.CommitSegment // every fresh commit, to send again
	hasFailed bool                     // a failure was injected (see ask)
	property  string                   // set while a restart is checked
}

// Run checks the conformance property on the targets that newTarget makes.
// Then it fails if an answer that an operation aims at (op.want) was never
// seen: that is the evidence that the draws reach every rule.
func Run(t testing.TB, newTarget func(*rapid.T) Target) {
	t.Helper()
	seen := map[string]int{}
	var want []string
	check := func() {
		rapid.Check(t, func(rt *rapid.T) {
			w := &world{t: rt, target: newTarget(rt), model: spec.New(), seen: seen}
			list := ops(w.target)
			want = want[:0]
			for _, o := range list {
				want = append(want, o.want...)
			}
			rt.Repeat(map[string]func(*rapid.T){"step": func(*rapid.T) {
				// The index is drawn here, not by Repeat, because the shrinker
				// moves it toward 0, and the simple operations come first.
				o := list[rapid.IntRange(0, len(list)-1).Draw(rt, "op")]
				rt.Logf("step %d: %s", w.step, o.name)
				seen[o.name]++
				o.run(w)
				w.step++
			}})
		})
	}
	check()
	// A fixed seed runs the same checks in every round, so it gets no more
	// rounds and its answers are not counted.
	asked := userFlags()
	if n, _ := strconv.Atoi(flag.Lookup("rapid.checks").Value.String()); n < 10 || asked["rapid.seed"] || asked["rapid.failfile"] {
		t.Logf("answers not counted: a fixed seed or too few checks")
		return
	}
	for range maxExtraRounds {
		if t.Failed() || !slices.ContainsFunc(want, func(k string) bool { return seen[k] == 0 }) {
			break
		}
		check()
	}
	if t.Failed() {
		return
	}
	for _, k := range want {
		if seen[k] == 0 {
			t.Errorf("no %q in any check: the draws do not reach it", k)
		}
	}
}

// userFlags are the flags that the person who runs the tests set. It is read
// once, before fewerChecks sets any (a flag.Set counts as set).
var userFlags = sync.OnceValue(func() map[string]bool {
	asked := map[string]bool{}
	flag.Visit(func(f *flag.Flag) { asked[f.Name] = true })
	return asked
})

// fewerChecks makes a slow test run fewer checks and steps than rapid's 100
// and 30, unless the person who runs it set -rapid.checks or -rapid.steps.
func fewerChecks(t *testing.T, checks, steps int) {
	t.Helper()
	asked := userFlags()
	for name, value := range map[string]int{"rapid.checks": checks, "rapid.steps": steps} {
		if asked[name] {
			continue
		}
		old := flag.Lookup(name).Value.String()
		if err := flag.Set(name, strconv.Itoa(value)); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = flag.Set(name, old) })
	}
}

// transient is an answer that says "try again": the target could not answer,
// and it said nothing wrong.
func transient(d runtime.ControlPlaneDone, err error) bool {
	return err != nil || d.Err == runtime.ErrUnavailable || d.Err == runtime.ErrTimeout || d.Err == runtime.ErrIO
}

func (w *world) call(op runtime.ControlPlaneOperation) (runtime.ControlPlaneDone, error) {
	ctx, cancel := context.WithTimeout(w.t.Context(), callTimeout)
	defer cancel()
	return w.target.Exec(ctx, op)
}

// ask sends op to the target. This is the "has failed" rule of the paper: once
// a failure was injected, an answer may be an error. Then the failures are
// stopped and op is sent again until a real answer comes, and that answer is
// compared with the model. An error is allowed, a wrong answer never is. The
// paper also allows "no data" after a failure. Our log does not lose data, so
// this rule is stricter.
func (w *world) ask(op runtime.ControlPlaneOperation) runtime.ControlPlaneDone {
	d, err := w.call(op)
	for try := 0; transient(d, err) && w.hasFailed && try < 300; try++ {
		w.target.Inject(-1)
		time.Sleep(10 * time.Millisecond)
		d, err = w.call(op)
	}
	if err != nil {
		w.t.Fatalf("step %d: %s: the target returned an error: %v", w.step, describe(op), err)
	}
	return d
}

// do sends op to the model and to the target, compares the answers and
// checks the invariants. label names the answer in the counts.
func (w *world) do(label string, op runtime.ControlPlaneOperation) {
	w.t.Helper()
	want := w.model.Apply(op)
	got := w.ask(op)
	w.seen[label+"/"+want.Err.String()]++
	if !sameAnswer(got, want) {
		w.t.Fatalf("step %d%s: %s\n  model:  %s\n  target: %s", w.step, w.property, describe(op), show(want), show(got))
	}
	if msg := badAnswer(got); msg != "" {
		w.t.Fatalf("step %d: %s\n  invariant broken: %s\n  target: %s", w.step, describe(op), msg, show(got))
	}
}

// afterRestart checks the property after a restart: forward progress after a
// clean one, persistence after a crash. Both say that every write answered
// before the restart is there after it, and no other write is. So the whole
// state must equal the model, and every earlier commit must still be a replay
// with the same ranges.
func (w *world) afterRestart(property string) {
	w.property = " (" + property + " after a restart)"
	defer func() { w.property = "" }()
	w.do("sweep.topics", contract.DescribeTopics{})
	for _, topic := range topicNames {
		for p := range int32(3) {
			w.do("sweep.find", contract.FindSegments{TopicPartition: contract.NewTopicPartition(topic, p)})
		}
	}
	for _, c := range w.commits {
		w.do("sweep.replay", c)
	}
	w.do("sweep.groups", contract.ListGroups{})
	for _, g := range groupNames {
		w.do("sweep.describeGroup", contract.DescribeGroup{Group: g})
		w.do("sweep.offsets", contract.OffsetFetch{Group: g})
	}
}

// sameAnswer compares the code and the response. A nil slice and an empty slice
// print the same, and that difference is not a bug.
func sameAnswer(a, b runtime.ControlPlaneDone) bool {
	return a.Err == b.Err && fmt.Sprintf("%+v", a.Resp) == fmt.Sprintf("%+v", b.Resp)
}

// badAnswer checks the invariants of one answer alone, without the model: the
// runs of a partition follow each other with no gap, the end offset is not
// before the end of the last run, and topics are sorted with no repeats.
func badAnswer(d runtime.ControlPlaneDone) string {
	switch r := d.Resp.(type) {
	case contract.FindSegmentsResp:
		for i, x := range r.Ranges {
			if x.Count <= 0 || (i > 0 && x.Base != r.Ranges[i-1].Base+contract.Offset(r.Ranges[i-1].Count)) {
				return fmt.Sprintf("run %d is not next to the run before it: %+v", i, r.Ranges)
			}
		}
		if n := len(r.Ranges); n > 0 && r.End < r.Ranges[n-1].Base+contract.Offset(r.Ranges[n-1].Count) {
			return fmt.Sprintf("end offset %d is before the end of the last run", r.End)
		}
	case contract.DescribeTopicsResp:
		for i := 1; i < len(r.Topics); i++ {
			if strings.Compare(r.Topics[i-1].Name, r.Topics[i].Name) >= 0 {
				return fmt.Sprintf("topics are not sorted: %+v", r.Topics)
			}
		}
	}
	return ""
}

func show(d runtime.ControlPlaneDone) string { return fmt.Sprintf("err=%v resp=%+v", d.Err, d.Resp) }

func describe(op runtime.ControlPlaneOperation) string { return fmt.Sprintf("%T%+v", op, op) }
