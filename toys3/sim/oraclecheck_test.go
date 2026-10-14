package sim

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"manifester/toys3/control"
	"manifester/toys3/veritas/veritastest"
)

// Conformance check C4 (VERITAS-PROPOSAL.md): the oracle and veritas.Store,
// two independent specs of the object store, agree. It is C1 with the
// sequential history also fed to an Oracle: after every operation the oracle
// must accept the history, and its one possible state must be the veritas's
// map, compared by the request that wrote each value. A CleanRestart is not
// a crash, so the oracle never sees one.
//
// The oracle names a value's writer by decoding it (ValueWriter), so every
// Put payload must be a self-describing value: Put sizes start at
// ValueHeader instead of 0. The other biases of C1 are unchanged.

// oracleCheck is C4 on storage from newBackend.
func oracleCheck(newBackend func() storeBackend) veritastest.Check[*storePair, storeOp] {
	c := storeCheck(control.NewMachine, newBackend, ValueHeader)
	newPair := c.New
	c.New = func() *storePair {
		p := newPair()
		p.oracle = NewOracle()
		return p
	}
	c.Invariant = func(p *storePair) error {
		if err := p.invariant(); err != nil {
			return err
		}
		return p.oracleAgrees()
	}
	return c
}

// oracleAgrees checks the history so far and compares the oracle's state
// with the veritas's.
func (p *storePair) oracleAgrees() error {
	if fails := p.oracle.CheckQuiescent(); len(fails) > 0 {
		return fmt.Errorf("oracle rejected the history: %s: %s", fails[0].Invariant, fails[0].Msg)
	}
	state, err := p.oracle.state()
	if err != nil {
		return err
	}
	var want refState
	for _, k := range p.ver.Keys() {
		v, _ := p.ver.Value(k)
		w, ok := ValueWriter(v)
		if !ok {
			return fmt.Errorf("veritas holds %s under %q, which is not a self-describing value", describeBody(v), k)
		}
		want = append(want, kv{k, w})
	}
	if !slices.Equal(state, want) {
		return fmt.Errorf("oracle state %s, veritas %s", state, want)
	}
	return nil
}

// state returns the oracle's estimate as one model state. A sequential,
// fault-free history leaves exactly one possible state; anything else is an
// error.
func (o *Oracle) state() (refState, error) {
	var st refState
	for _, g := range o.estimate {
		if len(g.states) != 1 {
			return nil, fmt.Errorf("oracle keeps %d possible states of keys %q after a sequential history", len(g.states), g.keys)
		}
		st = append(st, g.states[0]...)
	}
	slices.SortFunc(st, func(a, b kv) int { return strings.Compare(a.key, b.key) })
	for _, e := range st {
		if e.writer == unknown {
			return nil, fmt.Errorf("oracle widened %q to unknown after a sequential history", e.key)
		}
	}
	return st, nil
}

// TestC4OracleAgreesWithVeritasStore is conformance check C4. Flags as for
// TestC1ControllerMatchesVeritasStore.
func TestC4OracleAgreesWithVeritasStore(t *testing.T) {
	for _, b := range storeBackends {
		t.Run(b.name, func(t *testing.T) {
			veritastest.Run(t, oracleCheck(b.new), conformanceOptions("TestC4OracleAgreesWithVeritasStore/"+b.name+"$"))
		})
	}
}
