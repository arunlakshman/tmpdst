package core

import (
	"testing"

	"github.com/manifesterio/mds/dst/machine"
)

type recordingMachine struct{ got []Event }

func (m *recordingMachine) Step(e Event) []Action { m.got = append(m.got, e); return nil }
func (m *recordingMachine) StateHash() uint64     { return 11 }

// The dst contract passes the time as an argument. OnContract puts it in the Event.
func TestOnContractPutsTheTimeInTheEvent(t *testing.T) {
	rec := &recordingMachine{}
	m := OnContract(rec)
	m.Step(machine.Now{Mono: 5, Wall: 7}, NodeStarted{})
	if len(rec.got) != 1 || rec.got[0].At != 5 || rec.got[0].Wall != 7 {
		t.Fatalf("got %+v", rec.got)
	}
	if _, ok := rec.got[0].Body.(NodeStarted); !ok {
		t.Fatalf("body = %T", rec.got[0].Body)
	}
	if m.StateHash() != 11 {
		t.Fatalf("StateHash = %d", m.StateHash())
	}
}

// A factory that gives no machine stays nil, so the caller can report it.
func TestContractFactoryKeepsNil(t *testing.T) {
	f := ContractFactory(func(Environment, machine.NodeInstance) Machine { return nil })
	if m := f(nil, machine.NodeInstance{}); m != nil {
		t.Fatalf("a nil machine became %v", m)
	}
	g := ContractFactory(func(Environment, machine.NodeInstance) Machine { return &recordingMachine{} })
	if g(nil, machine.NodeInstance{}) == nil {
		t.Fatal("a machine became nil")
	}
}
