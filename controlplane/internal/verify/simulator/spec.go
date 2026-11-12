package simulator

import "github.com/manifesterio/mds/controlplane/spec"

// specModel is the reference model the oracle compares with. This file is the
// only place in the simulator that imports spec: to use another
// reference model, change newSpec and nothing else.
//
// The model sees the writes in the order of the log, so it needs no "maybe
// applied" state: a write whose answer was lost is simply in the log or not.
type specModel = spec.Model

func newSpec() specModel { return spec.New() }
