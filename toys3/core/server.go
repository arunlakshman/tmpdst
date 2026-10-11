package core

// Config is the server's static configuration.
type Config struct {
	// Dir is the data root; obj/ and tmp/ live under it and must already exist.
	Dir string
	// MaxObjectSize bounds PUT payloads (400 above it). 0 means no limit.
	MaxObjectSize int
}

// phase is the server's lifecycle stage within one incarnation.
type phase uint8

const (
	phaseBooting    phase = iota // before Started
	phaseRecovering              // listing and unlinking temp files; requests are queued
	phaseServing                 // normal operation
)

// Server is the toys3 state machine.
//
// Phase 2 fills in its state. Expected shape (all plain data, rebuilt from
// nothing on every boot):
//   - phase and the recovery bookkeeping (outstanding temp unlinks);
//   - nextIO, the IOID counter, and a table IOID → the in-flight operation
//     step waiting for it (a completion with an unknown IOID is ignored);
//   - per-key FIFO queues of ClientReq, and per-request progress (which
//     protocol step it is on, its temp path, the payload read by a COPY);
//   - requests received during recovery, in arrival order.
//
// StateHash must cover all of it, iterating maps in sorted key order.
type Server struct {
	env   Env
	cfg   Config
	phase phase
}

// New returns a server for one boot. It does nothing until Started.
func New(env Env, cfg Config) *Server {
	return &Server{env: env, cfg: cfg, phase: phaseBooting}
}

// Step consumes one event and returns the actions to execute.
func (s *Server) Step(ev Event) []Action {
	panic("toys3/core: Server.Step unimplemented (phase 2)")
}

// StateHash summarizes the server's entire state.
func (s *Server) StateHash() uint64 {
	panic("toys3/core: Server.StateHash unimplemented (phase 2)")
}
