// Package wireboundary is the boundary between the engine and the gRPC wire.
// It maps core ControlPlaneOperation/ControlPlaneResponse values to
// controlplane/api/v1 messages and back.
//
// Why a separate package? The engine (core, the simulator, the state machine)
// and the wire format (the generated protobuf messages) are two different
// models of the same data, and they must be able to change independently.
// Domain-driven design calls such a layer an anti-corruption layer: wire
// details stop here and never leak into the engine.
//
//   - core types are plain values. They are hashed and compared by the
//     simulator, so they must stay stable and exact.
//   - api/v1 messages are generated. Fields are pointers that can be nil, and
//     their shape follows the proto file, not the engine.
//
// So the rules of this package are:
//   - Only this package knows both models. The engine never reads a wire
//     message, and the server and client never build a core value by hand.
//   - It holds no logic. It only copies fields, so a wire change (a new field,
//     a renamed enum) is fixed here and nowhere else.
//   - Every ToCore function reads the message with its Get methods, so a nil
//     or missing part becomes a zero value and never a panic.
//
// The files follow the contexts: common.go (error and outcome codes, and the
// pieces that more than one context uses), topics.go, brokers.go and groups.go.
// Every call has a FromCore and a ToCore function, side by side. A small helper
// sits right under the call that uses it.
package wireboundary
