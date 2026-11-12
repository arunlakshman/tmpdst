// Package wire is the only place that converts the contract value types to and
// from the generated api messages. Nearly every function is generated
// (wire_gen.go, by the protoc plugin ops/tools/protoc-gen-mdswire from the .proto
// files); none of the conversions of a message is written by hand.
// A ToCore function reads the message with its Get methods, so a nil message or a
// missing part gives a zero value and never a panic.
// Two small pieces are by hand, because the names of the two sides differ: the
// table of error codes (errcode.go), and the outcome map and the gRPC error of a
// failed call (rpcerror.go).
package wire
