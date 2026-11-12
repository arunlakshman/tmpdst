// Package server is the gRPC adapter for the control-plane machine. Unary
// RPCs become Offer/fold reads on the run-loop goroutine; non-OK answers
// leave as gRPC status plus ErrorDetail (decision 57).
package server
