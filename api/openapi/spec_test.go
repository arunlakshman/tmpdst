package openapi_test

import (
	"os"
	"testing"
)

// JSON HTTP OpenAPI conformance was retired with the CP-DSM-7 gRPC hard cut.
// Spec files remain as historical docs until a gRPC descriptor doc lands.

func TestHTTPHandlersRemoved(t *testing.T) {
	src, err := os.ReadFile("../../controlplane/handler.go")
	if err != nil {
		t.Fatal(err)
	}
	if string(src) == "" {
		t.Fatal("empty handler.go")
	}
}
