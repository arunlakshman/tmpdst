// Package openapi embeds the OpenAPI 3.1 description of the control-plane
// HTTP API (cp/handler.go). controlplane.json is the source of truth (JSON is
// valid YAML, and the repo has no YAML dependency, decision I);
// controlplane.yaml is generated from it by cmd/specgen and a test fails if
// they differ. spec_test.go keeps the spec in sync with the Go wire types and
// replays its worked examples against cp.NewHandler.
package openapi

import _ "embed"

// JSON is the specification document.
//
//go:embed controlplane.json
var JSON []byte
