//go:build integration

package integrationtest

import (
	"fmt"
	"os"
	"testing"

	"github.com/manifesterio/mds/shared/testinfra"
)

// TestMain builds mdscontrolplane once and stops Podman containers from
// shared/testinfra when the package finishes. The Makefile requires Podman
// before go test; helpers never skip for a missing backend.
func TestMain(m *testing.M) {
	if err := prepareBinDir(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	code := m.Run()
	testinfra.Stop()
	_ = os.RemoveAll(binDir)
	os.Exit(code)
}
