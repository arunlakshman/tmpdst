package logtest_test

import (
	"testing"

	"github.com/manifesterio/mds/internal/controlplane/executor"
	"github.com/manifesterio/mds/internal/controlplane/executor/logtest"
)

// The suite must pass on the in-memory log, the simplest Log.
func TestSuiteOnMemoryLog(t *testing.T) {
	logtest.Run(t, func(*testing.T) executor.Log { return executor.NewMemoryLog() })
}
