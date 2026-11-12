//go:build integration

package s3store_test

import (
	"testing"

	"github.com/manifesterio/mds/controlplane/internal/testinfra"
)

func TestMain(m *testing.M) { testinfra.Main(m) }
