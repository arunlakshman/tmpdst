package replica_test

import (
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

// Forbidden I/O and adapter imports for the replica package (decision 56). It runs on a
// runtime.Clock and an engine only; the production adapters and the reference model stay out.
var forbiddenImportPrefixes = []string{
	"github.com/manifesterio/mds/controlplane/internal/system",
	"github.com/manifesterio/mds/controlplane/cmd",
	"github.com/manifesterio/mds/controlplane/internal/spec",
	"net/http",
	"github.com/cockroachdb/pebble",
	"github.com/aws/aws-sdk-go-v2/service/dynamodb",
}

func TestImportBoundary(t *testing.T) {
	config := &packages.Config{Mode: packages.NeedName | packages.NeedImports}
	pkgs, err := packages.Load(config, "github.com/manifesterio/mds/controlplane/internal/controlplane/replica")
	if err != nil {
		t.Fatalf("packages.Load: %v", err)
	}
	if len(pkgs) != 1 {
		t.Fatalf("packages.Load returned %d packages, want 1", len(pkgs))
	}
	pkg := pkgs[0]
	if len(pkg.Errors) > 0 {
		t.Fatalf("package errors: %v", pkg.Errors)
	}
	for path := range pkg.Imports {
		for _, prefix := range forbiddenImportPrefixes {
			if path == prefix || strings.HasPrefix(path, prefix+"/") {
				t.Errorf("forbidden import %q (matches %q)", path, prefix)
			}
		}
	}
}
