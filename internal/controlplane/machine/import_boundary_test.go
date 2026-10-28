package machine_test

import (
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

// Forbidden monorepo and I/O imports for controlplane/machine (decision 56).
var forbiddenImportPrefixes = []string{
	"manifester/internal/broker",
	"manifester/internal/verify/sim",
	"manifester/internal/sys",
	"manifester/cmd",
	"manifester/internal/verify/veritas",
	"manifester/internal/wire/kwire",
	"manifester/internal/wire/format",
	"net/http",
	"github.com/cockroachdb/pebble",
	"github.com/aws/aws-sdk-go-v2/service/dynamodb",
}

func TestImportBoundary(t *testing.T) {
	cfg := &packages.Config{Mode: packages.NeedName | packages.NeedImports}
	pkgs, err := packages.Load(cfg, "manifester/internal/controlplane/machine")
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
