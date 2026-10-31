package statemachine_test

import (
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

// Forbidden monorepo and I/O imports for controlplane/machine (decision 56).
var forbiddenImportPrefixes = []string{
	"mds/internal/broker",
	"mds/internal/verify/simulator",
	"mds/internal/system",
	"mds/cmd",
	"mds/internal/verify/speccheck",
	"mds/internal/wire/kafkawire",
	"mds/internal/wire/segmentformat",
	"net/http",
	"github.com/cockroachdb/pebble",
	"github.com/aws/aws-sdk-go-v2/service/dynamodb",
}

func TestImportBoundary(t *testing.T) {
	config := &packages.Config{Mode: packages.NeedName | packages.NeedImports}
	pkgs, err := packages.Load(config, "mds/internal/controlplane/statemachine")
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
