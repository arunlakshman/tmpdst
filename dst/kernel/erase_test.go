package kernel_test

import (
	"go/ast"
	"go/types"
	"path/filepath"
	"testing"

	"golang.org/x/tools/go/packages"
)

// TestTypeErasureOnlyInEraseGo loads the kernel package with go/types and
// checks the rule of the design (section 13):
//
//   - no field, variable, parameter, result or function in the package has a
//     type that contains any (interface{});
//   - no file other than erase.go declares an unexported interface type or a
//     struct field of type func().
//
// Exported interfaces (Node, Service, Rule, View) are the API, not erasure.
// Type parameter constraints such as [K comparable] are not values.
func TestTypeErasureOnlyInEraseGo(t *testing.T) {
	cfg := &packages.Config{Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax |
		packages.NeedTypes | packages.NeedTypesInfo}
	pkgs, err := packages.Load(cfg, ".")
	if err != nil {
		t.Fatal(err)
	}
	if len(pkgs) != 1 || len(pkgs[0].Errors) > 0 {
		t.Fatalf("load: %d packages, errors %v", len(pkgs), pkgs[0].Errors)
	}
	p := pkgs[0]
	for id, obj := range p.TypesInfo.Defs {
		if obj == nil {
			continue
		}
		file := filepath.Base(p.Fset.Position(id.Pos()).Filename)
		switch o := obj.(type) {
		case *types.Func: // catches results without a name
			if hasAny(o.Type()) {
				t.Errorf("%s: %s has a signature with any: %s", p.Fset.Position(id.Pos()), o.Name(), o.Type())
			}
		case *types.Var:
			if hasAny(o.Type()) {
				t.Errorf("%s: %s has a type with any: %s", p.Fset.Position(id.Pos()), o.Name(), o.Type())
			}
			if o.IsField() && file != "erase.go" && isPlainFunc(o.Type()) {
				t.Errorf("%s: field %s of type func() outside erase.go", p.Fset.Position(id.Pos()), o.Name())
			}
		case *types.TypeName:
			if _, isParam := o.Type().(*types.TypeParam); isParam {
				continue
			}
			if _, isIface := o.Type().Underlying().(*types.Interface); isIface && !o.Exported() && file != "erase.go" {
				t.Errorf("%s: unexported interface %s outside erase.go", p.Fset.Position(id.Pos()), o.Name())
			}
		}
	}
	if !checkedEraseGo(p.Syntax, p) {
		t.Fatal("erase.go was not found in the package")
	}
}

func checkedEraseGo(files []*ast.File, p *packages.Package) bool {
	for _, f := range files {
		if filepath.Base(p.Fset.Position(f.Pos()).Filename) == "erase.go" {
			return true
		}
	}
	return false
}

// hasAny reports whether t is, or is built from, the empty interface. It does
// not look inside named types: a named type is checked where it is declared.
func hasAny(t types.Type) bool {
	switch x := types.Unalias(t).(type) {
	case *types.Interface:
		return x.Empty()
	case *types.Named:
		u, ok := x.Underlying().(*types.Interface)
		return ok && u.Empty()
	case *types.TypeParam:
		return false
	case *types.Pointer:
		return hasAny(x.Elem())
	case *types.Slice:
		return hasAny(x.Elem())
	case *types.Array:
		return hasAny(x.Elem())
	case *types.Map:
		return hasAny(x.Key()) || hasAny(x.Elem())
	case *types.Chan:
		return hasAny(x.Elem())
	case *types.Signature:
		return tupleHasAny(x.Params()) || tupleHasAny(x.Results())
	case *types.Struct:
		for f := range x.Fields() {
			if hasAny(f.Type()) {
				return true
			}
		}
	}
	return false
}

func tupleHasAny(tu *types.Tuple) bool {
	for v := range tu.Variables() {
		if hasAny(v.Type()) {
			return true
		}
	}
	return false
}

// isPlainFunc reports whether t is func() with no parameters and no results.
func isPlainFunc(t types.Type) bool {
	s, ok := types.Unalias(t).(*types.Signature)
	return ok && s.Params().Len() == 0 && s.Results().Len() == 0
}

// The checker must find what it looks for. These checks run it on small
// type values made by hand.
func TestHasAnyFindsAny(t *testing.T) {
	empty := types.NewInterfaceType(nil, nil)
	if !hasAny(empty) || !hasAny(types.NewSlice(empty)) || !hasAny(types.NewMap(types.Typ[types.String], empty)) {
		t.Fatal("hasAny missed an empty interface")
	}
	if hasAny(types.Typ[types.Int]) || hasAny(types.NewSlice(types.Typ[types.String])) {
		t.Fatal("hasAny found any where there is none")
	}
	if !isPlainFunc(types.NewSignatureType(nil, nil, nil, nil, nil, false)) {
		t.Fatal("isPlainFunc missed func()")
	}
}
