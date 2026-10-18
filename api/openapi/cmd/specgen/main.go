// Command specgen renders api/openapi/controlplane.json as controlplane.yaml.
//
//	go run ./api/openapi/cmd/specgen            # rewrite controlplane.yaml
//	go run ./api/openapi/cmd/specgen -check     # fail if it is out of date
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"manifester/api/openapi/json2yaml"
)

func main() {
	if err := run(os.Args[1:], os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "specgen:", err)
		os.Exit(1)
	}
}

func run(args []string, _ io.Writer) error {
	fs := flag.NewFlagSet("specgen", flag.ContinueOnError)
	dir := fs.String("dir", "api/openapi", "directory holding controlplane.json")
	check := fs.Bool("check", false, "only verify controlplane.yaml is up to date")
	if err := fs.Parse(args); err != nil {
		return err
	}
	src, err := os.ReadFile(filepath.Join(*dir, "controlplane.json"))
	if err != nil {
		return err
	}
	want, err := json2yaml.Convert(src)
	if err != nil {
		return err
	}
	path := filepath.Join(*dir, "controlplane.yaml")
	if !*check {
		return os.WriteFile(path, want, 0o644)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if !bytes.Equal(got, want) {
		return fmt.Errorf("%s is out of date: run go run ./api/openapi/cmd/specgen", path)
	}
	return nil
}
