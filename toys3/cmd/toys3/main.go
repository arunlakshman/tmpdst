// Command toys3 serves the toys3 object store over HTTP.
//
//	toys3 -dir /var/lib/toys3 -addr :8080
//
// It wires the production runtime: prepare the data directory, start the
// event loop around core.NewMachine with an OSExecutor, and serve the S3
// handler. On Halt the process exits non-zero; run it under a supervisor that
// restarts it (recovery happens on boot).
package main

import (
	"flag"
	"log"
)

func main() {
	dir := flag.String("dir", "./toys3-data", "data directory (obj/ and tmp/ are created inside)")
	addr := flag.String("addr", ":8080", "HTTP listen address")
	flag.Parse()

	log.Printf("toys3: dir=%s addr=%s", *dir, *addr)
	// Phase 2: prod.PrepareDir(*dir); loop := prod.NewLoop(core.Config{Dir: *dir},
	// core.NewMachine, prod.OSExecutor{}, 1024); go loop.Run(ctx);
	// http.ListenAndServe(*addr, &prod.Handler{Loop: loop}).
	log.Fatal("toys3: not implemented yet (phase 2)")
}
