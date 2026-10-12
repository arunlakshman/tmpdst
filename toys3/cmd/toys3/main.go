// Command toys3 serves the toys3 object store over HTTP.
//
//	toys3 -dir /var/lib/toys3 -addr :8080
//
// It wires the production runtime: prepare the data directory, start the
// event loop around control.NewMachine with an OSExecutor, and serve the S3
// handler. On Trip the process exits non-zero; run it under a supervisor that
// restarts it (recovery happens on boot).
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"manifester/toys3/control"
	"manifester/toys3/prod"
)

func main() {
	dir := flag.String("dir", "./toys3-data", "data directory (obj/ and tmp/ are created inside)")
	addr := flag.String("addr", ":8080", "HTTP listen address")
	maxObj := flag.Int("max-object", 64<<20, "largest object in bytes (0 = unlimited)")
	flag.Parse()

	root, err := filepath.Abs(*dir)
	if err != nil {
		log.Fatalf("toys3: %v", err)
	}
	root = filepath.ToSlash(root)
	log.Printf("toys3: dir=%s addr=%s", root, *addr)
	if err := prod.PrepareDir(root); err != nil {
		log.Fatalf("toys3: prepare %s: %v", root, err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	loop := prod.NewLoop(control.Config{Dir: root, MaxObjectSize: *maxObj}, control.NewMachine, prod.OSExecutor{}, 1024)
	srv := &http.Server{Addr: *addr, Handler: &prod.Handler{Loop: loop, MaxBody: int64(*maxObj)}}

	go func() {
		err := loop.Run(ctx)
		if errors.Is(err, prod.ErrTripped) {
			// Fail-stop: in-memory state is untrusted. A supervisor restarts us
			// and recovery runs on boot.
			log.Fatalf("toys3: exiting: %v", err)
		}
		srv.Close()
	}()

	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("toys3: %v", err)
	}
	log.Printf("toys3: stopped")
}
