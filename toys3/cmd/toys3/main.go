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
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"manifester/toys3/control"
	"manifester/toys3/prod"
)

// config is the parsed command line.
type config struct {
	dir    string
	addr   string
	maxObj int
}

func parseFlags(args []string) (config, error) {
	var c config
	fs := flag.NewFlagSet("toys3", flag.ContinueOnError)
	fs.StringVar(&c.dir, "dir", "./toys3-data", "data directory (obj/ and tmp/ are created inside)")
	fs.StringVar(&c.addr, "addr", ":8080", "HTTP listen address")
	fs.IntVar(&c.maxObj, "max-object", 64<<20, "largest object in bytes (0 = unlimited)")
	return c, fs.Parse(args)
}

// run serves until ctx is cancelled (nil), the server fails, or the loop
// trips (an error: in-memory state is untrusted, so the process must exit
// and be restarted by a supervisor; recovery runs on boot).
func run(ctx context.Context, args []string) error {
	c, err := parseFlags(args)
	if err != nil {
		return err
	}
	root, err := filepath.Abs(c.dir)
	if err != nil {
		return err
	}
	root = filepath.ToSlash(root)
	log.Printf("toys3: dir=%s addr=%s", root, c.addr)
	if err := prod.PrepareDir(root); err != nil {
		return fmt.Errorf("prepare %s: %w", root, err)
	}
	loop := prod.NewLoop(control.Config{Dir: root, MaxObjectSize: c.maxObj}, control.NewMachine, prod.OSExecutor{}, 1024)
	srv := &http.Server{Addr: c.addr, Handler: &prod.Handler{Loop: loop, MaxBody: int64(c.maxObj)}}
	tripped := make(chan error, 1)
	go func() {
		err := loop.Run(ctx)
		if errors.Is(err, prod.ErrTripped) {
			tripped <- err
		}
		srv.Close()
	}()
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	select {
	case err := <-tripped:
		return fmt.Errorf("exiting: %w", err)
	default:
	}
	log.Printf("toys3: stopped")
	return nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:])
	stop()
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		log.Fatalf("toys3: %v", err)
	}
}
