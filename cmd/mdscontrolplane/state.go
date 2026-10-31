package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"mds/internal/controlplane/stateupdater"
	"mds/internal/controlplane/stateupdater/pebblestore"
)

// pebbleState hands the control-plane machine its Pebble store. The store is
// never trusted after a restart: it is rebuilt from the newest checkpoint and
// the log, so every open starts from an empty directory.
type pebbleState struct {
	flagDir string // -pebble-dir; empty means a temporary directory

	mu      sync.Mutex
	dir     string
	cur     *pebblestore.DB
	waiting *pebblestore.DB // opened by start, handed out by the first openStore
}

// start opens the first store now, so a bad directory fails the startup
// with an error and not inside the machine.
func (p *pebbleState) start() error {
	db, err := p.open()
	if err != nil {
		return err
	}
	p.mu.Lock()
	p.waiting = db
	p.mu.Unlock()
	return nil
}

// openStore is statemachine.Config.OpenStore.
func (p *pebbleState) openStore() (stateupdater.KeyValueStore, error) {
	p.mu.Lock()
	if db := p.waiting; db != nil {
		p.waiting = nil
		p.mu.Unlock()
		return db, nil
	}
	p.mu.Unlock()
	return p.open()
}

// open wipes the state directory and opens an empty Pebble database in it.
func (p *pebbleState) open() (*pebblestore.DB, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cur != nil {
		_ = p.cur.Close() // the database is replaced either way
		p.cur = nil
	}
	if p.dir == "" {
		p.dir = p.flagDir
		if p.dir == "" {
			d, err := os.MkdirTemp("", "mdscontrolplane-pebble-")
			if err != nil {
				return nil, err
			}
			p.dir = d
		}
	}
	if err := wipeState(p.dir); err != nil {
		return nil, err
	}
	db, err := pebblestore.Open(p.dir, pebblestore.Options{})
	if err != nil {
		return nil, err
	}
	p.cur = db
	return db, nil
}

// close closes the database and removes a temporary directory.
func (p *pebbleState) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cur != nil {
		_ = p.cur.Close() // shutting down; the state is rebuilt at the next start
		p.cur = nil
	}
	if p.flagDir == "" && p.dir != "" {
		_ = os.RemoveAll(p.dir) // best-effort cleanup of our own temporary directory
	}
}

// wipeState empties dir if it holds a Pebble database (a CURRENT file) or
// nothing; any other content is refused, so a wrong flag cannot delete data.
func wipeState(dir string) error {
	ents, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) || (err == nil && len(ents) == 0) {
		return os.MkdirAll(dir, 0o750)
	}
	if err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(dir, "CURRENT")); err != nil {
		return fmt.Errorf("-pebble-dir %s is not empty and holds no Pebble database; refusing to wipe it", dir)
	}
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	return os.MkdirAll(dir, 0o750)
}
