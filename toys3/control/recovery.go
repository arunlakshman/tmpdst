package control

import "manifester/toys3/format"

// Recovery runs once per boot, before serving: list tmp/, unlink every
// leftover temp file, fsync tmp/, emit Recovered. See the package doc.

// recStep is where recovery is (only meaningful while modeRecovering).
type recStep uint8

const (
	recListing     recStep = iota // waiting for FSListDone(tmp)
	recUnlinking                  // waiting for the FSUnlinkDone of every leftover
	recFsyncingTmp                // waiting for FSFsyncDirDone(tmp)
)

func (s *Controller) onStarted() []Action {
	if s.mode != modeStartup {
		return nil // a second Started in one incarnation: ignore
	}
	s.mode = modeRecovering
	s.rec = recListing
	io := s.newIO()
	s.recIO = []IOID{io}
	return []Action{FSList{IO: io, Dir: format.TmpDir(s.cfg.Dir)}}
}

// onRecoveryDone advances recovery. Any error trips: recovery must finish
// before serving, and restarting retries it from scratch.
func (s *Controller) onRecoveryDone(io IOID, body EventBody) []Action {
	switch s.rec {
	case recListing:
		d, ok := body.(FSListDone)
		if !ok {
			return nil
		}
		if d.Err != OK {
			return s.trip("recovery: list tmp: " + d.Err.String())
		}
		s.recIO = nil
		var acts []Action
		for _, name := range d.Names {
			uio := s.newIO()
			s.recIO = append(s.recIO, uio)
			acts = append(acts, FSUnlink{IO: uio, Path: format.TmpDir(s.cfg.Dir) + "/" + name})
		}
		if len(acts) == 0 {
			return s.fsyncTmpDir()
		}
		s.rec = recUnlinking
		return acts

	case recUnlinking:
		d, ok := body.(FSUnlinkDone)
		if !ok {
			return nil
		}
		switch d.Err {
		case OK:
			s.recRemoved++
		case ErrNotFound: // already gone: fine
		default:
			return s.trip("recovery: unlink temp: " + d.Err.String())
		}
		i := indexIO(s.recIO, io)
		s.recIO = append(s.recIO[:i], s.recIO[i+1:]...)
		if len(s.recIO) > 0 {
			return nil
		}
		return s.fsyncTmpDir()

	case recFsyncingTmp:
		d, ok := body.(FSFsyncDirDone)
		if !ok {
			return nil
		}
		if d.Err != OK {
			return s.trip("recovery: fsync tmp dir: " + d.Err.String())
		}
		s.mode = modeAuto
		s.recIO = nil
		acts := []Action{Emit{Telemetry: Recovered{TempsRemoved: s.recRemoved}}}
		return append(acts, s.startPermitted()...)
	}
	return nil
}

func (s *Controller) fsyncTmpDir() []Action {
	s.rec = recFsyncingTmp
	io := s.newIO()
	s.recIO = []IOID{io}
	return []Action{FSFsyncDir{IO: io, Dir: format.TmpDir(s.cfg.Dir)}}
}

func indexIO(ios []IOID, io IOID) int {
	for i, x := range ios {
		if x == io {
			return i
		}
	}
	return -1
}
