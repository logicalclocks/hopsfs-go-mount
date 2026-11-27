// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package hopsfsmount

import (
	"sort"
	"sync"
	"time"
)

// inFlightWrites tracks what a signal-triggered exit must wait for: the FUSE
// handlers that change a staged file or HopsFS while they run, and the files
// whose data is not in HopsFS yet (dirty, or created with a writer still
// open). A file stays owed between its last Write and the FLUSH that uploads
// it, while no handler runs, e.g. while the kernel flushes the fds of a
// killed process one after another.
type inFlightWrites struct {
	lock  sync.Mutex
	next  uint64
	ops   map[uint64]string
	owed  map[*FileINode]struct{}
	ended chan struct{} // one buffered slot, signalled when an operation ends or a file stops being owed
}

func (w *inFlightWrites) initLocked() {
	if w.ops == nil {
		w.ops = make(map[uint64]string)
		w.owed = make(map[*FileINode]struct{})
		w.ended = make(chan struct{}, 1)
	}
}

func (w *inFlightWrites) signal() {
	select {
	case w.ended <- struct{}{}:
	default:
	}
}

// trackWrite records one handler as running until the returned func is called.
// The path is computed by the caller before it takes any lock, so a later
// report never needs a node lock.
func (filesystem *FileSystem) trackWrite(operation string, path string) func() {
	w := &filesystem.writes
	w.lock.Lock()
	w.initLocked()
	w.next++
	id := w.next
	w.ops[id] = operation + " " + path
	w.lock.Unlock()

	return func() {
		w.lock.Lock()
		delete(w.ops, id)
		w.signal()
		w.lock.Unlock()
	}
}

// setOwed records whether file has data that is not in HopsFS yet.
func (filesystem *FileSystem) setOwed(file *FileINode, owed bool) {
	if filesystem == nil {
		return
	}
	w := &filesystem.writes
	w.lock.Lock()
	defer w.lock.Unlock()
	w.initLocked()
	if owed {
		w.owed[file] = struct{}{}
	} else if _, ok := w.owed[file]; ok {
		delete(w.owed, file)
		w.signal()
	}
}

// PendingWrites returns the running handlers and the files not yet in HopsFS, sorted.
func (filesystem *FileSystem) PendingWrites() []string {
	w := &filesystem.writes
	w.lock.Lock()
	defer w.lock.Unlock()
	pending := make([]string, 0, len(w.ops)+len(w.owed))
	for _, op := range w.ops {
		pending = append(pending, op)
	}
	for file := range w.owed {
		pending = append(pending, "unflushed "+file.AbsolutePath())
	}
	sort.Strings(pending)
	return pending
}

// WaitForWrites waits up to timeout until no tracked handler runs and every
// file's data is in HopsFS. It returns what is still pending when it gives up, or nil.
func (filesystem *FileSystem) WaitForWrites(timeout time.Duration) []string {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		w := &filesystem.writes
		w.lock.Lock()
		if len(w.ops) == 0 && len(w.owed) == 0 {
			w.lock.Unlock()
			return nil
		}
		ended := w.ended
		w.lock.Unlock()

		select {
		case <-ended:
		case <-deadline.C:
			return filesystem.PendingWrites()
		}
	}
}
