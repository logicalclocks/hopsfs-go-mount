// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package hopsfsmount

import (
	"sort"
	"sync"
	"time"
)

// inFlightWrites tracks the FUSE handlers that change a staged file or HopsFS
// while they run, so a signal-triggered exit can wait for them to return.
type inFlightWrites struct {
	lock  sync.Mutex
	next  uint64
	ops   map[uint64]string
	ended chan struct{} // one buffered slot, signalled when an operation ends
}

// trackWrite records one handler as running until the returned func is called.
// The path is computed by the caller before it takes any lock, so a later
// report never needs a node lock.
func (filesystem *FileSystem) trackWrite(operation string, path string) func() {
	w := &filesystem.writes
	w.lock.Lock()
	if w.ops == nil {
		w.ops = make(map[uint64]string)
		w.ended = make(chan struct{}, 1)
	}
	w.next++
	id := w.next
	w.ops[id] = operation + " " + path
	ended := w.ended
	w.lock.Unlock()

	return func() {
		w.lock.Lock()
		delete(w.ops, id)
		w.lock.Unlock()
		select {
		case ended <- struct{}{}:
		default:
		}
	}
}

// PendingWrites returns the tracked handlers still running, sorted.
func (filesystem *FileSystem) PendingWrites() []string {
	w := &filesystem.writes
	w.lock.Lock()
	defer w.lock.Unlock()
	pending := make([]string, 0, len(w.ops))
	for _, op := range w.ops {
		pending = append(pending, op)
	}
	sort.Strings(pending)
	return pending
}

// WaitForWrites waits up to timeout for every tracked handler to return.
// It returns the handlers still running when it gives up, or nil.
func (filesystem *FileSystem) WaitForWrites(timeout time.Duration) []string {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		w := &filesystem.writes
		w.lock.Lock()
		if len(w.ops) == 0 {
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
