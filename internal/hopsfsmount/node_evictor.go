// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package hopsfsmount

import (
	"container/list"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"bazil.org/fuse/fs"
)

// The kernel keeps the entry of every file it has looked up, and with it a
// reference to the file's node, until its own memory runs short. That memory
// is charged to the container that walked the tree, so the kernel never feels
// the pressure this mount is under. The evictor asks it to let go.

// entryInvalidator is the part of fs.Server the evictor needs.
type entryInvalidator interface {
	InvalidateEntry(parent fs.Node, name string) error
}

// registeredFiles holds a mount's kernel-referenced file nodes, oldest
// registration first. mu is a leaf lock, taken under nodeLife.mu.
type registeredFiles struct {
	mu    sync.Mutex
	files list.List // of *FileINode
}

// evictionInFlight names the entry the evictor is asking the kernel to drop.
//
// The kernel resolves that request by name, and only after any operation it
// is already running on the directory. A directory bound to the name in the
// meantime would be dropped in the file's place, which detaches whatever is
// mounted below it. So the two exclude each other: an operation that would
// bind a directory to this name fails with ESTALE, which makes the kernel run
// it again, and the evictor leaves alone a name such an operation has claimed.
type evictionInFlight struct {
	mu      sync.Mutex
	parent  fs.Node
	name    string
	binding map[entryName]int // names Mkdir and Rename are giving to a directory
}

type entryName struct {
	parent fs.Node
	name   string
}

// set announces the entry about to be evicted, or with a nil parent that none
// is. It reports false, announcing nothing, when the name is claimed.
func (e *evictionInFlight) set(parent fs.Node, name string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if parent != nil && e.binding[entryName{parent, name}] > 0 {
		return false
	}
	e.parent, e.name = parent, name
	return true
}

// claim reserves a name for a directory until release, and reports false when
// that name is being evicted.
func (e *evictionInFlight) claim(parent fs.Node, name string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.parent == parent && e.name == name {
		return false
	}
	if e.binding == nil {
		e.binding = make(map[entryName]int)
	}
	e.binding[entryName{parent, name}]++
	return true
}

func (e *evictionInFlight) release(parent fs.Node, name string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	key := entryName{parent, name}
	if e.binding[key]--; e.binding[key] <= 0 {
		delete(e.binding, key)
	}
}

func (e *evictionInFlight) names(parent fs.Node, name string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.parent == parent && e.name == name
}

// refuseDirectory passes on a lookup's result unless it would hand the kernel
// a directory under the name being evicted.
func (e *evictionInFlight) refuseDirectory(parent fs.Node, name string, node fs.Node) (fs.Node, error) {
	if _, isFile := node.(*FileINode); !isFile && e.names(parent, name) {
		return nil, syscall.ESTALE
	}
	return node, nil
}

// Entries the kernel was asked to drop.
var invalidatedEntries atomic.Int64

const nodeEvictionInterval = 100 * time.Millisecond

func (r *registeredFiles) add(file *FileINode) {
	r.mu.Lock()
	defer r.mu.Unlock()
	file.evictable = r.files.PushBack(file)
}

func (r *registeredFiles) remove(file *FileINode) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if file.evictable != nil {
		r.files.Remove(file.evictable)
		file.evictable = nil
	}
}

// oldestAbove returns the oldest files in excess of limit and moves them to
// the young end, so a file the kernel will not drop is not offered again
// before every other file has been.
func (r *registeredFiles) oldestAbove(limit int) []*FileINode {
	r.mu.Lock()
	defer r.mu.Unlock()
	excess := r.files.Len() - limit
	if excess <= 0 {
		return nil
	}
	victims := make([]*FileINode, 0, excess)
	for ; excess > 0; excess-- {
		oldest := r.files.Front()
		r.files.MoveToBack(oldest)
		victims = append(victims, oldest.Value.(*FileINode))
	}
	return victims
}

// entry returns the directory and name a live node is cached under.
func (life *nodeLife) entry() (parent parentDir, name string, ok bool) {
	life.mu.Lock()
	defer life.mu.Unlock()
	return life.parent, life.name, !life.retired && life.parent != nil
}

// evictFileNodes asks the kernel to drop the entries of the oldest files above
// limit. A dropped entry comes back as a FORGET, which retires the node. Open
// files are skipped: the kernel would keep them anyway. Directories are never
// offered, because dropping a directory's entry detaches whatever is mounted
// below it and breaks the working directory of processes inside it.
func (filesystem *FileSystem) evictFileNodes(server entryInvalidator, limit int) {
	for _, file := range filesystem.fileNodes.oldestAbove(limit) {
		if file.countActiveHandles() > 0 {
			continue
		}
		parent, name, ok := file.life.entry()
		if !ok {
			continue
		}
		parentNode, ok := parent.(fs.Node)
		if !ok {
			continue
		}
		if !filesystem.evicting.set(parentNode, name) {
			continue
		}
		// No mount lock is held for the request: the kernel takes the
		// directory's lock for it, and may hold that lock while waiting for
		// one of our replies.
		if parent.hasChild(name, file) && server.InvalidateEntry(parentNode, name) == nil {
			invalidatedEntries.Add(1)
		}
		filesystem.evicting.set(nil, "")
	}
}

// StartNodeEvictor keeps the number of kernel-referenced file nodes near
// MaxCachedFiles until stop is closed. With a limit of 0 nothing is evicted.
func (filesystem *FileSystem) StartNodeEvictor(server entryInvalidator, stop <-chan struct{}) {
	if MaxCachedFiles <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(nodeEvictionInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				filesystem.evictFileNodes(server, MaxCachedFiles)
			}
		}
	}()
}
