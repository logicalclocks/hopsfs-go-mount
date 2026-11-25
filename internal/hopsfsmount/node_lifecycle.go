// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package hopsfsmount

import (
	"sync"
	"sync/atomic"

	"bazil.org/fuse/fs"
)

// parentDir is a directory node as seen by its children: a path prefix and
// the children map they are cached in.
type parentDir interface {
	Pather
	forgetChild(name string, node fs.Node)
	hasChild(name string, node fs.Node) bool
	invalidateListing(name string)
}

// nodeLife ties a file or directory node to the map entry that caches it, so
// the node can leave that map when the kernel drops its last reference.
//
// Lock order: FUSE server metadata mutex, then mu, then the parent's
// childrenMutex. Nothing that holds a childrenMutex may take mu.
type nodeLife struct {
	mu      sync.Mutex
	parent  parentDir // directory whose children map holds this node; nil for the mount root
	name    string    // key in that map
	retired bool
}

// Kernel-referenced nodes, for the mount's statistics.
var registeredNodes atomic.Int64

// Registrations refused because the node had retired between an entry reply
// selecting it and the server registering it.
var refusedRegistrations atomic.Int64

// register implements fs.NodeRetirer.RegisterLocked.
func (life *nodeLife) register() bool {
	life.mu.Lock()
	defer life.mu.Unlock()
	if life.retired {
		refusedRegistrations.Add(1)
		return false
	}
	registeredNodes.Add(1)
	return true
}

// retire implements fs.NodeRetirer.RetireLocked for self, the node that owns
// life.
func (life *nodeLife) retire(self fs.Node) {
	life.mu.Lock()
	defer life.mu.Unlock()
	life.retired = true
	registeredNodes.Add(-1)
	if life.parent != nil {
		life.parent.forgetChild(life.name, self)
	}
}

// detach removes self from its parent's map without retiring it: the entry
// is gone from the backend while the kernel may still hold the node.
func (life *nodeLife) detach(self fs.Node) {
	life.mu.Lock()
	defer life.mu.Unlock()
	if life.parent != nil {
		life.parent.forgetChild(life.name, self)
	}
}

// invalidateListing drops what the parent's listing says about this node,
// whose attributes changed through this mount.
func (life *nodeLife) invalidateListing() {
	life.mu.Lock()
	defer life.mu.Unlock()
	if life.parent != nil {
		life.parent.invalidateListing(life.name)
	}
}

// move re-homes self from its current map entry to newParent/newName as one
// step with respect to retirement. It reports false, and moves nothing, when
// self has retired.
func (life *nodeLife) move(self fs.Node, newParent parentAdopter, newName string) bool {
	life.mu.Lock()
	if life.retired {
		life.mu.Unlock()
		return false
	}
	if life.parent != nil {
		life.parent.forgetChild(life.name, self)
	}
	replaced, numChildren := newParent.adoptChild(newName, self)
	life.parent = newParent
	life.name = newName
	life.mu.Unlock()

	logAdoptedChild(Rename, newName, replaced, numChildren)
	return true
}

type parentAdopter interface {
	parentDir
	adoptChild(name string, node fs.Node) (replaced bool, numChildren int)
}

func (dir *baseDirNode) forgetChild(name string, node fs.Node) {
	dir.lockChildrenMutex()
	defer dir.unlockChildrenMutex()
	if cur, ok := dir.children[name]; ok && cur == node {
		delete(dir.children, name)
		if len(dir.children) == 0 {
			// An emptied map keeps the buckets of its largest size.
			dir.children = nil
		}
	}
}

func (dir *baseDirNode) hasChild(name string, node fs.Node) bool {
	dir.lockChildrenMutex()
	defer dir.unlockChildrenMutex()
	return dir.children[name] == node
}

var _ fs.NodeRetirer = (*DirINode)(nil)
var _ fs.NodeRetirer = (*FileINode)(nil)

func (dir *DirINode) RegisterLocked() bool { return dir.life.register() }

func (file *FileINode) RegisterLocked() bool {
	if !file.life.register() {
		return false
	}
	file.FileSystem.fileNodes.add(file)
	return true
}

func (file *FileINode) RetireLocked() {
	file.FileSystem.fileNodes.remove(file)
	file.life.retire(file)
}

func (dir *DirINode) RetireLocked() {
	dir.life.retire(dir)
	dir.releaseCaches()
}
