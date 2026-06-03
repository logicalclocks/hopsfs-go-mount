// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package hopsfsmount

import (
	"hash/fnv"
	"sync"
	"syscall"
	"time"

	"bazil.org/fuse/fs"
	"hopsworks.ai/hopsfsmount/internal/hopsfsmount/logger"
)

// baseDirNode encapsulates the common cached directory state shared by real and virtual directories.
type baseDirNode struct {
	FileSystem    *FileSystem          // Pointer to the owning filesystem
	Attrs         Attrs                // Cached attributes of the directory, TODO: add TTL
	Parent        Pather               // Pointer to the parent directory (allows computing fully-qualified paths on demand)
	children      map[string]fs.Node   // Cached directory entries
	negativeCache map[string]time.Time // Caches "not found" results: name -> expiry time
	childrenMutex sync.Mutex           // for concurrent read and updates of children/negativeCache
	dirMutex      sync.Mutex           // One read or write operation on a directory at a time
}

func newDirINode(fileSystem *FileSystem, parent Pather, attrs Attrs) *DirINode {
	return &DirINode{
		baseDirNode: baseDirNode{
			FileSystem: fileSystem,
			Parent:     parent,
			Attrs:      attrs,
		},
	}
}

func (dir *baseDirNode) getChildInode(operation, name string) fs.Node {
	dir.lockChildrenMutex()
	defer dir.unlockChildrenMutex()

	if dir.children == nil {
		dir.children = make(map[string]fs.Node)
		return nil
	}

	node := dir.children[name]
	if node != nil {
		logger.Debug("Children's List. getChildInode ", logger.Fields{Operation: operation, Child: name, NumChildren: len(dir.children)})
	} else {
		logger.Debug("Children's List. getChildInode. Not Found  ", logger.Fields{Operation: operation, Child: name, NumChildren: len(dir.children)})
	}

	return node
}

func (dir *baseDirNode) removeChildInode(operation, name string) {
	dir.lockChildrenMutex()
	defer dir.unlockChildrenMutex()

	if dir.children != nil {
		delete(dir.children, name)
		logger.Debug("Children's List. removeChildInode ", logger.Fields{Operation: operation, Child: name, NumChildren: len(dir.children)})
	}
}

// used in rename. when an inode is moved from one dir to another
func (dir *baseDirNode) adoptChildInode(operation, name string, node fs.Node) {
	dir.lockChildrenMutex()
	defer dir.unlockChildrenMutex()

	if dir.children == nil {
		dir.children = make(map[string]fs.Node)
	}

	if _, ok := dir.children[name]; ok {
		logger.Debug("Children's List. Adopted inode. Replaced existing node ", logger.Fields{Operation: operation, Child: name, NumChildren: len(dir.children)})
	} else {
		logger.Debug("Children's List. Adopted inode. Added new node ", logger.Fields{Operation: operation, Child: name, NumChildren: len(dir.children)})
	}

	dir.children[name] = node
}

func (dir *baseDirNode) checkNegativeCache(operation, name string) bool {
	dir.lockChildrenMutex()
	defer dir.unlockChildrenMutex()

	if dir.negativeCache == nil {
		return false
	}

	expiry, ok := dir.negativeCache[name]
	if !ok {
		return false
	}

	if dir.FileSystem.Clock.Now().After(expiry) {
		delete(dir.negativeCache, name)
		return false
	}

	logger.Debug("Negative cache hit", logger.Fields{Operation: operation, Child: name})
	return true
}

// addNegativeCacheEntry adds a name to the negative cache with TTL = CacheAttrsTimeDuration.
// Must NOT hold childrenMutex when calling this.
func (dir *baseDirNode) addNegativeCacheEntry(name string) {
	dir.lockChildrenMutex()
	defer dir.unlockChildrenMutex()

	if dir.negativeCache == nil {
		dir.negativeCache = make(map[string]time.Time)
	}

	dir.negativeCache[name] = dir.FileSystem.Clock.Now().Add(CacheAttrsTimeDuration)
}

// removeNegativeCacheEntry removes a name from the negative cache.
// Must NOT hold childrenMutex when calling this.
func (dir *baseDirNode) removeNegativeCacheEntry(name string) {
	dir.lockChildrenMutex()
	defer dir.unlockChildrenMutex()

	if dir.negativeCache != nil {
		delete(dir.negativeCache, name)
	}
}

func (dir *baseDirNode) lockMutex() {
	dir.dirMutex.Lock()
}

func (dir *baseDirNode) unlockMutex() {
	dir.dirMutex.Unlock()
}

func (dir *baseDirNode) lockChildrenMutex() {
	dir.childrenMutex.Lock()
}

func (dir *baseDirNode) unlockChildrenMutex() {
	dir.childrenMutex.Unlock()
}

func (dir *baseDirNode) Forget() {
	dir.lockMutex()
	defer dir.unlockMutex()
	// inodes are removed on delete and rename operations.
	// this forget call is redundant and it causes problems.
	// In the mount point we identify inodes by names.
	// For example, we remove a file /some/dir/file. Before
	// the forget call is processed if the user recreates the
	// file /some/dir/file then processing forget request
	// would lead to deleting a correct inode
	// to fix this issue we have to use inode IDs
}

// inodeStatter is the interface satisfied by directory nodes that can stat their
// children against the backend. Used by parentStatInodeInHopsFS to dispatch a stat
// through a parent of unknown concrete type.
type inodeStatter interface {
	Pather
	statInodeInHopsFS(operation, name string, attrs *Attrs) (fs.Node, error)
}

// parentStatInodeInHopsFS stats name as a child of parent (real or synthetic), dispatching
// through the concrete type's statInodeInHopsFS so AbsolutePath resolves correctly.
// Returns ENOENT if parent is nil or does not implement inodeStatter.
func parentStatInodeInHopsFS(parent Pather, operation, name string, attrs *Attrs) (fs.Node, error) {
	if parent == nil {
		return nil, syscall.ENOENT
	}
	if statter, ok := parent.(inodeStatter); ok {
		return statter.statInodeInHopsFS(operation, name, attrs)
	}
	return nil, syscall.ENOENT
}

// syntheticInodeBase places all synthetic inode numbers in [2^62, 2^63), well above
// any plausible HopsFS backend inode (which grow monotonically from a small base)
// and within the positive range of a signed 64-bit integer.
const syntheticInodeBase uint64 = 1 << 62

// syntheticInode returns a deterministic inode number for key (typically a virtual
// path), placed in the synthetic range so it cannot collide with a backend inode.
// The same key always maps to the same number across mount sessions and hosts.
func syntheticInode(key string) uint64 {
	hasher := fnv.New64a()
	_, _ = hasher.Write([]byte(key))
	return syntheticInodeBase | (hasher.Sum64() & (syntheticInodeBase - 1))
}

// nodeAttrs extracts the cached Attrs from any known inode type.
// Returns (Attrs{}, false) for unrecognized node types.
func nodeAttrs(node fs.Node) (Attrs, bool) {
	switch n := node.(type) {
	case *DirINode:
		return n.Attrs, true
	case *VirtualDirINode:
		return n.Attrs, true
	case *FileINode:
		return n.Attrs, true
	default:
		return Attrs{}, false
	}
}

// nodeAttrsFresh reports whether node's cached attrs are still valid at now.
// Returns false for unknown node types and for a zero-valued Expires.
func nodeAttrsFresh(node fs.Node, now time.Time) bool {
	attrs, ok := nodeAttrs(node)
	if !ok {
		return false
	}
	return !now.After(attrs.Expires)
}
