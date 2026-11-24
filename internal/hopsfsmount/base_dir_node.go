// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package hopsfsmount

import (
	"hash/fnv"
	"os"
	"path"
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
	listing       *dirListing          // Owned by FileSystem.listings and guarded by its mutex
	listingGen    uint64               // Counts invalidations of this directory's listing; same guard
	childrenMutex sync.Mutex           // for concurrent read and updates of children/negativeCache
	dirMutex      sync.Mutex           // One read or write operation on a directory at a time
}

func newDirINode(fileSystem *FileSystem, parent parentDir, attrs Attrs) *DirINode {
	dir := &DirINode{
		baseDirNode: baseDirNode{
			FileSystem: fileSystem,
			Attrs:      attrs,
		},
	}
	if parent != nil {
		dir.Parent = parent
		dir.life.parent = parent
		dir.life.name = attrs.Name
	}
	return dir
}

func newFileINode(fileSystem *FileSystem, parent parentDir, attrs Attrs) *FileINode {
	file := &FileINode{FileSystem: fileSystem, Parent: parent, Attrs: attrs}
	file.life.parent = parent
	file.life.name = attrs.Name
	return file
}

// cacheChildInode caches a real child of parent, whose embedded base is dir,
// under name. An existing node of the right kind keeps its identity and takes
// the new attrs, unless fresh is set: Create and Mkdir pass fresh so that the
// node they return is one the kernel cannot have dropped already.
func (dir *baseDirNode) cacheChildInode(parent parentDir, operation, name string, attrs Attrs, fresh bool) fs.Node {
	node, created, numChildren := dir.cacheChildInodeLocked(parent, name, attrs, fresh)
	if created {
		logger.Debug("Children's List. Cached child ", logger.Fields{Operation: operation, Child: name, NumChildren: numChildren})
	}
	return node
}

func (dir *baseDirNode) cacheChildInodeLocked(parent parentDir, name string, attrs Attrs, fresh bool) (fs.Node, bool, int) {
	dir.lockChildrenMutex()
	defer dir.unlockChildrenMutex()

	if dir.children == nil {
		dir.children = make(map[string]fs.Node)
	}

	isDir := (attrs.Mode & os.ModeDir) != 0
	if !fresh {
		switch node := dir.children[name].(type) {
		case *DirINode:
			if isDir {
				node.Attrs = attrs
				return node, false, 0
			}
		case *FileINode:
			if !isDir {
				node.Attrs = attrs
				return node, false, 0
			}
		}
	}

	attrs.Name = name
	var node fs.Node
	if isDir {
		node = newDirINode(dir.FileSystem, parent, attrs)
	} else {
		node = newFileINode(dir.FileSystem, parent, attrs)
	}
	dir.children[name] = node
	return node, true, len(dir.children)
}

// The children-map helpers log after releasing childrenMutex: a node retiring
// on the FUSE request reader takes that lock, so a slow log sink must not
// hold it.
func (dir *baseDirNode) getChildInode(operation, name string) fs.Node {
	dir.lockChildrenMutex()
	if dir.children == nil {
		dir.children = make(map[string]fs.Node)
		dir.unlockChildrenMutex()
		return nil
	}
	node := dir.children[name]
	numChildren := len(dir.children)
	dir.unlockChildrenMutex()

	if node != nil {
		logger.Debug("Children's List. getChildInode ", logger.Fields{Operation: operation, Child: name, NumChildren: numChildren})
	} else {
		logger.Debug("Children's List. getChildInode. Not Found  ", logger.Fields{Operation: operation, Child: name, NumChildren: numChildren})
	}

	return node
}

func (dir *baseDirNode) removeChildInode(operation, name string) {
	dir.lockChildrenMutex()
	if dir.children == nil {
		dir.unlockChildrenMutex()
		return
	}
	delete(dir.children, name)
	numChildren := len(dir.children)
	dir.unlockChildrenMutex()

	logger.Debug("Children's List. removeChildInode ", logger.Fields{Operation: operation, Child: name, NumChildren: numChildren})
}

// used in rename. when an inode is moved from one dir to another
func (dir *baseDirNode) adoptChildInode(operation, name string, node fs.Node) {
	replaced, numChildren := dir.adoptChild(name, node)
	logAdoptedChild(operation, name, replaced, numChildren)
}

func (dir *baseDirNode) adoptChild(name string, node fs.Node) (replaced bool, numChildren int) {
	dir.lockChildrenMutex()
	defer dir.unlockChildrenMutex()
	if dir.children == nil {
		dir.children = make(map[string]fs.Node)
	}
	_, replaced = dir.children[name]
	numChildren = len(dir.children)
	dir.children[name] = node
	return replaced, numChildren
}

func logAdoptedChild(operation, name string, replaced bool, numChildren int) {
	if replaced {
		logger.Debug("Children's List. Adopted inode. Replaced existing node ", logger.Fields{Operation: operation, Child: name, NumChildren: numChildren})
	} else {
		logger.Debug("Children's List. Adopted inode. Added new node ", logger.Fields{Operation: operation, Child: name, NumChildren: numChildren})
	}
}

func (dir *baseDirNode) checkNegativeCache(operation, name string) bool {
	if !dir.negativeCacheHolds(name) {
		return false
	}
	logger.Debug("Negative cache hit", logger.Fields{Operation: operation, Child: name})
	return true
}

func (dir *baseDirNode) negativeCacheHolds(name string) bool {
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
		dir.FileSystem.negativeEntries.Add(-1)
		return false
	}

	return true
}

// addNegativeCacheEntry adds a name to the negative cache with TTL = CacheAttrsTimeDuration.
// Must NOT hold childrenMutex when calling this.
func (dir *baseDirNode) addNegativeCacheEntry(name string) {
	dir.lockChildrenMutex()
	defer dir.unlockChildrenMutex()

	now := dir.FileSystem.Clock.Now()
	for cached, expiry := range dir.negativeCache {
		if now.After(expiry) {
			delete(dir.negativeCache, cached)
			dir.FileSystem.negativeEntries.Add(-1)
		}
	}
	_, known := dir.negativeCache[name]
	if !known && dir.FileSystem.negativeEntries.Load() >= int64(NegativeCacheMaxEntries) {
		return
	}
	if dir.negativeCache == nil {
		dir.negativeCache = make(map[string]time.Time)
	}
	if !known {
		dir.FileSystem.negativeEntries.Add(1)
	}
	dir.negativeCache[name] = now.Add(CacheAttrsTimeDuration)
}

// removeNegativeCacheEntry removes a name from the negative cache.
// Must NOT hold childrenMutex when calling this.
func (dir *baseDirNode) removeNegativeCacheEntry(name string) {
	dir.lockChildrenMutex()
	defer dir.unlockChildrenMutex()

	if _, ok := dir.negativeCache[name]; ok {
		delete(dir.negativeCache, name)
		dir.FileSystem.negativeEntries.Add(-1)
	}
}

// releaseCaches frees what a directory that is leaving the namespace holds
// in the mount-wide caches.
func (dir *baseDirNode) releaseCaches() {
	dir.FileSystem.listings.drop(dir)
	dir.lockChildrenMutex()
	defer dir.unlockChildrenMutex()
	dir.FileSystem.negativeEntries.Add(-int64(len(dir.negativeCache)))
	dir.negativeCache = nil
}

func (dir *baseDirNode) invalidateListing(name string) {
	dir.FileSystem.listings.invalidate(dir, name)
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

// inodeStatter is the interface satisfied by directory nodes that can stat their
// children against the backend, whatever its concrete type.
type inodeStatter interface {
	Pather
	addNegativeCacheEntry(name string)
	invalidateListing(name string)
	fileSystem() *FileSystem
}

// refreshChildAttrs stats name under parent for a node that already exists
// and returns the new attributes. Unlike statInodeInHopsFS it touches no
// children map: a node that has left its parent's map must stay out of it.
func refreshChildAttrs(parent Pather, operation, name string) (Attrs, error) {
	statter, ok := parent.(inodeStatter)
	if !ok {
		return Attrs{}, syscall.ENOENT
	}
	childPath := path.Join(statter.AbsolutePath(), name)
	statCalls.Add(1)
	attrs, err := statter.fileSystem().getDFSConnector().Stat(childPath)
	if err != nil {
		logger.Info("Stat failed on backend", logger.Fields{Operation: operation, Path: childPath, Error: err})
		if err == syscall.ENOENT {
			statter.addNegativeCacheEntry(name)
			statter.invalidateListing(name)
		}
		return Attrs{}, err
	}
	return attrs, nil
}

func (dir *baseDirNode) fileSystem() *FileSystem { return dir.FileSystem }

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
