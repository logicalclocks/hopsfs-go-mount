// Copyright (c) Microsoft. All rights reserved.
// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package hopsfsmount

import (
	"os"
	"path"
	"sync"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
	"hopsworks.ai/hopsfsmount/internal/hopsfsmount/logger"
)

type inodeStatter interface {
	Pather
	statInodeInHopsFS(operation, name string, attrs *Attrs) (fs.Node, error)
}

func parentStatInodeInHopsFS(parent Pather, operation, name string, attrs *Attrs) (fs.Node, error) {
	if parent == nil {
		return nil, syscall.ENOENT
	}
	if statter, ok := parent.(inodeStatter); ok {
		return statter.statInodeInHopsFS(operation, name, attrs)
	}
	return nil, syscall.ENOENT
}

// baseDirNode encapsulates the common cached directory state shared by real and virtual directories.
type baseDirNode struct {
	FileSystem    *FileSystem        // Pointer to the owning filesystem
	Attrs         Attrs              // Cached attributes of the directory, TODO: add TTL
	Parent        Pather             // Pointer to the parent directory (allows computing fully-qualified paths on demand)
	children      map[string]fs.Node // Cached directory entries
	negativeCache map[string]time.Time
	childrenMutex sync.Mutex
	dirMutex      sync.Mutex
}

// Returns absolute path of the dir in HDFS namespace.
func (dir *baseDirNode) AbsolutePath() string {
	if dir.Parent == nil {
		return dir.FileSystem.SrcDir
	}
	return path.Join(dir.Parent.AbsolutePath(), dir.Attrs.Name)
}

// Returns absolute path of the child item of this directory.
func (dir *baseDirNode) AbsolutePathForChild(name string) string {
	return path.Join(dir.AbsolutePath(), name)
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
		logger.Debug("Children's List. getChildInode ", logger.Fields{Operation: operation, Parent: dir.AbsolutePath(), Child: name, NumChildren: len(dir.children)})
	} else {
		logger.Debug("Children's List. getChildInode. Not Found  ", logger.Fields{Operation: operation, Parent: dir.AbsolutePath(), Child: name, NumChildren: len(dir.children)})
	}

	return node
}

func (dir *baseDirNode) addOrUpdateChildInodeAttrs(operation, name string, attrs Attrs) fs.Node {
	dir.lockChildrenMutex()
	defer dir.unlockChildrenMutex()

	if dir.children == nil {
		dir.children = make(map[string]fs.Node)
	}

	shouldBeDir := (attrs.Mode & os.ModeDir) != 0
	if node, ok := dir.children[name]; ok {
		if shouldBeDir {
			if dnode, ok := (node).(*DirINode); ok {
				dnode.Attrs = attrs
				logger.Debug("Children's List. addOrUpdateChildInodeAttrs. Update ", logger.Fields{Operation: operation, Parent: dir.AbsolutePath(), Child: name, NumChildren: len(dir.children)})
				return node
			}
			node = &DirINode{FileSystem: dir.FileSystem, Parent: dir, Attrs: attrs}
		} else {
			if fnode, ok := (node).(*FileINode); ok {
				fnode.Attrs = attrs
				logger.Debug("Children's List. addOrUpdateChildInodeAttrs. Update ", logger.Fields{Operation: operation, Parent: dir.AbsolutePath(), Child: name, NumChildren: len(dir.children)})
				return node
			}
			node = &FileINode{FileSystem: dir.FileSystem, Parent: dir, Attrs: attrs}
		}

		dir.children[name] = node
		logger.Debug("Children's List. addOrUpdateChildInodeAttrs. Replace ", logger.Fields{Operation: operation, Parent: dir.AbsolutePath(), Child: name, NumChildren: len(dir.children)})
		return node
	} else {
		var node fs.Node
		if shouldBeDir {
			node = &DirINode{FileSystem: dir.FileSystem, Parent: dir, Attrs: attrs}
		} else {
			node = &FileINode{FileSystem: dir.FileSystem, Parent: dir, Attrs: attrs}
		}
		dir.children[name] = node
		logger.Debug("Children's List. addOrUpdateChildInodeAttrs. Add ", logger.Fields{Operation: operation, Parent: dir.AbsolutePath(), Child: name, NumChildren: len(dir.children)})
		return node
	}
}

func (dir *baseDirNode) removeChildInode(operation, name string) {
	dir.lockChildrenMutex()
	defer dir.unlockChildrenMutex()

	if dir.children != nil {
		delete(dir.children, name)
		logger.Debug("Children's List. removeChildInode ", logger.Fields{Operation: operation, Parent: dir.AbsolutePath(), Child: name, NumChildren: len(dir.children)})
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
		logger.Debug("Children's List. Adopted inode. Replaced existing node ", logger.Fields{Operation: operation, Parent: dir.AbsolutePath(), Child: name, NumChildren: len(dir.children)})
	} else {
		logger.Debug("Children's List. Adopted inode. Added new node ", logger.Fields{Operation: operation, Parent: dir.AbsolutePath(), Child: name, NumChildren: len(dir.children)})
	}

	dir.children[name] = node
}

// Performs Stat() query on the backend.
func (dir *baseDirNode) statInodeInHopsFS(operation, name string, attrs *Attrs) (fs.Node, error) {
	a, err := dir.FileSystem.getDFSConnector().Stat(path.Join(dir.AbsolutePath(), name))
	if err != nil {
		logger.Info("Stat failed on backend", logger.Fields{Operation: operation, Path: path.Join(dir.AbsolutePath(), name), Error: err})
		dir.removeChildInode(operation, name)
		if err == syscall.ENOENT {
			dir.addNegativeCacheEntry(name)
		}
		return nil, err
	}
	*attrs = a

	inode := dir.addOrUpdateChildInodeAttrs(operation, name, *attrs)
	logger.Info("Stat successful on backend", logger.Fields{Operation: operation, Path: path.Join(dir.AbsolutePath(), name), FileSize: attrs.Size,
		IsDir: attrs.Mode.IsDir(), IsRegular: attrs.Mode.IsRegular()})
	return inode, nil
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

	logger.Debug("Negative cache hit", logger.Fields{Operation: operation, Parent: dir.AbsolutePath(), Child: name})
	return true
}

func (dir *baseDirNode) addNegativeCacheEntry(name string) {
	dir.lockChildrenMutex()
	defer dir.unlockChildrenMutex()

	if dir.negativeCache == nil {
		dir.negativeCache = make(map[string]time.Time)
	}

	dir.negativeCache[name] = dir.FileSystem.Clock.Now().Add(CacheAttrsTimeDuration)
}

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

func (dir *baseDirNode) Symlink(ctx context.Context, req *fuse.SymlinkRequest) (fs.Node, error) {
	logger.Error("Unsupported Symlink operation.", logger.Fields{Operation: Symlink, Path: dir.AbsolutePath()})
	return nil, syscall.ENOTSUP
}

func (dir *baseDirNode) Readlink(ctx context.Context, req *fuse.ReadlinkRequest) (string, error) {
	logger.Error("Unsupported Readlink operation.", logger.Fields{Operation: ReadLink, Path: dir.AbsolutePath()})
	return "", syscall.ENOTSUP
}

func (dir *baseDirNode) Link(ctx context.Context, req *fuse.LinkRequest, old fs.Node) (fs.Node, error) {
	logger.Error("Unsupported Link operation.", logger.Fields{Operation: Link, Path: dir.AbsolutePath()})
	return nil, syscall.ENOTSUP
}

// https://libfuse.github.io/doxygen/structfuse__operations.html#abaa2a0bdc9b9955a399ea6973f6f4927
// Synchronize directory contents
// All dir operations are first performed on the backend. So no-op
func (dir *baseDirNode) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	logger.Info("Fsync called on Dir ", logger.Fields{Operation: Fsync, Path: dir.AbsolutePath()})
	return nil
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

func nodeAttrsFresh(node fs.Node, now time.Time) bool {
	attrs, ok := nodeAttrs(node)
	if !ok {
		return false
	}
	return !now.After(attrs.Expires)
}
