// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package hopsfsmount

import (
	"fmt"
	"os"
	"path"
	"syscall"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
	"hopsworks.ai/hopsfsmount/internal/hopsfsmount/logger"
)

// VirtualDirINode represents a synthetic directory inside a configured virtual root.
//
// The synthetic tree (every *VirtualDirINode) is a curated view of the backend:
// only paths listed in the configuration are visible, regardless of what the backend
// actually contains. The synthetic node enforces three properties:
//
//   - selective visibility: ReadDirAll lists only configured next-segment names
//   - selective lookup:    Lookup of unconfigured names returns ENOENT
//   - selective mutation:  Mkdir/Create/Remove/Rename/Rename2/Setattr return EPERM
//
// These properties end at a configured leaf path. Once Lookup reaches a leaf,
// it crosses into the real tree and returns a *DirINode or *FileINode backed by
// the corresponding HopsFS path. Operations inside the leaf use normal HopsFS
// semantics, subject only to backend permissions.
//
// The Parent chain crosses the boundary transparently because DirINode.Parent
// has interface type Pather (see path.go): a real *DirINode reached through a
// synthetic ancestor carries the *VirtualDirINode as its Parent, and AbsolutePath()
// walks up through it without caring which kind of node it traverses.
type VirtualDirINode struct {
	baseDirNode
	config  VirtualDirectoryConfig // The configured virtual directory this node belongs to
	relPath string                 // Position within the virtual directory's tree ("" = synthetic root, e.g. "projA" for branches)
}

// Verify that *VirtualDirINode implements necesary FUSE interfaces
var _ fs.Node = (*VirtualDirINode)(nil)
var _ fs.HandleReadDirAller = (*VirtualDirINode)(nil)
var _ fs.NodeStringLookuper = (*VirtualDirINode)(nil)
var _ fs.NodeMkdirer = (*VirtualDirINode)(nil)
var _ fs.NodeRemover = (*VirtualDirINode)(nil)
var _ fs.NodeRenamer = (*VirtualDirINode)(nil)
var _ fs.NodeForgetter = (*VirtualDirINode)(nil)
var _ fs.NodeSymlinker = (*VirtualDirINode)(nil)
var _ fs.NodeReadlinker = (*VirtualDirINode)(nil)
var _ fs.NodeLinker = (*VirtualDirINode)(nil)
var _ fs.NodeCreater = (*VirtualDirINode)(nil)
var _ fs.NodeFsyncer = (*VirtualDirINode)(nil)

// Returns absolute path of the virtual directory in the backend namespace.
func (dir *VirtualDirINode) AbsolutePath() string {
	return dir.config.Path(dir.relPath)
}

// Returns absolute path of the child item of this virtual directory.
func (dir *VirtualDirINode) AbsolutePathForChild(name string) string {
	return path.Join(dir.AbsolutePath(), name)
}

// Responds on FUSE request to get directory attributes.
func (dir *VirtualDirINode) Attr(ctx context.Context, a *fuse.Attr) error {
	dir.lockMutex()
	defer dir.unlockMutex()

	if dir.FileSystem.Clock.Now().After(dir.Attrs.Expires) {
		if err := dir.refreshSyntheticAttrs(); err != nil {
			return err
		}
	} else {
		logger.Info("Stat successful. Returning from Cache ", logger.Fields{Operation: GetattrDir, Path: path.Join(dir.AbsolutePath()), FileSize: dir.Attrs.Size,
			IsDir: dir.Attrs.Mode.IsDir(), IsRegular: dir.Attrs.Mode.IsRegular()})
	}
	return dir.Attrs.ConvertAttrToFuse(a)
}

// Responds on FUSE request to read directory.
func (dir *VirtualDirINode) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	dir.lockMutex()
	defer dir.unlockMutex()

	childNames := dir.config.childNames(dir.relPath)
	entries := make([]fuse.Dirent, 0, len(childNames))
	now := dir.FileSystem.Clock.Now()
	for _, childName := range childNames {
		childRelPath := path.Join(dir.relPath, childName)
		if dir.config.leafExists(childRelPath) {
			if node := dir.getChildInode(ReadDir, childName); node != nil && nodeAttrsFresh(node, now) {
				attrs, ok := nodeAttrs(node)
				if !ok {
					return nil, fmt.Errorf("unexpected cached node type for %s", childName)
				}
				entries = append(entries, fuse.Dirent{Inode: attrs.Inode, Name: childName, Type: attrs.FuseNodeType()})
				continue
			}

			entries = append(entries, fuse.Dirent{Inode: syntheticInode(path.Join("/", dir.config.Name, childRelPath)), Name: childName, Type: fuse.DT_Unknown})
			continue
		}

		child, err := dir.ensureSyntheticDirectoryChild(ReadDir, childRelPath, childName, dir.config.Path(childRelPath))
		if err != nil {
			entries = append(entries, fuse.Dirent{Inode: syntheticInode(path.Join("/", dir.config.Name, childRelPath)), Name: childName, Type: fuse.DT_Unknown})
			continue
		}
		entries = append(entries, fuse.Dirent{Inode: child.Attrs.Inode, Name: childName, Type: fuse.DT_Dir})
	}
	return entries, nil
}

// Responds on FUSE request to lookup the directory.
func (dir *VirtualDirINode) Lookup(ctx context.Context, name string) (fs.Node, error) {
	dir.lockMutex()
	defer dir.unlockMutex()

	return dir.lookupVirtualDirectoryChild(Lookup, name)
}

// All mutation operations on a synthetic directory return EPERM. The synthetic tree is
// structural — to write data, navigate to a real leaf path returned by Lookup and operate
// there. The methods below implement the FUSE Node*er interfaces required by bazil.org/fuse.

func (dir *VirtualDirINode) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	return nil, dir.rejectMutation(Mkdir, dir.AbsolutePathForChild(req.Name))
}

func (dir *VirtualDirINode) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	return nil, nil, dir.rejectMutation(Create, dir.AbsolutePathForChild(req.Name))
}

func (dir *VirtualDirINode) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	return dir.rejectMutation(Remove, dir.AbsolutePathForChild(req.Name))
}

func (dir *VirtualDirINode) Rename(ctx context.Context, req *fuse.RenameRequest, dstParentDir fs.Node) error {
	return dir.rejectMutation(Rename, dir.AbsolutePathForChild(req.OldName))
}

func (dir *VirtualDirINode) Rename2(ctx context.Context, req *fuse.Rename2Request, dstParentDir fs.Node) error {
	return dir.rejectMutation(Rename2, dir.AbsolutePathForChild(req.OldName))
}

func (dir *VirtualDirINode) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	return dir.rejectMutation(Chmod, dir.AbsolutePath())
}

// Symlink/Readlink/Link/Fsync are not supported on synthetic directories. They live on the
// outer type rather than the base so that the log lines carry the correct AbsolutePath
// (the *VirtualDirINode override that resolves to the backend path via config).

func (dir *VirtualDirINode) Symlink(ctx context.Context, req *fuse.SymlinkRequest) (fs.Node, error) {
	logger.Error("Unsupported Symlink operation.", logger.Fields{Operation: Symlink, Path: dir.AbsolutePath()})
	return nil, syscall.ENOTSUP
}

func (dir *VirtualDirINode) Readlink(ctx context.Context, req *fuse.ReadlinkRequest) (string, error) {
	logger.Error("Unsupported Readlink operation.", logger.Fields{Operation: ReadLink, Path: dir.AbsolutePath()})
	return "", syscall.ENOTSUP
}

func (dir *VirtualDirINode) Link(ctx context.Context, req *fuse.LinkRequest, old fs.Node) (fs.Node, error) {
	logger.Error("Unsupported Link operation.", logger.Fields{Operation: Link, Path: dir.AbsolutePath()})
	return nil, syscall.ENOTSUP
}

func (dir *VirtualDirINode) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	logger.Info("Fsync called on Dir ", logger.Fields{Operation: Fsync, Path: dir.AbsolutePath()})
	return nil
}

// ============================================================================
// Helpers below — called by the FUSE entry points above.
// ============================================================================

// lookupVirtualDirectoryChild resolves a child name within this synthetic directory.
// This is the function that crosses the synthetic/real boundary:
//
//   - Configured branch (childRelPath is a prefix of some path in config.Paths):
//     returns another *VirtualDirINode via ensureSyntheticDirectoryChild.
//   - Configured leaf (childRelPath equals an entry in config.Paths):
//     returns a real *DirINode or *FileINode via statInodeInHopsFS.
//     Everything beneath this point is normal HopsFS.
//   - Anything else: returns ENOENT regardless of what exists at the backend.
//
// The ENOENT-for-unconfigured rule is what makes the synthetic tree a curated
// allowlist: a backend entry that exists but is not in config.Paths is invisible.
func (dir *VirtualDirINode) lookupVirtualDirectoryChild(operation, name string) (fs.Node, error) {
	childRelPath := path.Join(dir.relPath, name)
	if !dir.config.relPathExists(childRelPath) {
		return nil, syscall.ENOENT
	}

	if dir.config.leafExists(childRelPath) {
		if node := dir.getChildInode(operation, name); node != nil && nodeAttrsFresh(node, dir.FileSystem.Clock.Now()) {
			return node, nil
		}
		return dir.statInodeInHopsFS(operation, name, &Attrs{})
	}

	child, err := dir.ensureSyntheticDirectoryChild(operation, childRelPath, name, dir.config.Path(childRelPath))
	if err != nil {
		return nil, err
	}
	return child, nil
}

// ensureSyntheticDirectoryChild returns the synthetic node for the given relPath, allocating it
// on first use and reusing the cached node when its attrs are still fresh. The cache is
// invalidated if the cached node represents a different relPath or backend path than requested.
func (dir *VirtualDirINode) ensureSyntheticDirectoryChild(operation string, relPath, name, backendPath string) (*VirtualDirINode, error) {
	if node := dir.getChildInode(operation, name); node != nil {
		if cachedDir, ok := node.(*VirtualDirINode); !ok || cachedDir.config.Name != dir.config.Name || cachedDir.relPath != relPath || cachedDir.AbsolutePath() != backendPath {
			dir.removeChildInode(operation, name)
		} else if !dir.FileSystem.Clock.Now().After(cachedDir.Attrs.Expires) {
			return cachedDir, nil
		}
	}

	attrs, err := buildVirtualDirectoryAttrs(dir.FileSystem, operation, relPath, name, backendPath, dir.config)
	if err != nil {
		return nil, err
	}

	node := &VirtualDirINode{
		baseDirNode: baseDirNode{
			FileSystem: dir.FileSystem,
			Parent:     dir,
			Attrs:      attrs,
		},
		config:  dir.config,
		relPath: relPath,
	}
	dir.adoptChildInode(operation, name, node)
	return node, nil
}

// statInodeInHopsFS stats the backend at this virtual directory's AbsolutePath joined with the
// given name, then caches the result as a child inode. Defined on *VirtualDirINode (rather than
// inherited from baseDirNode) so that dir.AbsolutePath() resolves to the *VirtualDirINode override
// that produces backend paths from config, not the parent-walk version on baseDirNode that would
// produce mount-side paths for synthetic roots.
func (dir *VirtualDirINode) statInodeInHopsFS(operation, name string, attrs *Attrs) (fs.Node, error) {
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

// refreshSyntheticAttrs re-stats the backend path this synthetic node represents and updates
// the cached Attrs in place. The synthetic Name and Inode are preserved so the FUSE kernel
// continues to see the same identity; only the backend-derived fields (mode, owner, timestamps,
// size, expiry) get refreshed.
func (dir *VirtualDirINode) refreshSyntheticAttrs() error {
	backendAttrs, err := dir.FileSystem.getDFSConnector().Stat(dir.AbsolutePath())
	if err != nil {
		return err
	}
	if !backendAttrs.Mode.IsDir() {
		return fmt.Errorf("virtual directory path %q is not a directory", dir.AbsolutePath())
	}
	backendAttrs.Name = dir.Attrs.Name
	backendAttrs.Inode = dir.Attrs.Inode
	if backendAttrs.Expires.IsZero() {
		backendAttrs.Expires = dir.FileSystem.Clock.Now().Add(CacheAttrsTimeDuration)
	}
	dir.Attrs = backendAttrs
	return nil
}

// rejectMutation logs the rejection and returns EPERM. Shared by all synthetic-side write
// operations above — see the block comment on Mkdir.
func (dir *VirtualDirINode) rejectMutation(operation, candidatePath string) error {
	logger.Warn("Rejected mutation outside configured virtual tree", logger.Fields{Operation: operation, Path: candidatePath})
	return syscall.EPERM
}

// addOrUpdateChildInodeAttrs caches a child inode under name. New real-backed children
// are wired with dir as their Parent so AbsolutePath() walks up through the synthetic
// chain to the correct backend path.
func (dir *VirtualDirINode) addOrUpdateChildInodeAttrs(operation, name string, attrs Attrs) fs.Node {
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
				logger.Debug("Children's List. addOrUpdateChildInodeAttrs. Update ", logger.Fields{Operation: operation, Child: name, NumChildren: len(dir.children)})
				return node
			}
			node = newDirINode(dir.FileSystem, dir, attrs)
		} else {
			if fnode, ok := (node).(*FileINode); ok {
				fnode.Attrs = attrs
				logger.Debug("Children's List. addOrUpdateChildInodeAttrs. Update ", logger.Fields{Operation: operation, Child: name, NumChildren: len(dir.children)})
				return node
			}
			node = &FileINode{FileSystem: dir.FileSystem, Parent: dir, Attrs: attrs}
		}
		dir.children[name] = node
		logger.Debug("Children's List. addOrUpdateChildInodeAttrs. Replace ", logger.Fields{Operation: operation, Child: name, NumChildren: len(dir.children)})
		return node
	}
	var node fs.Node
	if shouldBeDir {
		node = newDirINode(dir.FileSystem, dir, attrs)
	} else {
		node = &FileINode{FileSystem: dir.FileSystem, Parent: dir, Attrs: attrs}
	}
	dir.children[name] = node
	logger.Debug("Children's List. addOrUpdateChildInodeAttrs. Add ", logger.Fields{Operation: operation, Child: name, NumChildren: len(dir.children)})
	return node
}

// ============================================================================
// FileSystem helpers for the virtual directory feature.
// Entry points called from Dir.go appear first; their helpers follow.
// ============================================================================

// HasVirtualDirectory reports whether any virtual directory is configured.
func (filesystem *FileSystem) HasVirtualDirectory() bool {
	return len(filesystem.VirtualDirectories) > 0
}

// normalizeVirtualDirectoryConfig normalizes and validates the configured virtual directories in place.
func (filesystem *FileSystem) normalizeVirtualDirectoryConfig() error {
	normalized, err := normalizeVirtualDirectoryConfigs(filesystem.VirtualDirectories)
	if err != nil {
		return err
	}
	filesystem.VirtualDirectories = normalized
	return nil
}

// Helper for the mount root: resolve a configured virtual root name to either the real backend entry or a synthetic node.
func (filesystem *FileSystem) lookupVirtualRoot(dir *DirINode, opName, name string) (fs.Node, bool, error) {
	virtualDirectory, ok := filesystem.virtualDirectoryConfigByName(name)
	if !ok {
		return nil, false, nil
	}

	now := filesystem.Clock.Now()
	if node := dir.getChildInode(opName, name); node != nil {
		switch cached := node.(type) {
		case *VirtualDirINode:
			if cached.config.Name == virtualDirectory.Name && cached.relPath == "" && !filesystem.virtualRootCollision(name) && !now.After(cached.Attrs.Expires) {
				return cached, true, nil
			}
		case *DirINode:
			if !now.After(cached.Attrs.Expires) {
				return cached, true, nil
			}
		case *FileINode:
			if !now.After(cached.Attrs.Expires) {
				return cached, true, nil
			}
		}
	}

	// Probe the backend for a colliding real entry. We do not call dir.statInodeInHopsFS here:
	// its side effects (removeChildInode, addNegativeCacheEntry on ENOENT) would destroy the
	// cached *VirtualDirINode and pollute the negative cache for a name that, by configuration,
	// is always available through the virtual layer. A bare Stat gives us collision detection
	// without touching the cache.
	backendPath := path.Join(dir.AbsolutePath(), name)
	backendAttrs, err := filesystem.getDFSConnector().Stat(backendPath)
	if err == nil {
		// Collision: real backend entry shadows the synthetic. Cache and return the real node.
		backendAttrs.Name = name
		realNode := dir.addOrUpdateChildInodeAttrs(opName, name, backendAttrs)
		return realNode, true, nil
	}
	if err != syscall.ENOENT {
		// Transient backend error. Prefer a cached synthetic over failing the lookup, since
		// the synthetic's existence is a configuration property, not a backend property.
		if node := dir.getChildInode(opName, name); node != nil {
			if cached, ok := node.(*VirtualDirINode); ok && cached.config.Name == virtualDirectory.Name && cached.relPath == "" {
				return cached, true, nil
			}
		}
		return nil, true, err
	}

	// ENOENT: no real entry. Return the cached synthetic if it is still fresh, otherwise build one.
	if node := dir.getChildInode(opName, name); node != nil {
		if cached, ok := node.(*VirtualDirINode); ok && cached.config.Name == virtualDirectory.Name && cached.relPath == "" && !now.After(cached.Attrs.Expires) {
			return cached, true, nil
		}
	}

	child, childErr := filesystem.ensureVirtualRootChild(dir, virtualDirectory)
	if childErr != nil {
		return nil, true, childErr
	}
	return child, true, nil
}

// appendVirtualRootEntries merges configured virtual roots into a ReadDirAll result at the
// mount root. Real backend entries that share a name with a configured virtual root take
// precedence and the virtual entry is suppressed for that name. The collisions map is
// persisted on the FileSystem so that subsequent Lookups (via lookupVirtualRoot) apply the
// same precedence without re-listing. Non-colliding virtual roots are appended as
// DT_Unknown placeholders unless a fresh cached VirtualDirINode is available, in which
// case its attrs drive the dirent.
func (filesystem *FileSystem) appendVirtualRootEntries(dir *DirINode, entries []fuse.Dirent) ([]fuse.Dirent, error) {
	if !filesystem.HasVirtualDirectory() {
		return entries, nil
	}

	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		seen[entry.Name] = struct{}{}
	}

	collisions := make(map[string]bool)
	for _, virtualDirectory := range filesystem.VirtualDirectories {
		if _, ok := seen[virtualDirectory.Name]; ok {
			collisions[virtualDirectory.Name] = true
		}
	}
	filesystem.setVirtualRootCollisions(collisions)

	now := filesystem.Clock.Now()
	for _, virtualDirectory := range filesystem.VirtualDirectories {
		if _, ok := seen[virtualDirectory.Name]; ok {
			continue
		}
		if node := dir.getChildInode(ReadDir, virtualDirectory.Name); node != nil && nodeAttrsFresh(node, now) {
			if attrs, ok := nodeAttrs(node); ok {
				entries = append(entries, fuse.Dirent{Inode: attrs.Inode, Name: attrs.Name, Type: attrs.FuseNodeType()})
				continue
			}
		}
		entries = append(entries, fuse.Dirent{Inode: syntheticInode(path.Join("/", virtualDirectory.Name)), Name: virtualDirectory.Name, Type: fuse.DT_Unknown})
	}
	return entries, nil
}

// ensureVirtualRootChild builds and caches the synthetic root node for a configured virtual
// directory. The root's attrs are inherited from filesystem.SrcDir (the user's mounted project)
// so the virtual root surfaces with a plausible owner and mode rather than a synthetic default.
func (filesystem *FileSystem) ensureVirtualRootChild(dir *DirINode, virtualDirectory VirtualDirectoryConfig) (*VirtualDirINode, error) {
	attrs, err := buildVirtualDirectoryAttrs(filesystem, Lookup, "", virtualDirectory.Name, filesystem.SrcDir, virtualDirectory)
	if err != nil {
		return nil, err
	}

	node := &VirtualDirINode{
		baseDirNode: baseDirNode{
			FileSystem: filesystem,
			Parent:     dir,
			Attrs:      attrs,
		},
		config:  virtualDirectory,
		relPath: "",
	}
	dir.adoptChildInode(Lookup, virtualDirectory.Name, node)
	return node, nil
}

// virtualDirectoryConfigByName returns the configured virtual directory with the given name, if any.
func (filesystem *FileSystem) virtualDirectoryConfigByName(name string) (VirtualDirectoryConfig, bool) {
	for _, virtualDirectory := range filesystem.VirtualDirectories {
		if virtualDirectory.Name == name {
			return virtualDirectory, true
		}
	}
	return VirtualDirectoryConfig{}, false
}

// virtualRootCollision reports whether the configured virtual root name currently collides
// with a real backend entry, based on the last appendVirtualRootEntries call.
func (filesystem *FileSystem) virtualRootCollision(name string) bool {
	filesystem.virtualRootCollisionsLock.RLock()
	defer filesystem.virtualRootCollisionsLock.RUnlock()

	if filesystem.virtualRootCollisions == nil {
		return false
	}
	return filesystem.virtualRootCollisions[name]
}

// setVirtualRootCollisions records which configured virtual root names currently collide
// with a real backend entry at the mount root. Populated by appendVirtualRootEntries when
// the mount root is listed.
func (filesystem *FileSystem) setVirtualRootCollisions(collisions map[string]bool) {
	filesystem.virtualRootCollisionsLock.Lock()
	defer filesystem.virtualRootCollisionsLock.Unlock()

	if len(collisions) == 0 {
		filesystem.virtualRootCollisions = nil
		return
	}

	filesystem.virtualRootCollisions = make(map[string]bool, len(collisions))
	for name, collided := range collisions {
		if collided {
			filesystem.virtualRootCollisions[name] = true
		}
	}
}

// buildVirtualDirectoryAttrs computes the Attrs for a synthetic directory node by stat'ing
// statPath at the backend and inheriting owner, mode, timestamps and size from the result.
// The synthetic Inode is derived deterministically from <virtualName>/<relPath> via
// syntheticInode so the same backend path produces a stable inode number across mount sessions.
func buildVirtualDirectoryAttrs(filesystem *FileSystem, operation, relPath, name, statPath string, virtualDirectory VirtualDirectoryConfig) (Attrs, error) {
	attrs, err := filesystem.getDFSConnector().Stat(statPath)
	if err != nil {
		return Attrs{}, err
	}
	if !attrs.Mode.IsDir() {
		return Attrs{}, fmt.Errorf("%s: virtual directory path %q is not a directory", operation, statPath)
	}
	attrs.Name = name
	attrs.Inode = syntheticInode(path.Join("/", virtualDirectory.Name, relPath))
	if attrs.Expires.IsZero() {
		attrs.Expires = filesystem.Clock.Now().Add(CacheAttrsTimeDuration)
	}
	return attrs, nil
}
