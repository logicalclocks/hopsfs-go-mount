// Copyright (c) Microsoft. All rights reserved.
// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package hopsfsmount

import (
	"fmt"
	"github.com/colinmarc/hdfs/v2"
	"hash/fnv"
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

// Encapsulates state and operations for directory node on the HDFS file system
type DirINode struct {
	FileSystem            *FileSystem     // Pointer to the owning filesystem
	Attrs                 Attrs           // Cached attributes of the directory, TODO: add TTL
	Parent                *DirINode       // Pointer to the parent directory (allows computing fully-qualified paths on demand)
	BackendPath           string          // Optional backend path override for synthetic directories
	VirtualDirectoryName  string          // Name of the configured virtual directory that owns this inode
	VirtualStatPath       string          // Backend path used when refreshing synthetic metadata
	VirtualKind           VirtualDirKind  // Indicates whether this directory is synthetic or backed by the real tree
	VirtualRelPath        string          // Path relative to the owning virtual directory root
	VirtualRootCollisions map[string]bool // Root child names that collide with synthetic virtual roots
	children              map[string]fs.Node
	negativeCache         map[string]time.Time
	childrenMutex         sync.Mutex
	dirMutex              sync.Mutex
}

type VirtualDirKind int

const (
	VirtualDirNone VirtualDirKind = iota
	VirtualDirSynthetic
)

// Verify that *Dir implements necesary FUSE interfaces
var _ fs.Node = (*DirINode)(nil)
var _ fs.HandleReadDirAller = (*DirINode)(nil)
var _ fs.NodeStringLookuper = (*DirINode)(nil)
var _ fs.NodeMkdirer = (*DirINode)(nil)
var _ fs.NodeRemover = (*DirINode)(nil)
var _ fs.NodeRenamer = (*DirINode)(nil)
var _ fs.NodeForgetter = (*DirINode)(nil)
var _ fs.NodeSymlinker = (*DirINode)(nil)
var _ fs.NodeReadlinker = (*DirINode)(nil)
var _ fs.NodeLinker = (*DirINode)(nil)
var _ fs.NodeCreater = (*DirINode)(nil)
var _ fs.NodeFsyncer = (*DirINode)(nil)

// Returns absolute path of the dir in HDFS namespace
func (dir *DirINode) AbsolutePath() string {
	if dir.BackendPath != "" {
		return dir.BackendPath
	}
	if dir.Parent == nil {
		return dir.FileSystem.SrcDir
	} else {
		return path.Join(dir.Parent.AbsolutePath(), dir.Attrs.Name)
	}
}

// Returns absolute path of the child item of this directory
func (dir *DirINode) AbsolutePathForChild(name string) string {
	return path.Join(dir.AbsolutePath(), name)
}

// Responds on FUSE request to get directory attributes
func (dir *DirINode) Attr(ctx context.Context, a *fuse.Attr) error {
	dir.lockMutex()
	defer dir.unlockMutex()

	if dir.VirtualKind == VirtualDirSynthetic {
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

	if dir.VirtualKind == VirtualDirNone && dir.Parent != nil && dir.FileSystem.Clock.Now().After(dir.Attrs.Expires) {
		_, err := dir.Parent.statInodeInHopsFS(GetattrDir, dir.Attrs.Name, &dir.Attrs)
		if err != nil {
			return err
		}
	} else {
		logger.Info("Stat successful. Returning from Cache ", logger.Fields{Operation: GetattrDir, Path: path.Join(dir.AbsolutePath()), FileSize: dir.Attrs.Size,
			IsDir: dir.Attrs.Mode.IsDir(), IsRegular: dir.Attrs.Mode.IsRegular()})
	}
	return dir.Attrs.ConvertAttrToFuse(a)
}

func (dir *DirINode) getChildInode(operation, name string) fs.Node {
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

func (dir *DirINode) addOrUpdateChildInodeAttrs(operation, name string, attrs Attrs) fs.Node {
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

func (dir *DirINode) removeChildInode(operation, name string) {
	dir.lockChildrenMutex()
	defer dir.unlockChildrenMutex()

	if dir.children != nil {
		delete(dir.children, name)
		logger.Debug("Children's List. removeChildInode ", logger.Fields{Operation: operation, Parent: dir.AbsolutePath(), Child: name, NumChildren: len(dir.children)})
	}
}

// used in rename. when an inode is moved from one dir to another
func (dir *DirINode) adoptChildInode(operation, name string, node fs.Node) {
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

// Responds on FUSE request to lookup the directory
func (dir *DirINode) Lookup(ctx context.Context, name string) (fs.Node, error) {
	dir.lockMutex()
	defer dir.unlockMutex()

	return dir.LookupInt(Lookup, name)
}

func (dir *DirINode) virtualDirectoryConfig() (VirtualDirectoryConfig, bool) {
	if dir.VirtualDirectoryName == "" {
		return VirtualDirectoryConfig{}, false
	}
	return dir.FileSystem.virtualDirectoryConfigByName(dir.VirtualDirectoryName)
}

func (dir *DirINode) rootVirtualDirectoryCollision(name string) bool {
	return dir.VirtualRootCollisions != nil && dir.VirtualRootCollisions[name]
}

func (dir *DirINode) LookupInt(opName string, name string) (fs.Node, error) {
	if dir.Parent == nil && dir.FileSystem.HasVirtualDirectory() {
		if virtualDirectory, ok := dir.FileSystem.virtualDirectoryConfigByName(name); ok {
			now := dir.FileSystem.Clock.Now()
			if node := dir.getChildInode(opName, name); node != nil {
				switch cached := node.(type) {
				case *DirINode:
					if cached.VirtualKind == VirtualDirSynthetic && cached.VirtualDirectoryName == virtualDirectory.Name && !dir.rootVirtualDirectoryCollision(name) && now.Before(cached.Attrs.Expires) {
						return cached, nil
					}
					if cached.VirtualKind == VirtualDirNone && now.Before(cached.Attrs.Expires) {
						return cached, nil
					}
				case *FileINode:
					if now.Before(cached.Attrs.Expires) {
						return cached, nil
					}
				}
			}

			if dir.rootVirtualDirectoryCollision(name) {
				dir.removeChildInode(opName, name)
			}
			var attrs Attrs
			node, err := dir.statInodeInHopsFS(opName, name, &attrs)
			if err == nil {
				return node, nil
			}
			if err != syscall.ENOENT {
				return nil, err
			}
			child, childErr := dir.ensureVirtualDirectoryRootChild(opName, virtualDirectory)
			if childErr != nil {
				return nil, childErr
			}
			return child, nil
		}
	}

	if dir.VirtualKind == VirtualDirSynthetic {
		return dir.lookupVirtualDirectoryChild(opName, name)
	}

	if !dir.FileSystem.IsPathAllowed(dir.AbsolutePathForChild(name)) {
		return nil, syscall.ENOENT
	}

	if node := dir.getChildInode(opName, name); node != nil {
		return node, nil
	}

	// Check negative cache before hitting the backend
	if dir.checkNegativeCache(opName, name) {
		return nil, syscall.ENOENT
	}

	var attrs Attrs
	node, err := dir.statInodeInHopsFS(opName, name, &attrs)
	if err != nil {
		return nil, err
	}
	return node, nil
}

// Responds on FUSE request to read directory
func (dir *DirINode) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	dir.lockMutex()
	defer dir.unlockMutex()

	if dir.VirtualKind == VirtualDirSynthetic {
		return dir.readVirtualDirectoryEntries()
	}

	absolutePath := dir.AbsolutePath()
	logger.Info("Read directory", logger.Fields{Operation: ReadDir, Path: absolutePath})

	allAttrs, err := dir.FileSystem.getDFSConnector().ReadDir(absolutePath)
	if err != nil {
		logger.Warn("Failed to list DFS directory", logger.Fields{Operation: ReadDir, Path: absolutePath, Error: err})
		return nil, err
	}

	entries := make([]fuse.Dirent, 0, len(allAttrs))
	collisions := make(map[string]bool)
	for _, a := range allAttrs {
		if dir.FileSystem.IsPathAllowed(dir.AbsolutePathForChild(a.Name)) {
			// Creating Dirent structure as required by FUSE
			entries = append(entries, fuse.Dirent{
				Inode: a.Inode,
				Name:  a.Name,
				Type:  a.FuseNodeType()})
			// Speculatively pre-creating child Dir or File node with cached attributes,
			// since it's highly likely that we will have Lookup() call for this name
			// This is the key trick which dramatically speeds up 'ls'
			if dir.Parent == nil {
				if virtualDirectory, ok := dir.FileSystem.virtualDirectoryConfigByName(a.Name); ok {
					collisions[virtualDirectory.Name] = true
					dir.removeChildInode(ReadDir, a.Name)
				}
			}
			dir.addOrUpdateChildInodeAttrs(ReadDir, a.Name, a)
		}
	}

	if dir.Parent == nil && dir.FileSystem.HasVirtualDirectory() {
		dir.VirtualRootCollisions = collisions
		now := dir.FileSystem.Clock.Now()
		for _, virtualDirectory := range dir.FileSystem.VirtualDirectories {
			if collisions[virtualDirectory.Name] {
				continue
			}
			if node := dir.getChildInode(ReadDir, virtualDirectory.Name); node != nil && nodeAttrsFresh(node, now) {
				if attrs, ok := nodeAttrs(node); ok {
					entries = append(entries, fuse.Dirent{
						Inode: attrs.Inode,
						Name:  attrs.Name,
						Type:  attrs.FuseNodeType(),
					})
					continue
				}
			}
			entries = append(entries, fuse.Dirent{
				Inode: syntheticInode(path.Join("/", virtualDirectory.Name)),
				Name:  virtualDirectory.Name,
				Type:  fuse.DT_Unknown,
			})
		}
	}
	return entries, nil
}

func (dir *DirINode) readVirtualDirectoryEntries() ([]fuse.Dirent, error) {
	virtualDirectory, ok := dir.virtualDirectoryConfig()
	if !ok {
		return nil, fmt.Errorf("virtual directory %q is not configured", dir.VirtualDirectoryName)
	}
	childNames := virtualDirectory.childNames(dir.VirtualRelPath)
	entries := make([]fuse.Dirent, 0, len(childNames))
	now := dir.FileSystem.Clock.Now()
	for _, childName := range childNames {
		childRelPath := path.Join(dir.VirtualRelPath, childName)
		if virtualDirectory.leafExists(childRelPath) {
			if node := dir.getChildInode(ReadDir, childName); node != nil && nodeAttrsFresh(node, now) {
				attrs, ok := nodeAttrs(node)
				if !ok {
					return nil, fmt.Errorf("unexpected cached node type for %s", childName)
				}
				entries = append(entries, fuse.Dirent{
					Inode: attrs.Inode,
					Name:  childName,
					Type:  attrs.FuseNodeType(),
				})
				continue
			}

			entries = append(entries, fuse.Dirent{
				Inode: syntheticInode(path.Join("/", virtualDirectory.Name, childRelPath)),
				Name:  childName,
				Type:  fuse.DT_Unknown,
			})
			continue
		}

		child, err := dir.ensureSyntheticDirectoryChild(
			ReadDir,
			childRelPath,
			childName,
			virtualDirectory.Path(childRelPath),
			virtualDirectory.Path(childRelPath),
			dir.Attrs,
			virtualDirectory,
		)
		if err != nil {
			return nil, err
		}
		entries = append(entries, fuse.Dirent{
			Inode: child.Attrs.Inode,
			Name:  childName,
			Type:  fuse.DT_Dir,
		})
	}
	return entries, nil
}

func (dir *DirINode) ensureVirtualDirectoryRootChild(operation string, virtualDirectory VirtualDirectoryConfig) (*DirINode, error) {
	child, err := dir.ensureSyntheticDirectoryChild(
		operation,
		"",
		virtualDirectory.Name,
		virtualDirectory.BackendRoot,
		dir.FileSystem.SrcDir,
		dir.Attrs,
		virtualDirectory,
	)
	if err != nil {
		return nil, err
	}
	return child, nil
}

func (dir *DirINode) lookupVirtualDirectoryChild(operation, name string) (fs.Node, error) {
	virtualDirectory, ok := dir.virtualDirectoryConfig()
	if !ok {
		return nil, syscall.ENOENT
	}
	childRelPath := path.Join(dir.VirtualRelPath, name)
	if !virtualDirectory.relPathExists(childRelPath) {
		return nil, syscall.ENOENT
	}

	if virtualDirectory.leafExists(childRelPath) {
		if node := dir.getChildInode(operation, name); node != nil && nodeAttrsFresh(node, dir.FileSystem.Clock.Now()) {
			return node, nil
		}
		return dir.statInodeInHopsFS(operation, name, &Attrs{})
	}

	child, err := dir.ensureSyntheticDirectoryChild(
		operation,
		childRelPath,
		name,
		virtualDirectory.Path(childRelPath),
		virtualDirectory.Path(childRelPath),
		dir.Attrs,
		virtualDirectory,
	)
	if err != nil {
		return nil, err
	}
	return child, nil
}

func (dir *DirINode) ensureSyntheticDirectoryChild(operation string, relPath, name, backendPath, statPath string, fallback Attrs, virtualDirectory VirtualDirectoryConfig) (*DirINode, error) {
	if node := dir.getChildInode(operation, name); node != nil {
		if cachedDir, ok := node.(*DirINode); !ok || cachedDir.VirtualKind != VirtualDirSynthetic || cachedDir.VirtualDirectoryName != virtualDirectory.Name || cachedDir.VirtualStatPath != statPath || cachedDir.BackendPath != backendPath || cachedDir.VirtualRelPath != relPath {
			dir.removeChildInode(operation, name)
		}
	}

	attrs, err := dir.syntheticDirectoryAttrs(operation, relPath, name, statPath, fallback, virtualDirectory)
	if err != nil {
		return nil, err
	}

	node := dir.addOrUpdateChildInodeAttrs(operation, name, attrs)
	dnode, ok := node.(*DirINode)
	if !ok {
		return nil, fmt.Errorf("virtual directory path %q resolved to non-directory backend node", statPath)
	}
	dnode.VirtualKind = VirtualDirSynthetic
	dnode.VirtualDirectoryName = virtualDirectory.Name
	dnode.VirtualRelPath = relPath
	dnode.BackendPath = backendPath
	dnode.VirtualStatPath = statPath
	return dnode, nil
}

func (dir *DirINode) syntheticDirectoryAttrs(operation, relPath, name, statPath string, fallback Attrs, virtualDirectory VirtualDirectoryConfig) (Attrs, error) {
	if node := dir.getChildInode(operation, name); node != nil {
		if cachedDir, ok := node.(*DirINode); ok && cachedDir.VirtualKind == VirtualDirSynthetic && cachedDir.VirtualStatPath == statPath && !dir.FileSystem.Clock.Now().After(cachedDir.Attrs.Expires) {
			return cachedDir.Attrs, nil
		}
	}

	attrs := Attrs{
		Name:  name,
		Mode:  os.ModeDir | 0755,
		Inode: syntheticInode(path.Join("/", virtualDirectory.Name, relPath)),
		Uid:   fallback.Uid,
		Gid:   fallback.Gid,
		Mtime: dir.FileSystem.Clock.Now(),
		Ctime: dir.FileSystem.Clock.Now(),
	}

	if statPath != "" {
		backendAttrs, err := dir.FileSystem.getDFSConnector().Stat(statPath)
		if err != nil {
			return Attrs{}, err
		}
		if !backendAttrs.Mode.IsDir() {
			return Attrs{}, fmt.Errorf("%s: virtual directory path %q is not a directory", operation, statPath)
		}
		attrs.Mode = backendAttrs.Mode
		attrs.Uid = backendAttrs.Uid
		attrs.Gid = backendAttrs.Gid
		attrs.DFSUserName = backendAttrs.DFSUserName
		attrs.DFSGroupName = backendAttrs.DFSGroupName
		attrs.Mtime = backendAttrs.Mtime
		attrs.Ctime = backendAttrs.Ctime
		attrs.Size = backendAttrs.Size
		attrs.Expires = backendAttrs.Expires
	}

	if attrs.Expires.IsZero() {
		attrs.Expires = dir.FileSystem.Clock.Now().Add(CacheAttrsTimeDuration)
	}

	return attrs, nil
}

func (dir *DirINode) refreshSyntheticAttrs() error {
	if dir.VirtualStatPath == "" {
		return nil
	}

	backendAttrs, err := dir.FileSystem.getDFSConnector().Stat(dir.VirtualStatPath)
	if err != nil {
		return err
	}
	if !backendAttrs.Mode.IsDir() {
		return fmt.Errorf("virtual directory path %q is not a directory", dir.VirtualStatPath)
	}
	backendAttrs.Name = dir.Attrs.Name
	backendAttrs.Inode = dir.Attrs.Inode
	if backendAttrs.Expires.IsZero() {
		backendAttrs.Expires = dir.FileSystem.Clock.Now().Add(CacheAttrsTimeDuration)
	}
	dir.Attrs = backendAttrs
	return nil
}

func nodeAttrs(node fs.Node) (Attrs, bool) {
	switch n := node.(type) {
	case *DirINode:
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

func (dir *DirINode) virtualMutationAllowed(candidatePath string) bool {
	if dir.VirtualKind != VirtualDirSynthetic {
		return true
	}
	if virtualDirectory, ok := dir.virtualDirectoryConfig(); ok {
		return virtualDirectory.mutationAllowed(candidatePath)
	}
	return false
}

func syntheticInode(key string) uint64 {
	hasher := fnv.New64a()
	_, _ = hasher.Write([]byte(key))
	inode := hasher.Sum64()
	if inode == 0 {
		return 1
	}
	return inode
}

// Performs Stat() query on the backend
func (dir *DirINode) statInodeInHopsFS(operation, name string, attrs *Attrs) (fs.Node, error) {

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

// Responds on FUSE Mkdir request
func (dir *DirINode) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	dir.lockMutex()
	defer dir.unlockMutex()

	targetPath := dir.AbsolutePathForChild(req.Name)
	if !dir.virtualMutationAllowed(targetPath) {
		logger.Warn("Rejected mkdir outside configured virtual tree", logger.Fields{Operation: Mkdir, Path: targetPath})
		return nil, syscall.EPERM
	}

	// check user and group information first.
	userName, err := getUserName(req.Uid)
	if err != nil {
		logger.Error("Unable to find user information. ", logger.Fields{Operation: Mkdir,
			Path: targetPath, UID: req.Uid, HopsFSUserName: GetConnectionUser()})
		return nil, err
	}

	groupName, err := getGroupName(dir.AbsolutePathForChild(req.Name), req.Gid)
	if err != nil {
		logger.Error("Unable to find group information. ", logger.Fields{Operation: Mkdir,
			Path: targetPath, GID: req.Gid,
			GetGroupFromHopsFSDatasetPath: UseGroupFromHopsFsDatasetPath})
		return nil, err
	}
	req.Mode = ComputePermissions(req.Mode)
	err = dir.FileSystem.getDFSConnector().MkdirWithGroup(targetPath, req.Mode, groupName)
	if err != nil {
		logger.Info("mkdir failed", logger.Fields{Operation: Mkdir, Path: targetPath, Error: err})
		return nil, err
	}
	logger.Debug("mkdir successful with group", logger.Fields{Operation: Mkdir, Path: targetPath, Group: groupName})

	dir.removeNegativeCacheEntry(req.Name)
	newInode := dir.addOrUpdateChildInodeAttrs(Mkdir, req.Name,
		Attrs{
			Name:         req.Name,
			Mode:         req.Mode | os.ModeDir,
			Uid:          req.Uid,
			Gid:          req.Gid,
			DFSUserName:  userName,
			DFSGroupName: groupName,
		})
	return newInode, nil
}

// Responds on FUSE Create request
func (dir *DirINode) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	dir.lockMutex()
	defer dir.unlockMutex()

	req.Mode = ComputePermissions(req.Mode)
	targetPath := dir.AbsolutePathForChild(req.Name)
	if !dir.virtualMutationAllowed(targetPath) {
		logger.Warn("Rejected create outside configured virtual tree", logger.Fields{Operation: Create, Path: targetPath})
		return nil, nil, syscall.EPERM
	}
	logger.Info("Creating a new file", logger.Fields{Operation: Create, Path: targetPath, Mode: req.Mode, Flags: req.Flags})

	// first determine the usename and grup name for the new file
	userName, err := getUserName(req.Uid)
	if err != nil {
		logger.Error("Unable to find user information. ", logger.Fields{Operation: Create,
			Path: targetPath, UID: req.Uid, HopsFSUserName: GetConnectionUser()})
		return nil, nil, err
	}

	groupName, err := getGroupName(dir.AbsolutePathForChild(req.Name), req.Gid)
	if err != nil {
		logger.Error("Unable to find group information. ", logger.Fields{Operation: Create,
			Path: targetPath, GID: req.Gid,
			GetGroupFromHopsFSDatasetPath: UseGroupFromHopsFsDatasetPath})
		return nil, nil, err
	}

	newFileAttrs := Attrs{
		Name:         req.Name,
		Mode:         req.Mode,
		Uid:          req.Uid,
		Gid:          req.Gid,
		DFSUserName:  userName,
		DFSGroupName: groupName,
	}

	dir.removeNegativeCacheEntry(req.Name)
	file := (dir.addOrUpdateChildInodeAttrs(Create, req.Name, newFileAttrs)).(*FileINode)
	handle, err := file.NewFileHandle(false, req.Flags)
	if err != nil {
		logger.Error("File creation failed", logger.Fields{Operation: Create, Path: targetPath, Mode: req.Mode, Flags: req.Flags, Error: err})
		dir.removeChildInode(Create, req.Name)
		return nil, nil, err
	}
	// Note: handle is already added to activeHandles inside NewFileHandle
	// File created with groupname parameter - no chown needed
	logger.Debug("File created with group", logger.Fields{
		Operation: Create,
		Path:      targetPath,
		User:      userName,
		Group:     groupName,
	})

	//update the attributes of the file now
	_, err = dir.statInodeInHopsFS(Create, file.Attrs.Name, &file.Attrs)
	if err != nil {
		dir.removeChildInode(Create, req.Name)
		return nil, nil, err
	}

	return file, handle, nil
}

// Responds on FUSE Remove request
func (dir *DirINode) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	dir.lockMutex()
	defer dir.unlockMutex()

	targetPath := dir.AbsolutePathForChild(req.Name)
	if !dir.virtualMutationAllowed(targetPath) {
		logger.Warn("Rejected remove outside configured virtual tree", logger.Fields{Operation: Remove, Path: targetPath})
		return syscall.EPERM
	}

	logger.Debug("Removing path", logger.Fields{Operation: Remove, Path: targetPath})
	err := dir.FileSystem.getDFSConnector().Remove(targetPath)
	if err == nil {
		dir.removeChildInode(Remove, req.Name)
		// Invalidate staging file cache for the removed path
		if StagingCache != nil {
			StagingCache.Remove(targetPath)
		}
		logger.Info("Removed path", logger.Fields{Operation: Remove, Path: targetPath})
	} else {
		logger.Warn("Failed to remove path", logger.Fields{Operation: Remove, Path: targetPath, Error: err})
	}
	return err
}

// Responds on FUSE Rename request
func (srcParent *DirINode) Rename(ctx context.Context, req *fuse.RenameRequest, dstParentDir fs.Node) error {
	srcParent.lockMutex()
	defer srcParent.unlockMutex()

	return srcParent.renameInt(Rename, req.OldName, req.NewName, dstParentDir, hdfs.RENAME_OPTION_NONE)
}

func (srcParent *DirINode) renameInt(operationName, oldName, newName string, dstParentDir fs.Node, options hdfs.RenameOptions) error {
	oldPath := srcParent.AbsolutePathForChild(oldName)
	newPath := dstParentDir.(*DirINode).AbsolutePathForChild(newName)
	logger.Debug("Renaming", logger.Fields{Operation: operationName, From: oldPath, To: newPath})

	if !srcParent.virtualMutationAllowed(oldPath) || !dstParentDir.(*DirINode).virtualMutationAllowed(newPath) {
		logger.Warn("Rejected rename outside configured virtual tree", logger.Fields{Operation: operationName, From: oldPath, To: newPath})
		return syscall.EPERM
	}

	srcInode, err := srcParent.LookupInt(Rename, oldName)
	if err != nil {
		logger.Error("Rename failed. Src Inode not found", logger.Fields{Operation: operationName, From: oldPath, To: newPath})
		return err
	}

	dstInode, err := dstParentDir.(*DirINode).LookupInt(Rename, newName)
	if err == nil {
		logger.Debug("Rename. Dst Inode not found", logger.Fields{Operation: operationName, From: oldPath, To: newPath})
	}

	// update backend
	err = srcParent.FileSystem.getDFSConnector().Rename2(oldPath, newPath, options)
	if err != nil {
		logger.Error("Rename failed at the backend", logger.Fields{Operation: operationName, From: oldPath, To: newPath, Error: err})
		return err
	}

	// Transfer staging file cache entry from old path to new path if it exists
	if StagingCache != nil {
		StagingCache.Rename(oldPath, newPath)
	}

	// disconnect src inode
	if srcInode != nil {
		srcParent.removeChildInode(Rename, oldName)
	}

	// disconnect dst inode
	if dstInode != nil {
		dstParentDir.(*DirINode).removeChildInode(Rename, newName)
	}

	// Invalidate negative cache for the new name in the destination directory
	dstParentDir.(*DirINode).removeNegativeCacheEntry(newName)

	// Upon successful rename, updating in-memory representation of the file entry
	// file rename
	if fnode, ok := (srcInode).(*FileINode); ok {
		logger.Trace("Rename src is file", logger.Fields{Operation: operationName, From: oldPath, To: newPath})
		fnode.Attrs.Name = newName
		fnode.Parent = dstParentDir.(*DirINode)
		dstParentDir.(*DirINode).adoptChildInode(Rename, newName, fnode)
	}

	// dir rename
	if dnode, ok := (srcInode).(*DirINode); ok {
		logger.Trace("Rename src is dir", logger.Fields{Operation: operationName, From: oldPath, To: newPath})
		dnode.Attrs.Name = newName
		dnode.Parent = dstParentDir.(*DirINode)
		dstParentDir.(*DirINode).adoptChildInode(Rename, newName, dnode)
	}

	logger.Info("Renamed", logger.Fields{Operation: operationName, From: oldPath, To: newPath})
	return nil
}

// Responds on FUSE Rename request
func (srcParent *DirINode) Rename2(ctx context.Context, req *fuse.Rename2Request, dstParentDir fs.Node) error {
	srcParent.lockMutex()
	defer srcParent.unlockMutex()

	if req.Flags&fuse.RENAME_EXCHANGE == fuse.RENAME_EXCHANGE ||
		req.Flags&fuse.RENAME_WHITEOUT == fuse.RENAME_WHITEOUT {
		logger.Error("Rename2. Unsupported Flags ", logger.Fields{Operation: Rename2, Flags: req.Flags.String()})
		return syscall.EINVAL
	}

	options := hdfs.RENAME_OPTION_NONE
	if req.Flags&fuse.RENAME_NOREPLACE == fuse.RENAME_NOREPLACE {
		options = options | hdfs.RENAME_NOREPLACE
	}

	return srcParent.renameInt(Rename2, req.OldName, req.NewName, dstParentDir, hdfs.RenameOptions(options))
}

// Responds on FUSE Chmod request
func (dir *DirINode) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	dir.lockMutex()
	defer dir.unlockMutex()

	path := dir.AbsolutePath()
	if !dir.virtualMutationAllowed(path) {
		logger.Warn("Rejected setattr outside configured virtual tree", logger.Fields{Operation: Chmod, Path: path})
		return syscall.EPERM
	}

	if req.Valid.Size() {
		logger.Error(fmt.Sprintf("Unsupported operation. Can not set size of a directory"), logger.Fields{Operation: Chmod, Path: path})
		return syscall.ENOTSUP
	}

	if req.Valid.Mode() {
		if err := ChmodOp(&dir.Attrs, dir.FileSystem, path, req, resp); err != nil {
			logger.Warn("Setattr (chmod) failed. ", logger.Fields{Operation: Chmod, Path: path, Mode: req.Mode})
			return err
		}
	}

	if req.Valid.Uid() || req.Valid.Gid() {
		if err := SetAttrChownOp(&dir.Attrs, dir.FileSystem, path, req, resp); err != nil {
			logger.Warn("Setattr (chown/chgrp )failed", logger.Fields{Operation: Chmod, Path: path, UID: req.Uid, GID: req.Gid})
			return err
		}
	}

	if err := UpdateTS(&dir.Attrs, dir.FileSystem, path, req, resp); err != nil {
		return err
	}

	return nil
}

// Responds on FUSE request to forget inode
func (dir *DirINode) Forget() {
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

	// ask parent to remove me from the children list
	// logger.Debug(fmt.Sprintf("Forget for dir %s", dir.Attrs.Name), nil)
	// dir.Parent.removeChildInode(Forget, dir.Attrs.Name)
}

// checkNegativeCache returns true if the name is in the negative cache and not expired.
// Must NOT hold childrenMutex when calling this.
func (dir *DirINode) checkNegativeCache(operation, name string) bool {
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

// addNegativeCacheEntry adds a name to the negative cache with TTL = CacheAttrsTimeDuration.
// Must NOT hold childrenMutex when calling this.
func (dir *DirINode) addNegativeCacheEntry(name string) {
	dir.lockChildrenMutex()
	defer dir.unlockChildrenMutex()

	if dir.negativeCache == nil {
		dir.negativeCache = make(map[string]time.Time)
	}

	dir.negativeCache[name] = dir.FileSystem.Clock.Now().Add(CacheAttrsTimeDuration)
}

// removeNegativeCacheEntry removes a name from the negative cache.
// Must NOT hold childrenMutex when calling this.
func (dir *DirINode) removeNegativeCacheEntry(name string) {
	dir.lockChildrenMutex()
	defer dir.unlockChildrenMutex()

	if dir.negativeCache != nil {
		delete(dir.negativeCache, name)
	}
}

func (dir *DirINode) lockMutex() {
	dir.dirMutex.Lock()
}

func (dir *DirINode) unlockMutex() {
	dir.dirMutex.Unlock()
}

func (dir *DirINode) lockChildrenMutex() {
	dir.childrenMutex.Lock()
}

func (dir *DirINode) unlockChildrenMutex() {
	dir.childrenMutex.Unlock()
}

func (dir *DirINode) Symlink(ctx context.Context, req *fuse.SymlinkRequest) (fs.Node, error) {
	logger.Error("Unsupported Symlink operation.", logger.Fields{Operation: Symlink, Path: dir.AbsolutePath()})
	return nil, syscall.ENOTSUP
}

func (dir *DirINode) Readlink(ctx context.Context, req *fuse.ReadlinkRequest) (string, error) {
	logger.Error("Unsupported Readlink operation.", logger.Fields{Operation: ReadLink, Path: dir.AbsolutePath()})
	return "", syscall.ENOTSUP
}

func (dir *DirINode) Link(ctx context.Context, req *fuse.LinkRequest, old fs.Node) (fs.Node, error) {
	logger.Error("Unsupported Link operation.", logger.Fields{Operation: Link, Path: dir.AbsolutePath()})
	return nil, syscall.ENOTSUP
}

// https://libfuse.github.io/doxygen/structfuse__operations.html#abaa2a0bdc9b9955a399ea6973f6f4927
// Synchronize directory contents
// All dir operations are first performed on the backend. So no-op
func (dir *DirINode) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	logger.Info("Fsync called on Dir ", logger.Fields{Operation: Fsync, Path: dir.AbsolutePath()})
	return nil
}
