// Copyright (c) Microsoft. All rights reserved.
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

// VirtualDirINode represents a synthetic directory that is backed by one of the configured virtual roots.
type VirtualDirINode struct {
	baseDirNode
	config  VirtualDirectoryConfig
	relPath string
}

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

		child, err := dir.ensureSyntheticDirectoryChild(ReadDir, childRelPath, childName, dir.config.Path(childRelPath), dir.config.Path(childRelPath), dir.Attrs)
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

	child, err := dir.ensureSyntheticDirectoryChild(operation, childRelPath, name, dir.config.Path(childRelPath), dir.config.Path(childRelPath), dir.Attrs)
	if err != nil {
		return nil, err
	}
	return child, nil
}

func (dir *VirtualDirINode) ensureSyntheticDirectoryChild(operation string, relPath, name, backendPath, statPath string, fallback Attrs) (*VirtualDirINode, error) {
	if node := dir.getChildInode(operation, name); node != nil {
		if cachedDir, ok := node.(*VirtualDirINode); !ok || cachedDir.config.Name != dir.config.Name || cachedDir.relPath != relPath || cachedDir.AbsolutePath() != backendPath {
			dir.removeChildInode(operation, name)
		} else if !dir.FileSystem.Clock.Now().After(cachedDir.Attrs.Expires) {
			return cachedDir, nil
		}
	}

	attrs, err := buildVirtualDirectoryAttrs(dir.FileSystem, operation, relPath, name, statPath, fallback, dir.config)
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

func (dir *VirtualDirINode) rejectMutation(operation, candidatePath string) error {
	logger.Warn("Rejected mutation outside configured virtual tree", logger.Fields{Operation: operation, Path: candidatePath})
	return syscall.EPERM
}

// Virtual directories are structural only. Mutations must happen on the real leaf nodes returned by Lookup.
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

	var attrs Attrs
	node, err := dir.statInodeInHopsFS(opName, name, &attrs)
	if err == nil {
		return node, true, nil
	}
	if err != syscall.ENOENT {
		return nil, true, err
	}

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

func (filesystem *FileSystem) ensureVirtualRootChild(dir *DirINode, virtualDirectory VirtualDirectoryConfig) (*VirtualDirINode, error) {
	attrs, err := buildVirtualDirectoryAttrs(filesystem, Lookup, "", virtualDirectory.Name, filesystem.SrcDir, dir.Attrs, virtualDirectory)
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

func buildVirtualDirectoryAttrs(filesystem *FileSystem, operation, relPath, name, statPath string, fallback Attrs, virtualDirectory VirtualDirectoryConfig) (Attrs, error) {
	attrs := Attrs{
		Name:  name,
		Mode:  os.ModeDir | 0755,
		Inode: syntheticInode(path.Join("/", virtualDirectory.Name, relPath)),
		Uid:   fallback.Uid,
		Gid:   fallback.Gid,
		Mtime: filesystem.Clock.Now(),
		Ctime: filesystem.Clock.Now(),
	}

	if statPath != "" {
		backendAttrs, err := filesystem.getDFSConnector().Stat(statPath)
		if err != nil {
			return Attrs{}, err
		}
		if !backendAttrs.Mode.IsDir() {
			return Attrs{}, fmt.Errorf("%s: virtual directory path %q is not a directory", operation, statPath)
		}
		backendAttrs.Name = name
		backendAttrs.Inode = attrs.Inode
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
		attrs.Expires = filesystem.Clock.Now().Add(CacheAttrsTimeDuration)
	}
	return attrs, nil
}
