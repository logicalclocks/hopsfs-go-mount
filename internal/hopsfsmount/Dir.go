// Copyright (c) Microsoft. All rights reserved.
// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package hopsfsmount

import (
	"fmt"
	"github.com/colinmarc/hdfs/v2"
	"os"
	"path"
	"syscall"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
	"hopsworks.ai/hopsfsmount/internal/hopsfsmount/logger"
)

// DirINode represents a real backend-backed directory node.
type DirINode struct {
	baseDirNode
}

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

// Responds on FUSE request to get directory attributes.
func (dir *DirINode) Attr(ctx context.Context, a *fuse.Attr) error {
	dir.lockMutex()
	defer dir.unlockMutex()

	if dir.Parent != nil && dir.FileSystem.Clock.Now().After(dir.Attrs.Expires) {
		_, err := parentStatInodeInHopsFS(dir.Parent, GetattrDir, dir.Attrs.Name, &dir.Attrs)
		if err != nil {
			return err
		}
	} else {
		logger.Info("Stat successful. Returning from Cache ", logger.Fields{Operation: GetattrDir, Path: path.Join(dir.AbsolutePath()), FileSize: dir.Attrs.Size,
			IsDir: dir.Attrs.Mode.IsDir(), IsRegular: dir.Attrs.Mode.IsRegular()})
	}
	return dir.Attrs.ConvertAttrToFuse(a)
}

// Responds on FUSE request to lookup the directory.
func (dir *DirINode) Lookup(ctx context.Context, name string) (fs.Node, error) {
	dir.lockMutex()
	defer dir.unlockMutex()

	return dir.LookupInt(Lookup, name)
}

func (dir *DirINode) LookupInt(opName string, name string) (fs.Node, error) {
	if dir.Parent == nil && dir.FileSystem.HasVirtualDirectory() {
		if node, handled, err := dir.FileSystem.lookupVirtualRoot(dir, opName, name); handled || err != nil {
			return node, err
		}
	}

	if !dir.FileSystem.IsPathAllowed(dir.AbsolutePathForChild(name)) {
		return nil, syscall.ENOENT
	}

	if node := dir.getChildInode(opName, name); node != nil {
		return node, nil
	}

	// Check negative cache before hitting the backend.
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

// Responds on FUSE request to read directory.
func (dir *DirINode) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	dir.lockMutex()
	defer dir.unlockMutex()

	absolutePath := dir.AbsolutePath()
	logger.Info("Read directory", logger.Fields{Operation: ReadDir, Path: absolutePath})

	allAttrs, err := dir.FileSystem.getDFSConnector().ReadDir(absolutePath)
	if err != nil {
		logger.Warn("Failed to list DFS directory", logger.Fields{Operation: ReadDir, Path: absolutePath, Error: err})
		return nil, err
	}

	entries := make([]fuse.Dirent, 0, len(allAttrs))
	for _, a := range allAttrs {
		if dir.FileSystem.IsPathAllowed(dir.AbsolutePathForChild(a.Name)) {
			entries = append(entries, fuse.Dirent{
				Inode: a.Inode,
				Name:  a.Name,
				Type:  a.FuseNodeType(),
			})
			// Speculatively pre-create a child node with cached attributes.
			dir.addOrUpdateChildInodeAttrs(ReadDir, a.Name, a)
		}
	}

	if dir.Parent == nil && dir.FileSystem.HasVirtualDirectory() {
		var err error
		entries, err = dir.FileSystem.appendVirtualRootEntries(dir, entries)
		if err != nil {
			return nil, err
		}
	}
	return entries, nil
}

// Performs Stat() query on the backend.
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

// Responds on FUSE Mkdir request.
func (dir *DirINode) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	dir.lockMutex()
	defer dir.unlockMutex()

	targetPath := dir.AbsolutePathForChild(req.Name)

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

// Responds on FUSE Create request.
func (dir *DirINode) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	dir.lockMutex()
	defer dir.unlockMutex()

	req.Mode = ComputePermissions(req.Mode)
	targetPath := dir.AbsolutePathForChild(req.Name)
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

	// update the attributes of the file now.
	_, err = dir.statInodeInHopsFS(Create, file.Attrs.Name, &file.Attrs)
	if err != nil {
		dir.removeChildInode(Create, req.Name)
		return nil, nil, err
	}

	return file, handle, nil
}

// Responds on FUSE Remove request.
func (dir *DirINode) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	dir.lockMutex()
	defer dir.unlockMutex()

	targetPath := dir.AbsolutePathForChild(req.Name)

	logger.Debug("Removing path", logger.Fields{Operation: Remove, Path: targetPath})
	err := dir.FileSystem.getDFSConnector().Remove(targetPath)
	if err == nil {
		dir.removeChildInode(Remove, req.Name)
		if StagingCache != nil {
			StagingCache.Remove(targetPath)
		}
		logger.Info("Removed path", logger.Fields{Operation: Remove, Path: targetPath})
	} else {
		logger.Warn("Failed to remove path", logger.Fields{Operation: Remove, Path: targetPath, Error: err})
	}
	return err
}

// Responds on FUSE Rename request.
func (srcParent *DirINode) Rename(ctx context.Context, req *fuse.RenameRequest, dstParentDir fs.Node) error {
	srcParent.lockMutex()
	defer srcParent.unlockMutex()

	return srcParent.renameInt(Rename, req.OldName, req.NewName, dstParentDir, hdfs.RENAME_OPTION_NONE)
}

func (srcParent *DirINode) renameInt(operationName, oldName, newName string, dstParentDir fs.Node, options hdfs.RenameOptions) error {
	oldPath := srcParent.AbsolutePathForChild(oldName)
	newPath := dstParentDir.(*DirINode).AbsolutePathForChild(newName)
	logger.Debug("Renaming", logger.Fields{Operation: operationName, From: oldPath, To: newPath})

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

	if StagingCache != nil {
		StagingCache.Rename(oldPath, newPath)
	}

	if srcInode != nil {
		srcParent.removeChildInode(Rename, oldName)
	}

	if dstInode != nil {
		dstParentDir.(*DirINode).removeChildInode(Rename, newName)
	}

	dstParentDir.(*DirINode).removeNegativeCacheEntry(newName)

	if fnode, ok := (srcInode).(*FileINode); ok {
		logger.Trace("Rename src is file", logger.Fields{Operation: operationName, From: oldPath, To: newPath})
		fnode.Attrs.Name = newName
		fnode.Parent = dstParentDir.(*DirINode)
		dstParentDir.(*DirINode).adoptChildInode(Rename, newName, fnode)
	}

	if dnode, ok := (srcInode).(*DirINode); ok {
		logger.Trace("Rename src is dir", logger.Fields{Operation: operationName, From: oldPath, To: newPath})
		dnode.Attrs.Name = newName
		dnode.Parent = dstParentDir.(*DirINode)
		dstParentDir.(*DirINode).adoptChildInode(Rename, newName, dnode)
	}

	logger.Info("Renamed", logger.Fields{Operation: operationName, From: oldPath, To: newPath})
	return nil
}

// Responds on FUSE Rename request.
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

// Responds on FUSE Chmod request.
func (dir *DirINode) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	dir.lockMutex()
	defer dir.unlockMutex()

	path := dir.AbsolutePath()

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
