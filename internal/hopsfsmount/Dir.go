// Copyright (c) Microsoft. All rights reserved.
// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package hopsfsmount

import (
	"fmt"
	"github.com/colinmarc/hdfs/v2"
	"os"
	"path"
	"sync"
	"syscall"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
	"hopsworks.ai/hopsfsmount/internal/hopsfsmount/logger"
)

// Encapsulates state and operations for directory node on the HDFS file system
type DirINode struct {
	FileSystem    *FileSystem        // Pointer to the owning filesystem
	Attrs         Attrs              // Cached attributes of the directory, TODO: add TTL
	Parent        *DirINode          // Pointer to the parent directory (allows computing fully-qualified paths on demand)
	children      map[string]fs.Node // Cahed directory entries
	childrenMutex sync.Mutex         // for concurrent read and updates
	dirMutex      sync.Mutex         // One read or write operation on a directory at a time
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

// Returns absolute path of the dir in HDFS namespace
func (dir *DirINode) AbsolutePath() string {
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

	if dir.Parent != nil && dir.FileSystem.Clock.Now().After(dir.Attrs.Expires) {
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

	if node, ok := dir.children[name]; ok {
		if fnode, ok := (node).(*FileINode); ok {
			fnode.Attrs = attrs
		} else if dnode, ok := (node).(*DirINode); ok {
			dnode.Attrs = attrs
		}
		logger.Debug("Children's List. addOrUpdateChildInodeAttrs. Update ", logger.Fields{Operation: operation, Parent: dir.AbsolutePath(), Child: name, NumChildren: len(dir.children)})
		return node
	} else {
		var node fs.Node
		if (attrs.Mode & os.ModeDir) == 0 {
			node = &FileINode{FileSystem: dir.FileSystem, Parent: dir, Attrs: attrs}
		} else {
			node = &DirINode{FileSystem: dir.FileSystem, Parent: dir, Attrs: attrs}
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

func (dir *DirINode) LookupInt(opName string, name string) (fs.Node, error) {
	if !dir.FileSystem.IsPathAllowed(dir.AbsolutePathForChild(name)) {
		return nil, syscall.ENOENT
	}

	if node := dir.getChildInode(opName, name); node != nil {
		return node, nil
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
			// Creating Dirent structure as required by FUSE
			entries = append(entries, fuse.Dirent{
				Inode: a.Inode,
				Name:  a.Name,
				Type:  a.FuseNodeType()})
			// Speculatively pre-creating child Dir or File node with cached attributes,
			// since it's highly likely that we will have Lookup() call for this name
			// This is the key trick which dramatically speeds up 'ls'
			dir.addOrUpdateChildInodeAttrs(ReadDir, a.Name, a)
		}
	}
	return entries, nil
}

// Performs Stat() query on the backend
func (dir *DirINode) statInodeInHopsFS(operation, name string, attrs *Attrs) (fs.Node, error) {

	a, err := dir.FileSystem.getDFSConnector().Stat(path.Join(dir.AbsolutePath(), name))
	if err != nil {
		logger.Info("Stat failed on backend", logger.Fields{Operation: operation, Path: path.Join(dir.AbsolutePath(), name), Error: err})
		dir.removeChildInode(operation, name)
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

	// check user and group information first.
	userName, err := getUserName(req.Uid)
	if err != nil {
		logger.Error("Unable to find user information. ", logger.Fields{Operation: Mkdir,
			Path: dir.AbsolutePathForChild(req.Name), UID: req.Uid, HopsFSUserName: ForceOverrideUsername})
		return nil, err
	}

	groupName, err := getGroupName(dir.AbsolutePathForChild(req.Name), req.Gid)
	if err != nil {
		logger.Error("Unable to find group information. ", logger.Fields{Operation: Mkdir,
			Path: dir.AbsolutePathForChild(req.Name), GID: req.Gid,
			GetGroupFromHopsFSDatasetPath: UseGroupFromHopsFsDatasetPath})
		return nil, err
	}
	req.Mode = ComputePermissions(req.Mode)
	err = dir.FileSystem.getDFSConnector().Mkdir(dir.AbsolutePathForChild(req.Name), req.Mode)
	if err != nil {
		logger.Info("mkdir failed", logger.Fields{Operation: Mkdir, Path: path.Join(dir.AbsolutePath(), req.Name), Error: err})
		return nil, err
	}
	logger.Debug("mkdir successful", logger.Fields{Operation: Mkdir, Path: path.Join(dir.AbsolutePath(), req.Name)})

	err = ChownOp(dir.FileSystem, dir.AbsolutePathForChild(req.Name), userName, groupName)
	if err != nil {
		logger.Warn("Unable to change ownership of new dir", logger.Fields{Operation: Create, Path: dir.AbsolutePathForChild(req.Name),
			UID: req.Uid, GID: req.Gid, Error: err})
		//unable to change the ownership of the directory. so delete it as the operation as a whole failed
		dir.FileSystem.getDFSConnector().Remove(dir.AbsolutePathForChild(req.Name))
		return nil, err
	}

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
	logger.Info("Creating a new file", logger.Fields{Operation: Create, Path: dir.AbsolutePathForChild(req.Name), Mode: req.Mode, Flags: req.Flags})

	// first determine the usename and grup name for the new file
	userName, err := getUserName(req.Uid)
	if err != nil {
		logger.Error("Unable to find user information. ", logger.Fields{Operation: Create,
			Path: dir.AbsolutePathForChild(req.Name), UID: req.Uid, HopsFSUserName: ForceOverrideUsername})
		return nil, nil, err
	}

	groupName, err := getGroupName(dir.AbsolutePathForChild(req.Name), req.Gid)
	if err != nil {
		logger.Error("Unable to find group information. ", logger.Fields{Operation: Create,
			Path: dir.AbsolutePathForChild(req.Name), GID: req.Gid,
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

	file := (dir.addOrUpdateChildInodeAttrs(Create, req.Name, newFileAttrs)).(*FileINode)
	handle, err := file.NewFileHandle(false, req.Flags)
	if err != nil {
		logger.Error("File creation failed", logger.Fields{Operation: Create, Path: dir.AbsolutePathForChild(req.Name), Mode: req.Mode, Flags: req.Flags, Error: err})
		dir.removeChildInode(Create, req.Name)
		return nil, nil, err
	}

	file.AddHandle(handle)
	err = ChownOp(dir.FileSystem, dir.AbsolutePathForChild(req.Name), userName, groupName)
	if err != nil {
		logger.Warn("Unable to change ownership of new file", logger.Fields{Operation: Create, Path: dir.AbsolutePathForChild(req.Name),
			UID: req.Uid, GID: req.Gid, Error: err})
		//unable to change the ownership of the file. so delete it as the operation as a whole failed
		dir.FileSystem.getDFSConnector().Remove(dir.AbsolutePathForChild(req.Name))
		dir.removeChildInode(Create, req.Name)
		return nil, nil, err
	}

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

	path := dir.AbsolutePathForChild(req.Name)
	logger.Debug("Removing path", logger.Fields{Operation: Remove, Path: path})
	err := dir.FileSystem.getDFSConnector().Remove(path)
	if err == nil {
		dir.removeChildInode(Remove, req.Name)
		logger.Info("Removed path", logger.Fields{Operation: Remove, Path: path})
	} else {
		logger.Warn("Failed to remove path", logger.Fields{Operation: Remove, Path: path, Error: err})
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

	// disconnect src inode
	if srcInode != nil {
		srcParent.removeChildInode(Rename, oldName)
	}

	// disconnect dst inode
	if dstInode != nil {
		dstParentDir.(*DirINode).removeChildInode(Rename, newName)
	}

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

	if req.Valid.Size() {
		logger.Error(fmt.Sprintf("Unsupported operation. Can not set size of a directory"), logger.Fields{Operation: Chmod, Path: path})
		return syscall.ENOTSUP
	}

	if req.Valid.Mode() {
		req.Mode = ComputePermissions(req.Mode)
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
