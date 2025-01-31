// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package hopsfsmount

import (
	"os"
	"syscall"
	"time"

	"bazil.org/fuse"
	"hopsworks.ai/hopsfsmount/internal/hopsfsmount/logger"
	"hopsworks.ai/hopsfsmount/internal/hopsfsmount/ugcache"
)

func ChmodOp(attrs *Attrs, fileSystem *FileSystem, path string, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	logger.Info("Setting attributes", logger.Fields{Operation: Chmod, Path: path, Mode: req.Mode})
	err := fileSystem.getDFSConnector().Chmod(path, req.Mode)
	if err != nil {
		return err
	} else {
		attrs.Mode = req.Mode
		resp.Attr.Mode = req.Mode
		return nil
	}
}

func SetAttrChownOp(attrs *Attrs, fileSystem *FileSystem, path string, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {

	var userName = attrs.DFSUserName
	var groupName = attrs.DFSGroupName
	var err error

	if req.Valid.Uid() {
		userName, err = getUserName(req.Uid)
		if err != nil {
			logger.Error("Unable to find user information. ", logger.Fields{Operation: Setattr,
				Path: path, UID: req.Uid, HopsFSUserName: ForceOverrideUsername})
			return err
		}
	}

	if req.Valid.Gid() {
		groupName, err = getGroupName(path, req.Gid)
		if err != nil {
			logger.Error("Unable to find group information. ", logger.Fields{Operation: Setattr,
				Path: path, GID: req.Gid, GetGroupFromHopsFSDatasetPath: UseGroupFromHopsFsDatasetPath})
			return err
		}
	}

	err = ChownOp(fileSystem, path, userName, groupName)
	if err != nil {
		return err
	}

	if req.Valid.Uid() {
		attrs.Uid = req.Uid
		attrs.DFSUserName = userName
	}

	if req.Valid.Gid() {
		attrs.Gid = req.Gid
		attrs.DFSGroupName = groupName
	}
	return nil
}

func ChownOp(fileSystem *FileSystem, path string, userName string, groupName string) error {
	logger.Info("Setting attributes", logger.Fields{Operation: Chown, Path: path, User: userName, Group: groupName})
	return fileSystem.getDFSConnector().Chown(path, userName, groupName)
}

func getUserName(uid uint32) (string, error) {
	var userName = ""
	if ForceOverrideUsername != "" {
		userName = ForceOverrideUsername
	} else {
		userName = ugcache.LookupUserName(uid)
	}

	if userName == "" {
		return "", syscall.EACCES
	}

	return userName, nil
}

func getGroupName(path string, gid uint32) (string, error) {
	var groupName string
	if UseGroupFromHopsFsDatasetPath {
		groupName = getGroupNameFromPath(path)
	} else {
		groupName = ugcache.LookupGroupName(gid)
	}

	if groupName == "" {
		return "", syscall.EACCES
	} else {
		return groupName, nil
	}
}

func UpdateTS(attrs *Attrs, fileSystem *FileSystem, path string, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {

	// in future if we need access time then we can update the file system client to support it
	if req.Valid.Atime() {
		logger.Debug("The stat op in hopsfs client returns os.FileInfo which does not have access time. Ignoring atime for now", nil)
	}

	if req.Valid.Mtime() {
		attrs.Mtime = time.Unix(int64(req.Mtime.Second()), 0)
	}

	if req.Valid.Handle() {
		logger.Warn("Setattr Handle is not implemented yet.", nil)
	}

	if req.Valid.AtimeNow() {
		logger.Debug("Setattr AtimeNow is not implemented yet.", nil)
	}

	if req.Valid.MtimeNow() {
		logger.Debug("Setattr MtimeNow is not implemented yet.", nil)
	}

	if req.Valid.LockOwner() {
		logger.Warn("Setattr LockOwner is not implemented yet.", nil)
	}

	return nil
}

func getGroupNameFromPath(path string) string {
	logger.Debug("Getting group name from path", logger.Fields{Path: path})
	result := HopfsProjectDatasetGroupRegex.FindAllStringSubmatch(path, -1)
	if len(result) == 0 {
		return ""
	}

	return result[0][1] + "__" + result[0][2]
}

func ComputePermissions(defaultPerm os.FileMode) os.FileMode {
	if UserUmask == "" {
		return defaultPerm
	}
	var fullPermissions = os.FileMode(0777)
	mode := fullPermissions &^ Umask
	// Update execute permissions for owner, group, and others
	// The executable bit is only set if the given umask allows it and the default permissions have it set
	mode = updateExecutableBit(mode, defaultPerm, 0100, Umask&0100 == 0) // Owner
	mode = updateExecutableBit(mode, defaultPerm, 0010, Umask&0010 == 0) // Group
	mode = updateExecutableBit(mode, defaultPerm, 0001, Umask&0001 == 0) // Others
	if defaultPerm.IsDir() {
		return mode | os.ModeDir
	}
	return mode
}

func updateExecutableBit(mode, defaultPerm os.FileMode, bit os.FileMode, umaskAllows bool) os.FileMode {
	if umaskAllows && defaultPerm&bit != 0 {
		mode |= bit
	} else {
		mode &^= bit
	}
	return mode
}
