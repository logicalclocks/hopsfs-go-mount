// Copyright (c) Microsoft. All rights reserved.
// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package hopsfsmount

import (
	"fmt"
	"strconv"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"hopsworks.ai/hopsfsmount/internal/hopsfsmount/logger"

	"golang.org/x/net/context"

	"io"
	"os"
	"os/exec"
	"os/user"
	"strings"
	"sync"
)

type FileSystem struct {
	HdfsAccessors       []HdfsAccessor // Interface to access HDFS
	hdfsAccessorsIndex  int
	SrcDir              string       // Src directory that will mounted
	AllowedPrefixes     []string     // List of allowed path prefixes (only those prefixes are exposed via mountpoint)
	ReadOnly            bool         // Indicates whether mount filesystem with readonly
	DelaySyncUntilClose bool         // If true, ignore sync/flush operations until file close
	Mounted             bool         // True if filesystem is mounted
	RetryPolicy         *RetryPolicy // Retry policy
	Clock               Clock        // interface to get wall clock time
	FsInfo              FsInfo       // Usage of HDFS, including capacity, remaining, used sizes.

	closeOnUnmount     []io.Closer // list of opened files (zip archives) to be closed on unmount
	closeOnUnmountLock sync.Mutex  // mutex to protet closeOnUnmount
}

// Verify that *FileSystem implements necesary FUSE interfaces
var _ fs.FS = (*FileSystem)(nil)
var _ fs.FSStatfser = (*FileSystem)(nil)

// Creates an instance of mountable file system
func NewFileSystem(hdfsAccessors []HdfsAccessor, srcDir string, allowedPrefixes []string, readOnly bool, delaySyncUntilClose bool, retryPolicy *RetryPolicy, clock Clock) (*FileSystem, error) {
	return &FileSystem{
		HdfsAccessors:       hdfsAccessors,
		Mounted:             false,
		AllowedPrefixes:     allowedPrefixes,
		ReadOnly:            readOnly,
		DelaySyncUntilClose: delaySyncUntilClose,
		RetryPolicy:         retryPolicy,
		Clock:               clock,
		SrcDir:              srcDir}, nil
}

// Mounts the filesystem
func (filesystem *FileSystem) Mount(mountPoint string, conf ...fuse.MountOption) (*fuse.Conn, error) {
	var conn *fuse.Conn
	var err error
	conn, err = fuse.Mount(
		mountPoint,
		conf...,
	)
	if err != nil {
		return nil, err
	}
	filesystem.Mounted = true
	return conn, nil
}

// Unmounts the filesysten (invokes fusermount tool)
func (filesystem *FileSystem) Unmount(mountPoint string) {
	if !filesystem.Mounted {
		return
	}
	filesystem.Mounted = false
	logger.Info("Unmounting...", nil)
	cmd := exec.Command("fusermount3", "-zu", mountPoint)
	err := cmd.Run()

	// Closing all the files
	filesystem.closeOnUnmountLock.Lock()
	defer filesystem.closeOnUnmountLock.Unlock()
	for _, f := range filesystem.closeOnUnmount {
		f.Close()
	}

	if err != nil {
		logger.Fatal(fmt.Sprintf("Unable to unmount FS. Error: %v", err), nil)
	}
}

// Returns root directory of the filesystem
func (filesystem *FileSystem) Root() (fs.Node, error) {
	//get UID and GID for the current user
	cu, err := user.Current()
	if err != nil {
		logger.Fatal(fmt.Sprintf("Faile to get current user information. Error: %v", err), nil)
	}
	uid64, _ := strconv.ParseUint(cu.Uid, 10, 32)
	gid64, _ := strconv.ParseUint(cu.Gid, 10, 32)

	return &DirINode{FileSystem: filesystem, Parent: nil, Attrs: Attrs{
		Inode: 1,
		Uid:   uint32(uid64),
		Gid:   uint32(gid64),
		Mode:  0755 | os.ModeDir,
		Mtime: filesystem.Clock.Now(),
		Ctime: filesystem.Clock.Now()},
	}, nil
}

// Returns if given absoute path allowed by any of the prefixes
func (filesystem *FileSystem) IsPathAllowed(path string) bool {
	if path == "/" {
		return true
	}
	for _, prefix := range filesystem.AllowedPrefixes {
		if prefix == "*" {
			return true
		}
		p := "/" + prefix
		if p == path || strings.HasPrefix(path, p+"/") {
			return true
		}
	}
	return false
}

// Register a file to be closed on Unmount()
func (filesystem *FileSystem) CloseOnUnmount(file io.Closer) {
	filesystem.closeOnUnmountLock.Lock()
	defer filesystem.closeOnUnmountLock.Unlock()
	filesystem.closeOnUnmount = append(filesystem.closeOnUnmount, file)
}

// Statfs is called to obtain file system metadata.
// It should write that data to resp.
func (filesystem *FileSystem) Statfs(ctx context.Context, req *fuse.StatfsRequest, resp *fuse.StatfsResponse) error {
	fsInfo, err := filesystem.getDFSConnector().StatFs()
	if err != nil {
		logger.Warn("Stat DFS failed", logger.Fields{Operation: StatFS, Error: err})
		return err
	}
	resp.Bsize = 1024
	resp.Bfree = fsInfo.remaining / uint64(resp.Bsize)
	resp.Bavail = resp.Bfree
	resp.Blocks = fsInfo.capacity / uint64(resp.Bsize)
	return nil
}

func (filesystem *FileSystem) getDFSConnector() HdfsAccessor {
	filesystem.hdfsAccessorsIndex = filesystem.hdfsAccessorsIndex + 1
	index := filesystem.hdfsAccessorsIndex % len(filesystem.HdfsAccessors)
	return filesystem.HdfsAccessors[index]
}
