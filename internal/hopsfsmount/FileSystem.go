// Copyright (c) Microsoft. All rights reserved.
// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package hopsfsmount

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"hopsworks.ai/hopsfsmount/internal/hopsfsmount/logger"

	"golang.org/x/net/context"

	"io"
	"os"
	"os/exec"
	"os/user"
	"sync"
)

type FileSystem struct {
	HdfsAccessors       []HdfsAccessor // Interface to access HDFS
	hdfsAccessorsIndex  int            // Round-robin index over HdfsAccessors
	hdfsAccessorsLock   sync.Mutex     // guards HdfsAccessors and hdfsAccessorsIndex
	maxHdfsAccessors    int            // HdfsAccessors grows on demand up to this, when newHdfsAccessor is set
	newHdfsAccessor     func() (HdfsAccessor, error)
	SrcDir              string                   // Src directory that will mounted
	AllowedPrefixes     []string                 // List of allowed path prefixes (only those prefixes are exposed via mountpoint)
	VirtualDirectories  []VirtualDirectoryConfig // Configured virtual directories exposed as synthetic entries at the mount root
	ReadOnly            bool                     // Indicates whether mount filesystem with readonly
	DelaySyncUntilClose bool                     // If true, ignore sync/flush operations until file close
	Mounted             bool                     // True if filesystem is mounted
	RetryPolicy         *RetryPolicy             // Retry policy for failed backend operations
	Clock               Clock                    // interface to get wall clock time
	FsInfo              FsInfo                   // Usage of HDFS, including capacity, remaining, used sizes.

	closeOnUnmount            []io.Closer     // list of opened files (zip archives) to be closed on unmount
	closeOnUnmountLock        sync.Mutex      // mutex to protect closeOnUnmount
	virtualRootCollisions     map[string]bool // Configured virtual root names that collide with a real backend entry at the mount root
	virtualRootCollisionsLock sync.RWMutex    // guards virtualRootCollisions
}

// Verify that *FileSystem implements necesary FUSE interfaces
var _ fs.FS = (*FileSystem)(nil)
var _ fs.FSStatfser = (*FileSystem)(nil)

// Creates an instance of mountable file system
func NewFileSystem(hdfsAccessors []HdfsAccessor, srcDir string, allowedPrefixes []string, readOnly bool, delaySyncUntilClose bool, retryPolicy *RetryPolicy, clock Clock, opts ...FileSystemOption) (*FileSystem, error) {
	filesystem := &FileSystem{
		HdfsAccessors:       hdfsAccessors,
		Mounted:             false,
		AllowedPrefixes:     allowedPrefixes,
		ReadOnly:            readOnly,
		DelaySyncUntilClose: delaySyncUntilClose,
		RetryPolicy:         retryPolicy,
		Clock:               clock,
		SrcDir:              srcDir}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(filesystem)
	}
	if err := filesystem.normalizeVirtualDirectoryConfig(); err != nil {
		return nil, err
	}
	return filesystem, nil
}

// WithExtraConnectors lets the file system add up to extra connectors, made
// by newConnector, whenever every connector it has is busy. A connector is
// busy for the whole of a call, and a call under a subtree operation waits
// inside the client for as long as the operation runs, so without spare
// connectors one such call holds up every other operation on the mount.
// An added connector stays, its namenode connection open, for the life of
// the mount: the namenode closes idle connections only once it holds
// ipc.client.idlethreshold (4000) of them.
func WithExtraConnectors(extra int, newConnector func() (HdfsAccessor, error)) FileSystemOption {
	return func(filesystem *FileSystem) {
		filesystem.maxHdfsAccessors = len(filesystem.HdfsAccessors) + extra
		filesystem.newHdfsAccessor = newConnector
	}
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

	return newDirINode(filesystem, nil, Attrs{
		Inode: 1,
		Uid:   uint32(uid64),
		Gid:   uint32(gid64),
		Mode:  0755 | os.ModeDir,
		Mtime: filesystem.Clock.Now(),
		Ctime: filesystem.Clock.Now(),
	}), nil
}

// Returns if given absoute path allowed by any of the prefixes
func (filesystem *FileSystem) IsPathAllowed(candidate string) bool {
	if candidate == "/" {
		return true
	}
	for _, prefix := range filesystem.AllowedPrefixes {
		if prefix == "*" {
			return true
		}
		p := "/" + prefix
		if p == candidate || strings.HasPrefix(candidate, p+"/") {
			return true
		}
	}

	for _, virtualDirectory := range filesystem.VirtualDirectories {
		if virtualDirectory.isPathAllowed(candidate) {
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

// A request waiting for a connector to free scans the pool again after these
// waits, doubling from the first to the last, so it picks up a freed
// connector within maxConnectorWait without spinning on the pool lock.
const (
	minConnectorWait = time.Millisecond
	maxConnectorWait = 10 * time.Millisecond
)

// Returns a connector, round robin, reserved for the caller's next call (see
// TryReserve). When every connector is in a call or reserved, one more is
// added, up to the limit WithExtraConnectors set; at the limit the caller
// waits for a connector to free. Call it where the call is made, as in
// getDFSConnector().Stat(path): a reservation not taken up keeps other
// requests off the connector until it lapses.
func (filesystem *FileSystem) getDFSConnector() HdfsAccessor {
	wait := minConnectorWait
	for {
		filesystem.hdfsAccessorsLock.Lock()
		n := len(filesystem.HdfsAccessors)
		start := filesystem.hdfsAccessorsIndex + 1
		for i := 0; i < n; i++ {
			index := (start + i) % n
			if filesystem.HdfsAccessors[index].TryReserve() {
				filesystem.hdfsAccessorsIndex = index
				connector := filesystem.HdfsAccessors[index]
				filesystem.hdfsAccessorsLock.Unlock()
				return connector
			}
		}
		if filesystem.newHdfsAccessor != nil && n < filesystem.maxHdfsAccessors {
			connector, err := filesystem.newHdfsAccessor()
			if err == nil {
				// Visible to other callers once the lock is released.
				connector.TryReserve()
				filesystem.HdfsAccessors = append(filesystem.HdfsAccessors, connector)
				filesystem.hdfsAccessorsIndex = n
				filesystem.hdfsAccessorsLock.Unlock()
				logger.Info(fmt.Sprintf("All %d connectors are busy, added one: %d of at most %d", n, n+1, filesystem.maxHdfsAccessors), nil)
				return connector
			}
			logger.Warn(fmt.Sprintf("All %d connectors are busy and a new one could not be made", n), logger.Fields{Error: err})
		}
		filesystem.hdfsAccessorsLock.Unlock()
		// Every connector can be waiting out a subtree operation for minutes.
		time.Sleep(wait)
		wait = min(2*wait, maxConnectorWait)
	}
}
