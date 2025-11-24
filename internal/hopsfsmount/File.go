// Copyright (c) Microsoft. All rights reserved.
// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package hopsfsmount

import (
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"sync"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
	"golang.org/x/sys/unix"
	"hopsworks.ai/hopsfsmount/internal/hopsfsmount/logger"
)

type FileINode struct {
	FileSystem *FileSystem // pointer to the FieSystem which owns this file
	Attrs      Attrs       // Cache of file attributes // TODO: implement TTL
	Parent     *DirINode   // Pointer to the parent directory (allows computing fully-qualified paths on demand)

	activeHandles   []*FileHandle // list of opened file handles
	fileMutex       sync.Mutex    // mutex for file operation such as open, delete
	fileProxy       FileProxy     // file proxy. Could be LocalRWFileProxy or RemoteFileProxy
	fileHandleMutex sync.Mutex    // mutex for file handle
	dataMutex       sync.Mutex    // mutex for file handle
}

// Verify that *File implements necesary FUSE interfaces
var _ fs.Node = (*FileINode)(nil)
var _ fs.NodeOpener = (*FileINode)(nil)
var _ fs.NodeFsyncer = (*FileINode)(nil)
var _ fs.NodeSetattrer = (*FileINode)(nil)
var _ fs.NodeForgetter = (*FileINode)(nil)

// Retuns absolute path of the file in HDFS namespace
func (file *FileINode) AbsolutePath() string {
	return path.Join(file.Parent.AbsolutePath(), file.Attrs.Name)
}

// Responds to the FUSE file attribute request
func (file *FileINode) Attr(ctx context.Context, a *fuse.Attr) error {
	// Check fileProxy under fileHandleMutex to avoid race with closeStaging()
	file.lockFileHandles()
	lrwfp, hasLocalProxy := file.fileProxy.(*LocalRWFileProxy)
	if hasLocalProxy {
		// Keep lock held while we stat to prevent close race
		fileInfo, err := lrwfp.localFile.Stat()
		file.unlockFileHandles()
		if err != nil {
			logger.Warn("stat failed on staging file", logger.Fields{Operation: GetattrFile, Path: file.AbsolutePath(), Error: err})
			return err
		}
		// update the local cache under fileMutex
		file.lockFile()
		file.Attrs.Size = uint64(fileInfo.Size())
		file.Attrs.Mtime = fileInfo.ModTime()
		err = file.Attrs.ConvertAttrToFuse(a)
		file.unlockFile()
		return err
	}
	file.unlockFileHandles()

	// No local proxy - use cache or fetch from backend
	file.lockFile()
	defer file.unlockFile()

	if file.FileSystem.Clock.Now().After(file.Attrs.Expires) {
		_, err := file.Parent.statInodeInHopsFS(GetattrFile, file.Attrs.Name, &file.Attrs)
		if err != nil {
			return err
		}
	} else {
		logger.Info("Stat successful. Returning from Cache ", logger.Fields{Operation: GetattrFile, Path: file.AbsolutePath(), FileSize: file.Attrs.Size, IsDir: file.Attrs.Mode.IsDir(), IsRegular: file.Attrs.Mode.IsRegular()})
	}
	return file.Attrs.ConvertAttrToFuse(a)
}

// Responds to the FUSE file open request (creates new file handle)
func (file *FileINode) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	file.lockFile()
	defer file.unlockFile()

	logger.Debug("Opening file", logger.Fields{Operation: Open, Path: file.AbsolutePath(), Flags: req.Flags, FileSize: file.Attrs.Size})
	handle, err := file.NewFileHandle(true, req.Flags)
	if err != nil {
		return nil, err
	}

	// if page cache is not enabled then read directly from HopsFS
	if !EnablePageCache {
		resp.Flags = fuse.OpenDirectIO
	}

	resp.Handle = fuse.HandleID(handle.fhID)

	file.AddHandle(handle)
	return handle, nil
}

// Registers an opened file handle
func (file *FileINode) AddHandle(handle *FileHandle) {
	file.lockFileHandles()
	defer file.unlockFileHandles()
	file.activeHandles = append(file.activeHandles, handle)
}

// Unregisters an opened file handle
func (file *FileINode) RemoveHandle(handle *FileHandle) {
	// Only need fileHandleMutex to modify activeHandles and close fileProxy
	// Don't use fileMutex to avoid lock ordering issues (dataMutex -> fileMutex)
	file.lockFileHandles()
	defer file.unlockFileHandles()

	for i, h := range file.activeHandles {
		if h == handle {
			file.activeHandles = append(file.activeHandles[:i], file.activeHandles[i+1:]...)
			break
		}
	}

	//close the staging file if it is the last handle
	if len(file.activeHandles) == 0 {
		file.closeStaging()
	} else {
		logger.Trace("Staging file is not closed.", file.logInfo(logger.Fields{Operation: Close}))
	}
}

// close staging file
func (file *FileINode) closeStaging() {
	if file.fileProxy != nil { // if not already closed
		err := file.fileProxy.Close()
		if err != nil {
			logger.Error("Failed to close staging file", file.logInfo(logger.Fields{Operation: Close, Error: err}))
		}
		file.fileProxy = nil
		logger.Info("Staging file is closed", file.logInfo(logger.Fields{Operation: Close}))
	}
}

// Responds to the FUSE Fsync request
func (file *FileINode) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	// Take a snapshot of handles and release lock before dispatching
	// This avoids deadlock when handle methods call upgradeHandleForWriting()
	file.lockFileHandles()
	handles := make([]*FileHandle, len(file.activeHandles))
	copy(handles, file.activeHandles)
	file.unlockFileHandles()

	logger.Info(fmt.Sprintf("Dispatching fsync request to all open handles: %d", len(handles)), logger.Fields{Operation: Fsync})

	var retErr error
	for _, handle := range handles {
		err := handle.Fsync(ctx, req)
		if err != nil {
			retErr = err
		}
	}
	return retErr
}

// Invalidates metadata cache, so next ls or stat gives up-to-date file attributes
func (file *FileINode) InvalidateMetadataCache() {
	logger.Debug("InvalidateMetadataCache ", file.logInfo(logger.Fields{}))
	file.Attrs.Expires = file.FileSystem.Clock.Now().Add(-1 * time.Second)
}

// Responds on FUSE Chmod request
func (file *FileINode) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	logger.Debug("Setattr request received: ", logger.Fields{Operation: Setattr})

	if req.Valid.Size() {
		// Take a snapshot of handles and release lock before dispatching
		// This avoids deadlock when handle methods call upgradeHandleForWriting()
		file.lockFileHandles()
		handles := make([]*FileHandle, len(file.activeHandles))
		copy(handles, file.activeHandles)
		file.unlockFileHandles()

		logger.Info(fmt.Sprintf("Dispatching truncate request to all open handles: %d", len(handles)), logger.Fields{Operation: Setattr})
		var err_out error = nil
		for _, handle := range handles {
			err := handle.Truncate(int64(req.Size))
			if err != nil {
				err_out = err
			}
			resp.Attr.Size = req.Size
			file.Attrs.Size = req.Size
		}
		return err_out
	}

	// For other setattr operations, use fileMutex to protect metadata
	file.lockFile()
	defer file.unlockFile()

	path := file.AbsolutePath()

	if req.Valid.Mode() {
		if err := ChmodOp(&file.Attrs, file.FileSystem, path, req, resp); err != nil {
			return err
		}
	}

	if req.Valid.Uid() || req.Valid.Gid() {
		if err := SetAttrChownOp(&file.Attrs, file.FileSystem, path, req, resp); err != nil {
			return err
		}
	}

	if err := UpdateTS(&file.Attrs, file.FileSystem, path, req, resp); err != nil {
		return err
	}

	return nil
}

// Responds on FUSE request to forget inode
func (file *FileINode) Forget() {
	file.lockFile()
	defer file.unlockFile()
	// see comment in Dir.go for Forget handler
	// ask parent to remove me from the children list
	// logger.Debug(fmt.Sprintf("Forget for file %s", file.Attrs.Name), nil)
	// file.Parent.removeChildInode(Forget, file.Attrs.Name)
}

func (file *FileINode) countActiveHandles() int {
	file.lockFileHandles()
	file.unlockFileHandles()
	return len(file.activeHandles)
}

func (file *FileINode) createStagingFile(operation string, existsInDFS bool) (*os.File, error) {
	if file.fileProxy != nil {
		return nil, nil // there is already an active handle.
	}

	//create staging file
	absPath := file.AbsolutePath()
	hdfsAccessor := file.FileSystem.getDFSConnector()
	if !existsInDFS { // it  is a new file so create it in the DFS
		w, err := hdfsAccessor.CreateFile(absPath, ComputePermissions(file.Attrs.Mode), false)
		if err != nil {
			logger.Error("Failed to create file in DFS", file.logInfo(logger.Fields{Operation: operation, Error: err}))
			return nil, err
		}
		logger.Info("Created an empty file in DFS", file.logInfo(logger.Fields{Operation: operation}))
		w.Close()
	} else {
		// Request to write to existing file
		_, err := hdfsAccessor.Stat(absPath)
		if err != nil {
			logger.Error("Failed to stat file in DFS", file.logInfo(logger.Fields{Operation: operation, Error: err}))
			return nil, syscall.ENOENT
		}
	}

	stagingFile, err := ioutil.TempFile(StagingDir, "stage")
	if err != nil {
		logger.Error("Failed to create staging file", file.logInfo(logger.Fields{Operation: operation, Error: err}))
		return nil, err
	}
	os.Remove(stagingFile.Name())
	logger.Info("Created staging file", file.logInfo(logger.Fields{Operation: operation, TmpFile: stagingFile.Name()}))

	if existsInDFS {
		if err := file.downloadToStaging(stagingFile, operation); err != nil {
			return nil, err
		}
	}
	return stagingFile, nil
}

func (file *FileINode) downloadToStaging(stagingFile *os.File, operation string) error {
	hdfsAccessor := file.FileSystem.getDFSConnector()
	absPath := file.AbsolutePath()

	reader, err := hdfsAccessor.OpenRead(absPath)
	if err != nil {
		logger.Error("Failed to open file in DFS", file.logInfo(logger.Fields{Operation: operation, Error: err}))
		// TODO remove the staging file if there are no more active handles
		return err
	}

	nc, err := io.Copy(stagingFile, reader)
	if err != nil {
		logger.Error("Failed to copy content to staging file", file.logInfo(logger.Fields{Operation: operation, Error: err}))
		return err
	}
	reader.Close()
	logger.Info(fmt.Sprintf("Downloaded a copy to stating dir. %d bytes copied", nc), file.logInfo(logger.Fields{Operation: operation}))
	return nil
}

// Creates new file handle
func (file *FileINode) NewFileHandle(existsInDFS bool, flags fuse.OpenFlags) (*FileHandle, error) {
	file.lockFileHandles()
	defer file.unlockFileHandles()

	fh := &FileHandle{File: file, fileFlags: flags, fhID: rand.Uint64()}
	operation := Create
	if existsInDFS {
		operation = Open
	}

	if operation == Create {
		// there must be no existing file handles for create operation
		if file.fileProxy != nil {
			logger.Panic("Unexpected file state during creation", file.logInfo(logger.Fields{Flags: flags}))
		}
		if err := file.checkDiskSpace(); err != nil {
			return nil, err
		}
		stagingFile, err := file.createStagingFile(operation, existsInDFS)
		if err != nil {
			return nil, err
		}
		fh.File.fileProxy = &LocalRWFileProxy{localFile: stagingFile, file: file}
		logger.Info("Opened file, RW handle", fh.logInfo(logger.Fields{Operation: operation, Flags: fh.fileFlags}))
	} else {
		if file.fileProxy != nil {
			fh.File.fileProxy = file.fileProxy
			logger.Info("Opened file, Returning existing handle", fh.logInfo(logger.Fields{Operation: operation, Flags: fh.fileFlags}))
		} else {
			// we alway open the file in RO mode. when the client writes to the file
			// then we upgrade the handle. However, if the file is already opened in
			// in RW state then we use the existing RW handle
			reader, err := file.FileSystem.getDFSConnector().OpenRead(file.AbsolutePath())
			if err != nil {
				logger.Warn("Opening file failed", fh.logInfo(logger.Fields{Operation: operation, Flags: fh.fileFlags, Error: err}))
				return nil, err
			} else {
				fh.File.fileProxy = &RemoteROFileProxy{hdfsReader: reader, file: file}
				logger.Info("Opened file, RO handle", fh.logInfo(logger.Fields{Operation: operation, Flags: fh.fileFlags}))
			}
		}
	}

	return fh, nil
}

// changes RO file handle to RW
func (file *FileINode) upgradeHandleForWriting(me *FileHandle, operation string) error {
	file.lockFileHandles()
	defer file.unlockFileHandles()

	var upgrade = false
	if _, ok := file.fileProxy.(*LocalRWFileProxy); ok {
		upgrade = false
	} else if _, ok := file.fileProxy.(*RemoteROFileProxy); ok {
		upgrade = true
	} else {
		logger.Panic("Unrecognized remote file proxy", nil)
	}

	if !upgrade {
		return nil
	} else {

		logger.Info(fmt.Sprintf("Upgrading file handle for writing. Active handles %d", len(file.activeHandles)), file.logInfo(logger.Fields{Operation: operation}))

		// No need to lock other handles - they're all blocked by the same dataMutex
		// that the caller (Write/Truncate) already holds

		remoteROFileProxy, _ := file.fileProxy.(*RemoteROFileProxy)
		remoteROFileProxy.hdfsReader.Close() // close this read only handle
		file.fileProxy = nil

		if err := file.checkDiskSpace(); err != nil {
			return err
		}

		stagingFile, err := file.createStagingFile("Open", true)
		if err != nil {
			return err
		}

		file.fileProxy = &LocalRWFileProxy{localFile: stagingFile, file: file}
		logger.Info("Open handle upgrade to support RW ", file.logInfo(logger.Fields{Operation: operation}))
		return nil
	}
}

func (file *FileINode) checkDiskSpace() error {
	var stat unix.Statfs_t
	wd, err := os.Getwd()
	if err != nil {
		return err
	}
	unix.Statfs(wd, &stat)
	// Available blocks * size per block = available space in bytes
	bytesAvailable := stat.Bavail * uint64(stat.Bsize)
	if bytesAvailable < 64*1024*1024 {
		return syscall.ENOSPC
	} else {
		return nil
	}
}

func (file *FileINode) logInfo(fields logger.Fields) logger.Fields {
	f := logger.Fields{Path: file.AbsolutePath()}
	for k, e := range fields {
		f[k] = e
	}
	return f
}

func (file *FileINode) lockFileHandles() {
	file.fileHandleMutex.Lock()
}

func (file *FileINode) unlockFileHandles() {
	file.fileHandleMutex.Unlock()
}

func (file *FileINode) lockFile() {
	file.fileMutex.Lock()
}

func (file *FileINode) unlockFile() {
	file.fileMutex.Unlock()
}
