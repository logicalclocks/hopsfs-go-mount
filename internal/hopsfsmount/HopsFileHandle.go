// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package hopsfsmount

import (
	"io"
	"sync"
	"syscall"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
	"hopsworks.ai/hopsfsmount/internal/hopsfsmount/logger"
)

// Represents a handle to an open file
type FileHandle struct {
	File              *FileINode
	mutex             sync.Mutex     // all operations on the handle are serialized to simplify invariants
	fileFlags         fuse.OpenFlags // flags used to creat the file
	tatalBytesRead    int64
	totalBytesWritten int64
	fhID              uint64 // file handle id. for debugging only
}

// Verify that *FileHandle implements necesary FUSE interfaces
var _ fs.Node = (*FileHandle)(nil)
var _ fs.HandleReader = (*FileHandle)(nil)
var _ fs.HandleReleaser = (*FileHandle)(nil)
var _ fs.HandleWriter = (*FileHandle)(nil)
var _ fs.NodeFsyncer = (*FileHandle)(nil)
var _ fs.HandleFlusher = (*FileHandle)(nil)
var _ fs.HandlePoller = (*FileHandle)(nil)

func (fh *FileHandle) dataChanged() bool {
	if fh.totalBytesWritten > 0 {
		return true
	} else {
		return false
	}
}

// Lock order: dataMutex (3) → fileHandleMutex (1) via upgradeHandleForWriting
func (fh *FileHandle) Truncate(size int64) error {
	fh.File.lockData()
	defer fh.File.unlockData()

	// as an optimization the file is initially opened in readonly mode
	fh.File.upgradeHandleForWriting(fh, Truncate)

	sizeChanged, err := fh.File.fileProxy.Truncate(size)
	if err != nil {
		logger.Error("Failed to truncate file", fh.logInfo(logger.Fields{Operation: Truncate, Bytes: size, Error: err}))
		return err
	}

	fh.totalBytesWritten += sizeChanged

	// Mark file as dirty (protected by dataMutex we're holding)
	fh.File.markDirty()

	logger.Info("Truncated file", fh.logInfo(logger.Fields{Operation: Truncate, Bytes: size}))
	return nil
}

// Returns attributes of the file associated with this handle
// Delegates to File.Attr which handles its own locking (fileMutex → fileHandleMutex)
// Note: No dataMutex here to avoid invalid lock order dataMutex (3) → fileMutex (2)
func (fh *FileHandle) Attr(ctx context.Context, a *fuse.Attr) error {
	return fh.File.Attr(ctx, a)
}

// Responds to FUSE Read request
// Lock order: dataMutex (3) alone
func (fh *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	fh.File.lockData()
	defer fh.File.unlockData()

	buf := resp.Data[0:req.Size]
	nr, err := fh.File.fileProxy.ReadAt(buf, req.Offset)
	resp.Data = buf[0:nr]
	fh.tatalBytesRead += int64(nr)

	if err != nil {
		if err == io.EOF {
			logger.Debug("Completed reading", fh.logInfo(logger.Fields{Operation: Read, Error: err, Bytes: nr}))
			if nr >= 0 {
				return nil
			} else {
				return err
			}
		} else {
			logger.Error("Failed to read", fh.logInfo(logger.Fields{Operation: Read, Error: err, Bytes: nr}))
			return err
		}
	}
	return err
}

// Responds to FUSE Write request
// Lock order: dataMutex (3) → fileHandleMutex (1) via upgradeHandleForWriting
func (fh *FileHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	fh.File.lockData()
	defer fh.File.unlockData()

	// as an optimization the file is initially opened in readonly mode
	fh.File.upgradeHandleForWriting(fh, Write)

	nw, err := fh.File.fileProxy.WriteAt(req.Data, req.Offset)
	resp.Size = nw
	fh.totalBytesWritten += int64(nw)
	if err != nil {
		logger.Error("Failed to write to staging file", fh.logInfo(logger.Fields{Operation: Write, Error: err}))
		return err
	}

	// Mark file as dirty (protected by dataMutex we're holding)
	fh.File.markDirty()

	logger.Debug("Write data to staging file", fh.logInfo(logger.Fields{Operation: Write, Bytes: nw, ReqOffset: req.Offset}))
	return nil
}

// Responds to the FUSE Flush request.
// IMPORTANT: Data must be uploaded to DFS in Flush, not in Release (close).
// The FUSE library handles each request in a separate goroutine, and the kernel
// does not wait for Release to complete before returning from the close() syscall.
// Only Flush is synchronous - the kernel waits for Flush to complete before
// returning from close(). If we defer upload to Release, subsequent file operations
// may start before the previous file's upload is complete.
// Lock order: dataMutex (3) → fileHandleMutex (1) via flushToDFS
func (fh *FileHandle) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	fh.File.lockData()
	defer fh.File.unlockData()
	return fh.File.flushToDFS(Flush)
}

// Responds to the FUSE Fsync request
// Lock order: dataMutex (3) → fileHandleMutex (1) via flushToDFS
func (fh *FileHandle) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	// If delaySyncUntilClose is enabled, skip fsync until file close
	if fh.File.FileSystem.DelaySyncUntilClose {
		logger.Debug("Fsync deferred until close", fh.logInfo(logger.Fields{Operation: Fsync}))
		return nil
	}

	fh.File.lockData()
	defer fh.File.unlockData()
	return fh.File.flushToDFS(Fsync)
}

// Closes the handle
// Lock order: dataMutex (3) → fileHandleMutex (1) via flushToDFS and RemoveHandle
func (fh *FileHandle) Release(_ context.Context, _ *fuse.ReleaseRequest) error {
	fh.File.lockData()
	defer fh.File.unlockData()

	// Flush any dirty data before closing
	err := fh.File.flushToDFS(Close)
	if err != nil {
		logger.Error("Failed to flush data on close", fh.logInfo(logger.Fields{Operation: Close, Error: err}))
		// Continue with close even if flush fails
	}

	//close the file handle if it is the last handle
	fh.File.InvalidateMetadataCache()
	fh.File.RemoveHandle(fh)

	logger.Info("Closed file handle ", fh.logInfo(logger.Fields{Operation: Close, Flags: fh.fileFlags, TotalBytesRead: fh.tatalBytesRead, TotalBytesWritten: fh.totalBytesWritten}))
	return err // Return flush error if any
}

func (fh *FileHandle) Poll(ctx context.Context, req *fuse.PollRequest, resp *fuse.PollResponse) error {
	logger.Warn("Polling is not supported ", fh.logInfo(logger.Fields{Operation: Poll}))
	return syscall.ENOSYS
}

func (fh *FileHandle) logInfo(fields logger.Fields) logger.Fields {
	f := logger.Fields{FileHandleID: fh.fhID, Path: fh.File.AbsolutePath()}
	for k, e := range fields {
		f[k] = e
	}
	return f
}
