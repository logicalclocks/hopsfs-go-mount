// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package hopsfsmount

import (
	"fmt"
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

func (fh *FileHandle) Truncate(size int64) error {
	fh.lockHandle()
	defer fh.unlockHandle()

	// as an optimization the file is initially opened in readonly mode
	fh.File.upgradeHandleForWriting(fh, Truncate)

	sizeChanged, err := fh.File.fileProxy.Truncate(size)
	if err != nil {
		logger.Error("Failed to truncate file", fh.logInfo(logger.Fields{Operation: Truncate, Bytes: size, Error: err}))
		return err
	}

	fh.totalBytesWritten += sizeChanged

	logger.Info("Truncated file", fh.logInfo(logger.Fields{Operation: Truncate, Bytes: size}))
	return nil
}

// Returns attributes of the file associated with this handle
func (fh *FileHandle) Attr(ctx context.Context, a *fuse.Attr) error {
	fh.lockHandle()
	defer fh.unlockHandle()
	return fh.File.Attr(ctx, a)
}

// Responds to FUSE Read request
func (fh *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	fh.lockHandle()
	defer fh.unlockHandle()

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
func (fh *FileHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	fh.lockHandle()
	defer fh.unlockHandle()

	// as an optimization the file is initially opened in readonly mode
	fh.File.upgradeHandleForWriting(fh, Write)

	nw, err := fh.File.fileProxy.WriteAt(req.Data, req.Offset)
	resp.Size = nw
	fh.totalBytesWritten += int64(nw)
	if err != nil {
		logger.Error("Failed to write to staging file", fh.logInfo(logger.Fields{Operation: Write, Error: err}))
		return err
	} else {
		logger.Debug("Write data to staging file", fh.logInfo(logger.Fields{Operation: Write, Bytes: nw, ReqOffset: req.Offset}))
		return nil
	}
}

func (fh *FileHandle) copyToDFS(operation string) error {
	if fh.totalBytesWritten == 0 { // Nothing to do
		return nil
	}
	defer fh.File.InvalidateMetadataCache()

	logger.Debug("Uploading to DFS", fh.logInfo(logger.Fields{Operation: operation, Bytes: fh.totalBytesWritten}))

	op := fh.File.FileSystem.RetryPolicy.StartOperation()
	for {
		err := fh.FlushAttempt(operation)
		if err != io.EOF || IsSuccessOrNonRetriableError(err) || !op.ShouldRetry("Flush() %s", err) {
			return err
		}
		// Reconnect and try again
		fh.File.FileSystem.getDFSConnector().Close()
		logger.Warn("Failed to copy file to DFS", fh.logInfo(logger.Fields{Operation: operation}))
	}
}

func (fh *FileHandle) FlushAttempt(operation string) error {
	hdfsAccessor := fh.File.FileSystem.getDFSConnector()
	//delete the file and then rewrite.
	//note we can not rely on the overwrite functionality of CreateFile API.
	//For example if the file has permission set to 444 then we can not overwrite it
	err := hdfsAccessor.Remove(fh.File.AbsolutePath())
	if err != nil {
		// may be this is a retry and the file has already been deleted
		// log error and continue
		logger.Warn("Unable to delete the file during flush.", fh.logInfo(logger.Fields{Operation: operation, Error: err}))
	}

	w, err := hdfsAccessor.CreateFile(fh.File.AbsolutePath(), fh.File.Attrs.Mode, true)
	if err != nil {
		logger.Error("Error creating file in DFS", fh.logInfo(logger.Fields{Operation: operation, Error: err}))
		return err
	}

	//open the file for reading and upload to DFS
	err = fh.File.fileProxy.SeekToStart()
	if err != nil {
		logger.Error("Unable to seek to the begenning of the temp file", fh.logInfo(logger.Fields{Operation: operation, Error: err}))
		return err
	}

	written := uint64(0)
	for {
		b := make([]byte, 65536)
		nr, err := fh.File.fileProxy.Read(b)
		if err != nil && err != io.EOF {
			logger.Error("Failed to read from staging file", fh.logInfo(logger.Fields{Operation: operation, Error: err}))
			return err
		}

		if nr > 0 {
			b = b[:nr]
			nw, err := w.Write(b)
			if err != nil {
				logger.Error("Failed to write to DFS", fh.logInfo(logger.Fields{Operation: operation, Error: err}))
				w.Close()
				return err
			}

			if nr != nw {
				logger.Error(fmt.Sprintf("Incorrect bytes read/written. Bytes reads %d, %d", nr, nw),
					fh.logInfo(logger.Fields{Operation: operation, Error: err}))
				w.Close()
				logger.Error(fmt.Sprintf("incorrect bytes read/written. Bytes reads %d, %d", nr, nw), logger.Fields{Path: fh.File.AbsolutePath()})
				return syscall.EIO
			}

			written += uint64(nw)
		}

		if err != nil && err == io.EOF {
			break
		}
	}

	err = w.Close()
	if err != nil {
		logger.Error("Failed to close file in DFS", fh.logInfo(logger.Fields{Operation: operation, Error: err}))
		return err
	}
	logger.Info("Uploaded to DFS", fh.logInfo(logger.Fields{Operation: operation, Bytes: written}))

	fh.File.Attrs.Size = written
	return nil
}

// Responds to the FUSE Flush request
func (fh *FileHandle) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	fh.lockHandle()
	defer fh.unlockHandle()
	if fh.dataChanged() {
		logger.Info("Flush file", fh.logInfo(logger.Fields{Operation: Flush}))
		return fh.copyToDFS(Flush)
	} else {
		logger.Info("Flush file. Ignoring requst as no data has changed", fh.logInfo(logger.Fields{Operation: Flush}))
		return nil
	}
}

// Responds to the FUSE Fsync request
func (fh *FileHandle) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	fh.lockHandle()
	defer fh.unlockHandle()
	if fh.dataChanged() {
		logger.Info("Fsync file", fh.logInfo(logger.Fields{Operation: Fsync}))
		return fh.copyToDFS(Fsync)
	} else {
		return nil
	}
}

// Closes the handle
func (fh *FileHandle) Release(_ context.Context, _ *fuse.ReleaseRequest) error {
	fh.lockHandle()
	defer fh.unlockHandle()

	//close the file handle if it is the last handle
	fh.File.InvalidateMetadataCache()
	fh.File.RemoveHandle(fh)

	logger.Info("Closed file handle ", fh.logInfo(logger.Fields{Operation: Close, Flags: fh.fileFlags, TotalBytesRead: fh.tatalBytesRead, TotalBytesWritten: fh.totalBytesWritten}))
	return nil
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

func (fh *FileHandle) lockHandle() {
	fh.mutex.Lock()
}

func (fh *FileHandle) unlockHandle() {
	fh.mutex.Unlock()
}
