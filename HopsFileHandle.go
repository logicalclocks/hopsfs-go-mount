// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"io"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	logger "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

// Represends a handle to an open file
type FileHandle struct {
	File      *File
	Mutex     sync.Mutex // all operations on the handle are serialized to simplify invariants
	handle    *os.File
	fileFlags fuse.OpenFlags // flags used to creat the file
}

// Verify that *FileHandle implements necesary FUSE interfaces
var _ fs.Node = (*FileHandle)(nil)
var _ fs.HandleReader = (*FileHandle)(nil)
var _ fs.HandleReleaser = (*FileHandle)(nil)
var _ fs.HandleWriter = (*FileHandle)(nil)
var _ fs.NodeFsyncer = (*FileHandle)(nil)
var _ fs.HandleFlusher = (*FileHandle)(nil)

func (fh *FileHandle) createStagingFile(operation string, existsInDFS bool) error {
	if fh.File.tmpFile != "" {
		if _, err := os.Stat(fh.File.tmpFile); err == nil {
			return nil
		}
	}

	//create staging file
	absPath := fh.File.AbsolutePath()
	hdfsAccessor := fh.File.FileSystem.HdfsAccessor
	if !existsInDFS { // it  is a new file so create it in the DFS
		w, err := hdfsAccessor.CreateFile(absPath, fh.File.Attrs.Mode, false)
		if err != nil {
			logger.WithFields(logger.Fields{Operation: operation, Path: absPath, Error: err}).Error("Failed to create file in DFS")
			return err
		}
		w.Close()
	} else {
		// Request to write to existing file
		_, err := hdfsAccessor.Stat(absPath)
		if err != nil {
			logger.WithFields(logger.Fields{Operation: operation, Path: absPath, Error: err}).Error("Failed to stat file in DFS")
			return &os.PathError{Op: operation, Path: absPath, Err: os.ErrNotExist}
		}
	}

	stagingFile, err := ioutil.TempFile(stagingDir, "stage")
	if err != nil {
		logger.WithFields(logger.Fields{Operation: operation, Path: absPath, Error: err}).Error("Failed to create staging file")
		return err
	}
	defer stagingFile.Close()
	logger.WithFields(logger.Fields{Operation: operation, Path: absPath, TmpFile: stagingFile.Name()}).Info("Created staging file ")

	fh.File.tmpFile = stagingFile.Name()
	return nil
}

func (fh *FileHandle) downloadToStaging(operation string) error {
	logger.WithFields(logger.Fields{Operation: operation, Path: fh.File.AbsolutePath()}).Debug("Downloading a copy to stating dir")
	hdfsAccessor := fh.File.FileSystem.HdfsAccessor
	absPath := fh.File.AbsolutePath()

	reader, err := hdfsAccessor.OpenRead(absPath)
	if err != nil {
		logger.WithFields(logger.Fields{Operation: operation, Path: absPath, Error: err}).Error("Failed to open file in DFS")
		// TODO remove the staging file if there are no more active handles
		return &os.PathError{Op: operation, Path: absPath, Err: err}
	}

	tmpFileHandle, err := os.OpenFile(fh.File.tmpFile, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		logger.WithFields(logger.Fields{Operation: operation, Path: absPath, TmpFile: fh.File.tmpFile, Error: err}).Error("Failed to open staging file")
		return &os.PathError{Op: operation, Path: absPath, Err: err}
	}

	nc, err := io.Copy(tmpFileHandle, reader)
	if err != nil {
		logger.WithFields(logger.Fields{Operation: operation, Path: absPath, TmpFile: fh.File.tmpFile, Error: err}).Error("Failed to copy content to staging file")
		tmpFileHandle.Close()
		// TODO remote the statging file.
		return &os.PathError{Op: operation, Path: absPath, Err: err}
	}
	reader.Close()
	tmpFileHandle.Close()
	logger.WithFields(logger.Fields{Operation: operation, Path: fh.File.AbsolutePath()}).Infof("Downloaded a copy to stating dir. %d bytes copied", nc)
	return nil
}

// Creates new file handle
func NewFileHandle(file *File, existsInDFS bool, flags fuse.OpenFlags) (*FileHandle, error) {

	operation := Create
	if existsInDFS {
		operation = Open
	}

	fh := &FileHandle{File: file, fileFlags: flags}
	checkDiskSpace(fh.File.FileSystem.HdfsAccessor)

	if err := fh.createStagingFile(operation, existsInDFS); err != nil {
		return nil, err
	}

	//	isTrunc := false
	//	isAppend := false
	//	if (flags | fuse.OpenTruncate) == fuse.OpenTruncate {
	//		isTrunc = true
	//	}
	//	if (flags | fuse.OpenAppend) == fuse.OpenAppend {
	//		isAppend = true
	//	}
	truncateContent := flags.IsWriteOnly() && (flags&fuse.OpenAppend != fuse.OpenAppend)

	if existsInDFS && !truncateContent {
		// TODO handle the case of truncate.
		if err := fh.downloadToStaging(operation); err != nil {
			return nil, err
		}
	}

	if truncateContent {
		logger.WithFields(logger.Fields{Operation: operation, Path: fh.File.AbsolutePath()}).Info("Truncate file to 0")
		os.Truncate(fh.File.tmpFile, 0)
	}

	// remove the O_EXCL flag as the staging file is now created.
	//int((flags&^fuse.OpenExclusive)|fuse.OpenReadWrite)
	fileHandle, err := os.OpenFile(fh.File.tmpFile, int(fuse.OpenReadWrite), 0600)
	if err != nil {
		return nil, &os.PathError{Op: operation, Path: fh.File.tmpFile, Err: err}
	}

	fh.handle = fileHandle
	return fh, nil
}

func checkDiskSpace(hdfsAccessor HdfsAccessor) error {
	//TODO FIXME
	//	fsInfo, err := hdfsAccessor.StatFs()
	//	if err != nil {
	//		// Donot abort, continue writing
	//		Error.Println("Failed to get HDFS usage, ERROR:", err)
	//	} else if uint64(req.Offset) >= fsInfo.remaining {
	//		Error.Println("[", fhw.Handle.File.AbsolutePath(), "] writes larger size (", req.Offset, ")than HDFS available size (", fsInfo.remaining, ")")
	//		return errors.New("Too large file")
	//	}
	return nil
}

// Returns attributes of the file associated with this handle
func (fh *FileHandle) Attr(ctx context.Context, a *fuse.Attr) error {
	fh.Mutex.Lock()
	defer fh.Mutex.Unlock()
	return fh.File.Attr(ctx, a)
}

// Responds to FUSE Read request
func (fh *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	fh.Mutex.Lock()
	defer fh.Mutex.Unlock()

	buf := resp.Data[0:req.Size]
	_, err := fh.handle.Seek(req.Offset, 0)
	if err != nil {
		return err
	}

	nr, err := fh.handle.Read(buf)
	resp.Data = resp.Data[0:nr]

	if err != nil {
		if err == io.EOF {
			// EOF isn't a error, reporting successful read to FUSE
			logger.WithFields(logger.Fields{Operation: Read, Path: fh.File.AbsolutePath(), Bytes: nr}).Info("Finished reading from staging file")
			return nil
		} else {
			logger.WithFields(logger.Fields{Operation: Read, Path: fh.File.AbsolutePath(), Error: err, Bytes: nr}).Error("Failed to read from staging file")
			return err
		}
	}
	logger.WithFields(logger.Fields{Operation: Read, Path: fh.File.AbsolutePath(), Bytes: nr}).Info("Read from staging file")
	return err
}

// Responds to FUSE Write request
func (fh *FileHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	fh.Mutex.Lock()
	defer fh.Mutex.Unlock()

	var nw int
	var err error
	// if fh.fileFlags&fuse.OpenAppend == fuse.OpenAppend {
	// Info.Println("using write API")
	// nw, err = fh.handle.Write(req.Data)
	// } else {
	// Info.Println("using writeAt API")
	nw, err = fh.handle.WriteAt(req.Data, req.Offset)
	// }
	resp.Size = nw
	if err != nil {
		logger.WithFields(logger.Fields{Operation: Write, Path: fh.File.AbsolutePath(), Error: err}).Warn("Failed to write to staging file")
		return err
	} else {
		logger.WithFields(logger.Fields{Operation: Write, Path: fh.File.AbsolutePath(), Bytes: nw}).Info("Write to staging file")
		return nil
	}
}

func (fh *FileHandle) copyToDFS(operation string) error {
	info, _ := os.Stat(fh.File.tmpFile)
	var size int64 = 0
	if info != nil {
		size = info.Size()
	}

	if size == 0 { // Nothing to do
		return nil
	}
	defer fh.File.InvalidateMetadataCache()

	logger.WithFields(logger.Fields{Operation: Write, Path: fh.File.AbsolutePath(), Bytes: size}).Info("Uploading to DFS")

	op := fh.File.FileSystem.RetryPolicy.StartOperation()
	for {
		err := fh.FlushAttempt(operation)
		if err != io.EOF || IsSuccessOrBenignError(err) || !op.ShouldRetry("Flush()", err) {
			return err
		}
		// Restart a new connection, https://github.com/colinmarc/hdfs/issues/86
		fh.File.FileSystem.HdfsAccessor.Close()
		logger.WithFields(logger.Fields{Operation: operation, Path: fh.File.AbsolutePath()}).Warn("Failed to copy file to DFS")
		// Wait for 30 seconds before another retry to get another set of datanodes.
		// https://community.hortonworks.com/questions/2474/how-to-identify-stale-datanode.html
		time.Sleep(30 * time.Second)
	}
	return nil
}

func (fh *FileHandle) FlushAttempt(operation string) error {
	hdfsAccessor := fh.File.FileSystem.HdfsAccessor
	w, err := hdfsAccessor.CreateFile(fh.File.AbsolutePath(), fh.File.Attrs.Mode, true)
	if err != nil {
		logger.WithFields(logger.Fields{Operation: operation, Path: fh.File.AbsolutePath(), Error: err}).Warn("Error creating file in DFS")
		return err
	}

	//open the file for reading and upload to DFS
	in, err := os.Open(fh.File.tmpFile)
	defer in.Close()

	if err != nil {
		logger.WithFields(logger.Fields{Operation: operation, Path: fh.File.AbsolutePath(), TmpFile: fh.File.tmpFile, Error: err}).Warn("Failed to open staging file")
		return &os.PathError{Op: operation, Path: fh.File.AbsolutePath(), Err: err}
	}

	b := make([]byte, 65536)
	written := 0
	for {
		nr, err := in.Read(b)
		if err != nil {
			if err != io.EOF {
				logger.WithFields(logger.Fields{Operation: operation, Path: fh.File.AbsolutePath(), Error: err}).Warn("Failed to read from staging file")
			}
			break
		}
		b = b[:nr]

		nw, err := w.Write(b)
		if err != nil {
			logger.WithFields(logger.Fields{Operation: operation, Path: fh.File.AbsolutePath(), Error: err}).Warn("Failed to write to DFS")
			w.Close()
			return err
		}
		logger.WithFields(logger.Fields{Operation: operation, Path: fh.File.AbsolutePath(), Bytes: nw}).Trace("Written to DFS")
		written += nw
	}

	err = w.Close()
	if err != nil {
		logger.WithFields(logger.Fields{Operation: operation, Path: fh.File.AbsolutePath(), Error: err}).Warn("Failed to close file in DFS")
		return err
	}
	logger.WithFields(logger.Fields{Operation: operation, Path: fh.File.AbsolutePath(), Bytes: written}).Info("Completed copying to DFS")
	return nil
}

// Responds to the FUSE Flush request
func (fh *FileHandle) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	fh.Mutex.Lock()
	defer fh.Mutex.Unlock()
	logger.WithFields(logger.Fields{Operation: Flush, Path: fh.File.AbsolutePath()}).Info("Flush file")
	return fh.copyToDFS(Flush)
}

// Responds to the FUSE Fsync request
func (fh *FileHandle) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	fh.Mutex.Lock()
	defer fh.Mutex.Unlock()
	logger.WithFields(logger.Fields{Operation: Fsync, Path: fh.File.AbsolutePath()}).Info("Fsync file")
	return fh.copyToDFS(Fsync)
}

// Closes the handle
func (fh *FileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	fh.Mutex.Lock()
	defer fh.Mutex.Unlock()
	err := fh.handle.Close()

	if err != nil {
		logger.WithFields(logger.Fields{Operation: Close, Path: fh.File.AbsolutePath(), Error: err}).Warn("Failed to close staging file")
	}
	fh.handle = nil
	fh.File.InvalidateMetadataCache()
	fh.File.RemoveHandle(fh)
	logger.WithFields(logger.Fields{Operation: Close, Path: fh.File.AbsolutePath()}).Warn("Close file")
	return err
}