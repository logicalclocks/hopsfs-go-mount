// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package hopsfsmount

import (
	"io"
	"syscall"

	"hopsworks.ai/hopsfsmount/internal/hopsfsmount/logger"
)

type RemoteROFileProxy struct {
	hdfsReader ReadSeekCloser
	file       *FileINode
}

var _ FileProxy = (*RemoteROFileProxy)(nil)

func (p *RemoteROFileProxy) Truncate(size int64) (int64, error) {
	// No locking needed - caller holds dataMutex which serializes all I/O
	logger.Fatal("Truncate API is not supported. Read only mode", nil)
	return 0, nil
}

func (p *RemoteROFileProxy) WriteAt(b []byte, off int64) (n int, err error) {
	// No locking needed - caller holds dataMutex which serializes all I/O
	logger.Fatal("WriteAt API is not supported. Read only mode", nil)
	return 0, nil
}

func (p *RemoteROFileProxy) ReadAt(b []byte, off int64) (int, error) {
	// No locking needed - caller holds dataMutex which serializes all I/O
	logger.Debug("RemoteFileProxy ReadAt", p.file.logInfo(logger.Fields{Operation: Read, Offset: off}))

	if off < 0 {
		logger.Error("WriteAt. Negative offset", logger.Fields{Path: p.file.AbsolutePath()})
		return 0, syscall.EINVAL
	}
	maxBytesToRead := len(b)

	if err := p.hdfsReader.Seek(off); err != nil {
		return 0, err
	}

	var err error = nil
	var n int = 0
	for len(b) > 0 {
		m, e := p.hdfsReader.Read(b)

		if m > 0 {
			n += m
			b = b[m:]
		}

		if e != nil {
			err = e
			break
		}
	}

	if err != nil && err == io.EOF && n > 0 {
		// no need to throw io.EOF
		logger.Debug("RemoteFileProxy Finished reading", nil)
		err = nil
	}

	logger.Debug("RemoteFileProxy ReadAt", p.file.logInfo(logger.Fields{Operation: Read, MaxBytesToRead: maxBytesToRead,
		BytesRead: n, Error: err, Offset: off}))
	return n, err
}

func (p *RemoteROFileProxy) SeekToStart() (err error) {
	// No locking needed - caller holds dataMutex which serializes all I/O
	err = p.hdfsReader.Seek(0)
	if err != nil {
		logger.Debug("RemoteFileProxy SeekToStart failed", p.file.logInfo(logger.Fields{Operation: SeekToStart, Offset: 0, Error: err}))
		return err
	} else {
		logger.Debug("RemoteFileProxy SeekToStart", p.file.logInfo(logger.Fields{Operation: SeekToStart, Offset: 0}))
		return nil
	}

}

func (p *RemoteROFileProxy) Read(b []byte) (n int, err error) {
	// No locking needed - caller holds dataMutex which serializes all I/O
	n, err = p.hdfsReader.Read(b)

	if err != nil {
		logger.Debug("RemoteFileProxy Read", p.file.logInfo(logger.Fields{Operation: Read, MaxBytesToRead: len(b), Error: err}))
		return n, err
	} else {
		logger.Debug("RemoteFileProxy Read", p.file.logInfo(logger.Fields{Operation: Read, MaxBytesToRead: len(b), TotalBytesRead: n}))
		return n, nil
	}
}

func (p *RemoteROFileProxy) Close() error {
	//NOTE: Locking is done in File.go (closeStaging and upgradeHandleForWriting hold fileHandleMutex)
	err := p.hdfsReader.Close()
	if err != nil {
		logger.Debug("RemoteFileProxy Close failed", p.file.logInfo(logger.Fields{Operation: Close, Error: err}))
		return err
	} else {
		logger.Debug("RemoteFileProxy Close", p.file.logInfo(logger.Fields{Operation: Close}))
		return nil
	}
}

func (p *RemoteROFileProxy) Sync() error {
	// No locking needed - caller holds dataMutex which serializes all I/O
	logger.Fatal("Sync API is not supported. Read only mode", nil)
	return nil
}
