// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package hopsfsmount

import (
	"math"
	"os"

	"hopsworks.ai/hopsfsmount/internal/hopsfsmount/logger"
)

type LocalRWFileProxy struct {
	localFile *os.File // handle to the temp file in staging dir
	file      *FileINode
}

var _ FileProxy = (*LocalRWFileProxy)(nil)

func (p *LocalRWFileProxy) Truncate(size int64) (int64, error) {
	// No locking needed - caller holds dataMutex which serializes all I/O
	statBefore, err := p.localFile.Stat()
	if err != nil {
		return 0, err
	}

	err = p.localFile.Truncate(size)
	if err != nil {
		return 0, err
	}

	statAfter, err := p.localFile.Stat()
	if err != nil {
		return 0, err
	}

	return int64(math.Abs(float64(statAfter.Size()) - float64(statBefore.Size()))), nil
}

func (p *LocalRWFileProxy) WriteAt(b []byte, off int64) (n int, err error) {
	// No locking needed - caller holds dataMutex which serializes all I/O
	return p.localFile.WriteAt(b, off)
}

func (p *LocalRWFileProxy) ReadAt(b []byte, off int64) (n int, err error) {
	// No locking needed - caller holds dataMutex which serializes all I/O
	n, err = p.localFile.ReadAt(b, off)
	logger.Trace("LocalFileProxy ReadAt", p.file.logInfo(logger.Fields{Operation: Read, Bytes: n, Error: err, Offset: off}))
	return
}

func (p *LocalRWFileProxy) SeekToStart() (err error) {
	// No locking needed - caller holds dataMutex which serializes all I/O
	_, err = p.localFile.Seek(0, 0)
	return
}

func (p *LocalRWFileProxy) Read(b []byte) (n int, err error) {
	// No locking needed - caller holds dataMutex which serializes all I/O
	return p.localFile.Read(b)
}

func (p *LocalRWFileProxy) Close() error {
	//NOTE: Locking is done in File.go (closeStaging holds fileHandleMutex)
	return p.localFile.Close()
}

// TODO why there is a sync in File.go and also here
func (p *LocalRWFileProxy) Sync() error {
	// No locking needed - caller holds dataMutex which serializes all I/O
	return p.localFile.Sync()
}
