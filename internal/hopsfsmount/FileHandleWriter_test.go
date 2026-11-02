// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package hopsfsmount

import (
	"io"
	"os"
	"syscall"
	"testing"

	"bazil.org/fuse"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestReadWriteFile(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	hdfsAccessor.EXPECT().IsAvailable().Return(true).AnyTimes()
	fileName := "/testWriteFile_1"
	fs, _ := NewFileSystem([]HdfsAccessor{hdfsAccessor}, "/", []string{"*"}, false, DelaySyncUntilClose, NewDefaultRetryPolicy(mockClock), mockClock)

	hdfswriter := NewMockHdfsWriter(mockCtrl)

	hdfswriter.EXPECT().Close().Return(nil).AnyTimes()
	hdfsAccessor.EXPECT().CreateFileWithGroup(fileName, gomock.Any(), gomock.Any(), gomock.Any()).Return(hdfswriter, nil).AnyTimes()
	hdfsAccessor.EXPECT().Stat(fileName).Return(Attrs{Name: fileName}, nil).AnyTimes()
	hdfsAccessor.EXPECT().Chown(fileName, gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	hdfswriter.EXPECT().Close().Return(nil).AnyTimes()
	// Mock for flushToDFS during Release
	hdfsAccessor.EXPECT().Remove(fileName).Return(nil).AnyTimes()
	hdfswriter.EXPECT().Write(gomock.Any()).Return(11, nil).AnyTimes()

	root, _ := fs.Root()
	_, h, err := root.(*DirINode).Create(nil, &fuse.CreateRequest{Name: fileName,
		Flags: fuse.OpenReadWrite | fuse.OpenCreate, Mode: os.FileMode(0757)}, &fuse.CreateResponse{})

	// file := root.(*Dir).NodeFromAttrs(Attrs{Name: fileName, Mode: os.FileMode(0757)}).(*File)
	// writeHandle, err := NewFileHandle(file, true, fuse.OpenReadWrite)
	fileHandle := h.(*FileHandle)
	assert.Nil(t, err)

	// Test for normal write
	hdfsAccessor.EXPECT().StatFs().Return(FsInfo{capacity: uint64(100), used: uint64(20), remaining: uint64(80)}, nil).AnyTimes()
	err = fileHandle.Write(nil, &fuse.WriteRequest{Data: []byte("hello world"), Offset: int64(0)}, &fuse.WriteResponse{})
	assert.Nil(t, err)
	assert.Equal(t, fileHandle.totalBytesWritten, int64(11))

	// now open the file gaing and read it.
	buffer := make([]byte, 65536)
	readReq := &fuse.ReadRequest{Offset: 0, Size: len(buffer)}
	readResp := &fuse.ReadResponse{Data: buffer}
	fileHandle.Read(nil, readReq, readResp)
	assert.Equal(t, int(11), len(string(readResp.Data)))
	err = fileHandle.Release(nil, nil)
	assert.Nil(t, err)
}

func TestFaultTolerantWriteFile(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	hdfsAccessor.EXPECT().IsAvailable().Return(true).AnyTimes()
	fileName := "/testWriteFile_1"
	fs, _ := NewFileSystem([]HdfsAccessor{hdfsAccessor}, "/", []string{"*"}, false, DelaySyncUntilClose, NewDefaultRetryPolicy(mockClock), mockClock)

	hdfswriter := NewMockHdfsWriter(mockCtrl)

	hdfswriter.EXPECT().Close().Return(nil).AnyTimes()
	hdfsAccessor.EXPECT().Stat(fileName).Return(Attrs{Name: fileName, Mode: os.FileMode(0757)}, nil).AnyTimes()
	hdfsAccessor.EXPECT().Chown(fileName, gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	hdfswriter.EXPECT().Close().Return(nil).AnyTimes()

	hdfsAccessor.EXPECT().StatFs().Return(FsInfo{capacity: uint64(100), used: uint64(20), remaining: uint64(80)}, nil).AnyTimes()
	hdfsAccessor.EXPECT().Remove("/testWriteFile_1").Return(nil).AnyTimes()
	hdfsAccessor.EXPECT().CreateFileWithGroup(fileName, os.FileMode(0757), gomock.Any(), gomock.Any()).DoAndReturn(func(path string,
		mode os.FileMode, overwrite bool, groupname string) (HdfsWriter, error) {
		return hdfswriter, nil
	}).AnyTimes()

	root, _ := fs.Root()
	_, h, err := root.(*DirINode).Create(nil, &fuse.CreateRequest{Name: fileName,
		Flags: fuse.OpenReadWrite | fuse.OpenCreate, Mode: os.FileMode(0757)}, &fuse.CreateResponse{})

	// Test for newfilehandlewriter
	hdfsAccessor.EXPECT().CreateFileWithGroup(fileName, os.FileMode(0757), false, gomock.Any()).Return(hdfswriter, nil).AnyTimes()
	hdfswriter.EXPECT().Close().Return(nil).AnyTimes()
	writeHandle := h.(*FileHandle)
	assert.Nil(t, err)

	// Test for normal write
	err = writeHandle.Write(nil, &fuse.WriteRequest{Data: []byte("hello world"), Offset: int64(0)}, &fuse.WriteResponse{})
	assert.Nil(t, err)
	assert.Equal(t, writeHandle.totalBytesWritten, int64(11))

	binaryData := make([]byte, 65536)
	writeHandle.File.fileProxy.SeekToStart()
	nr, _ := writeHandle.File.fileProxy.Read(binaryData)
	binaryData = binaryData[:nr]

	// Mock the EOF error to test the fault tolerant write/flush
	hdfswriter.EXPECT().Write(binaryData).Return(0, io.EOF).AnyTimes()
	hdfswriter.EXPECT().Close().Return(nil).AnyTimes()
	// FlushAttempt is now a file-level method, so we call Flush which delegates to file.flushToDFS
	err = writeHandle.Flush(nil, nil)
	assert.Equal(t, io.EOF, err)

	// The connection would be closed
	hdfsAccessor.EXPECT().Close().Return(nil).AnyTimes()
	// New connection being established
	newhdfswriter := NewMockHdfsWriter(mockCtrl)
	newhdfswriter.EXPECT().Write(binaryData).Return(11, nil).AnyTimes()
	newhdfswriter.EXPECT().Close().Return(nil).AnyTimes()
	hdfswriter = newhdfswriter
	hdfsAccessor.EXPECT().StatFs().Return(FsInfo{capacity: uint64(100), used: uint64(20), remaining: uint64(80)}, nil).AnyTimes()
	hdfsAccessor.EXPECT().Remove(fileName).Return(nil).AnyTimes()
	// hdfsAccessor.EXPECT().CreateFile(fileName, os.FileMode(0757), gomock.Any()).Return(newhdfswriter, nil).AnyTimes()

	hdfsAccessor.EXPECT().Remove(fileName).Return(nil).AnyTimes()
	err = writeHandle.Flush(nil, nil)
	assert.Nil(t, err)

	// Test for closing file
	err = writeHandle.Release(nil, nil)
	assert.Nil(t, err)
}

// openExistingFileReadOnly opens a file the backend has, with 5 bytes. The
// mount opens it read-only and switches the handle to a staging copy on the
// first write or truncate, which stats the file again.
func openExistingFileReadOnly(t *testing.T, name string) (*FileHandle, *MockHdfsAccessor, *MockReadSeekCloserWithPseudoRandomContent) {
	// With the staging cache on, the open itself downloads a staging copy and
	// there is no switch to test.
	originalCache := StagingCache
	t.Cleanup(func() { StagingCache = originalCache })
	StagingCache = nil

	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	hdfsAccessor.EXPECT().IsAvailable().Return(true).AnyTimes()
	fs, _ := NewFileSystem([]HdfsAccessor{hdfsAccessor}, "/", []string{"*"}, false, DelaySyncUntilClose, NewDefaultRetryPolicy(mockClock), mockClock)
	root, _ := fs.Root()
	file := root.(*DirINode).addOrUpdateChildInodeAttrs("unit_test", name, Attrs{Name: name, Mode: os.FileMode(0644)}).(*FileINode)

	reader := &MockReadSeekCloserWithPseudoRandomContent{FileSize: 5, ReaderStats: &ReaderStats{}}
	hdfsAccessor.EXPECT().OpenRead("/"+name).Return(reader, nil)
	h, err := file.Open(nil, &fuse.OpenRequest{Flags: fuse.OpenReadWrite}, &fuse.OpenResponse{})
	assert.Nil(t, err)
	return h.(*FileHandle), hdfsAccessor, reader
}

// A NameNode or connection failure during the switch to a staging copy must
// reach the application as that failure: ENOENT would tell it that the file
// it holds open does not exist. The read only proxy, shared by every handle of
// the file, must survive the failure.
func TestTruncateReturnsTheErrorOfAFailedSwitchToStaging(t *testing.T) {
	fileHandle, hdfsAccessor, reader := openExistingFileReadOnly(t, "existing")

	hdfsAccessor.EXPECT().Stat("/existing").Return(Attrs{}, syscall.EIO)
	assert.Equal(t, syscall.EIO, fileHandle.Truncate(0))

	assert.False(t, reader.IsClosed)
	resp := &fuse.ReadResponse{Data: make([]byte, 5)}
	assert.NotPanics(t, func() {
		assert.Nil(t, fileHandle.Read(nil, &fuse.ReadRequest{Size: 5}, resp))
	})
	assert.Equal(t, 5, len(resp.Data))
}

// The write after a failed switch tries the switch again.
func TestWriteReturnsTheErrorOfAFailedSwitchToStaging(t *testing.T) {
	fileHandle, hdfsAccessor, reader := openExistingFileReadOnly(t, "existing")
	write := func() error {
		var err error
		assert.NotPanics(t, func() {
			err = fileHandle.Write(nil, &fuse.WriteRequest{Data: []byte("hello world"), Offset: int64(0)}, &fuse.WriteResponse{})
		})
		return err
	}

	hdfsAccessor.EXPECT().Stat("/existing").Return(Attrs{}, syscall.EIO)
	assert.Equal(t, syscall.EIO, write())
	assert.False(t, reader.IsClosed)

	hdfsAccessor.EXPECT().Stat("/existing").Return(Attrs{Name: "existing", Mode: os.FileMode(0644), Size: 5}, nil)
	hdfsAccessor.EXPECT().OpenRead("/existing").Return(&MockReadSeekCloserWithPseudoRandomContent{FileSize: 5, ReaderStats: &ReaderStats{}}, nil)
	assert.Nil(t, write())
	assert.True(t, reader.IsClosed)
	_, staged := fileHandle.File.fileProxy.(*LocalRWFileProxy)
	assert.True(t, staged)
}

// A download of the staging copy that fails partway is a failed switch too,
// and must close the DFS reader it opened.
func TestWriteClosesTheReaderOfAFailedDownloadToStaging(t *testing.T) {
	fileHandle, hdfsAccessor, reader := openExistingFileReadOnly(t, "existing")
	download := NewMockReadSeekCloser(gomock.NewController(t))
	download.EXPECT().Read(gomock.Any()).Return(0, syscall.EIO)
	download.EXPECT().Close().Return(nil)
	hdfsAccessor.EXPECT().Stat("/existing").Return(Attrs{Name: "existing", Mode: os.FileMode(0644), Size: 5}, nil)
	hdfsAccessor.EXPECT().OpenRead("/existing").Return(download, nil)

	err := fileHandle.Write(nil, &fuse.WriteRequest{Data: []byte("hello world"), Offset: int64(0)}, &fuse.WriteResponse{})
	assert.Equal(t, syscall.EIO, err)
	assert.False(t, reader.IsClosed)
}

func TestFlushFile(t *testing.T) {
	t.Skip()
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	hdfsAccessor.EXPECT().IsAvailable().Return(true).AnyTimes()
	readSeekCloser := NewMockReadSeekCloser(mockCtrl)

	hdfsAccessor.EXPECT().OpenRead("/testWriteFile_2").Return(readSeekCloser, nil).AnyTimes()
	readSeekCloser.EXPECT().Read(gomock.Any()).Return(0, io.EOF).AnyTimes()
	readSeekCloser.EXPECT().Seek(gomock.Any()).Return(nil).AnyTimes()
	readSeekCloser.EXPECT().Position().Return(int64(0), nil).AnyTimes()
	readSeekCloser.EXPECT().Close().Return(nil).AnyTimes()

	hdfswriter := NewMockHdfsWriter(mockCtrl)
	hdfswriter.EXPECT().Close().Return(nil).AnyTimes()
	hdfsAccessor.EXPECT().StatFs().Return(FsInfo{capacity: uint64(100), used: uint64(20), remaining: uint64(80)}, nil).AnyTimes()
	hdfsAccessor.EXPECT().Stat("/testWriteFile_2").Return(Attrs{Name: "testWriteFile_2"}, nil)
	fileName := "/testWriteFile_2"
	fs, _ := NewFileSystem([]HdfsAccessor{hdfsAccessor}, "/", []string{"*"}, false, DelaySyncUntilClose, NewDefaultRetryPolicy(mockClock), mockClock)

	hdfsAccessor.EXPECT().Remove(fileName).Return(nil).AnyTimes()
	hdfsAccessor.EXPECT().CreateFile(fileName, os.FileMode(0757), true).Return(hdfswriter, nil).AnyTimes()
	hdfswriter.EXPECT().Close().Return(nil).AnyTimes()
	hdfswriter.EXPECT().Write([]byte("hello world")).Return(0, nil).AnyTimes()

	// Test for newfilehandlewriter with existing file
	root, _ := fs.Root()
	file := (root.(*DirINode).addOrUpdateChildInodeAttrs("unit_test", "testWriteFile_2", Attrs{Name: "testWriteFile_2", Mode: os.FileMode(0757)})).(*FileINode)
	fh, _ := file.Open(nil, &fuse.OpenRequest{}, &fuse.OpenResponse{})
	fileHandle := fh.(*FileHandle)

	// Test for flush
	_ = fileHandle.Write(nil, &fuse.WriteRequest{Data: []byte("hello world"), Offset: int64(0)}, &fuse.WriteResponse{})
	err := fileHandle.Flush(nil, nil)
	assert.Nil(t, err)

	err = fileHandle.Release(nil, nil)
	assert.Nil(t, err)
}
