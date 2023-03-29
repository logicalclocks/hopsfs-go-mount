// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package main

import (
	"io"
	"os"
	"testing"

	"bazil.org/fuse"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestReadWriteFile(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	fileName := "/testWriteFile_1"
	fs, _ := NewFileSystem([]HdfsAccessor{hdfsAccessor}, "/", []string{"*"}, false, NewDefaultRetryPolicy(mockClock), mockClock)

	hdfswriter := NewMockHdfsWriter(mockCtrl)

	hdfswriter.EXPECT().Close().Return(nil).AnyTimes()
	hdfsAccessor.EXPECT().CreateFile(fileName, os.FileMode(0757), gomock.Any()).Return(hdfswriter, nil).AnyTimes()
	hdfsAccessor.EXPECT().Stat(fileName).Return(Attrs{Name: fileName}, nil).AnyTimes()
	hdfsAccessor.EXPECT().Chown(fileName, gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	hdfswriter.EXPECT().Close().Return(nil).AnyTimes()

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
	fileName := "/testWriteFile_1"
	fs, _ := NewFileSystem([]HdfsAccessor{hdfsAccessor}, "/", []string{"*"}, false, NewDefaultRetryPolicy(mockClock), mockClock)

	hdfswriter := NewMockHdfsWriter(mockCtrl)

	hdfswriter.EXPECT().Close().Return(nil).AnyTimes()
	hdfsAccessor.EXPECT().Stat(fileName).Return(Attrs{Name: fileName, Mode: os.FileMode(0757)}, nil).AnyTimes()
	hdfsAccessor.EXPECT().Chown(fileName, gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	hdfswriter.EXPECT().Close().Return(nil).AnyTimes()

	hdfsAccessor.EXPECT().StatFs().Return(FsInfo{capacity: uint64(100), used: uint64(20), remaining: uint64(80)}, nil).AnyTimes()
	hdfsAccessor.EXPECT().Remove("/testWriteFile_1").Return(nil).AnyTimes()
	hdfsAccessor.EXPECT().CreateFile(fileName, os.FileMode(0757), gomock.Any()).DoAndReturn(func(path string,
		mode os.FileMode, overwrite bool) (HdfsWriter, error) {
		return hdfswriter, nil
	}).AnyTimes()

	root, _ := fs.Root()
	_, h, err := root.(*DirINode).Create(nil, &fuse.CreateRequest{Name: fileName,
		Flags: fuse.OpenReadWrite | fuse.OpenCreate, Mode: os.FileMode(0757)}, &fuse.CreateResponse{})

	// Test for newfilehandlewriter
	hdfsAccessor.EXPECT().CreateFile(fileName, os.FileMode(0757), false).Return(hdfswriter, nil).AnyTimes()
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
	err = writeHandle.FlushAttempt("test_flush")
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

func TestFlushFile(t *testing.T) {
	t.Skip()
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
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
	fs, _ := NewFileSystem([]HdfsAccessor{hdfsAccessor}, "/", []string{"*"}, false, NewDefaultRetryPolicy(mockClock), mockClock)

	hdfsAccessor.EXPECT().Remove(fileName).Return(nil).AnyTimes()
	hdfsAccessor.EXPECT().CreateFile(fileName, os.FileMode(0757), true).Return(hdfswriter, nil).AnyTimes()
	hdfswriter.EXPECT().Close().Return(nil).AnyTimes()
	hdfswriter.EXPECT().Write([]byte("hello world")).Return(0, nil).AnyTimes()

	// Test for newfilehandlewriter with existing file
	root, _ := fs.Root()
	file := root.(*DirINode).NodeFromAttrs(Attrs{Name: "testWriteFile_2", Mode: os.FileMode(0757)}).(*FileINode)
	fh, _ := file.Open(nil, &fuse.OpenRequest{}, &fuse.OpenResponse{})
	fileHandle := fh.(*FileHandle)

	// Test for flush
	_ = fileHandle.Write(nil, &fuse.WriteRequest{Data: []byte("hello world"), Offset: int64(0)}, &fuse.WriteResponse{})
	err := fileHandle.Flush(nil, nil)
	assert.Nil(t, err)

	err = fileHandle.Release(nil, nil)
	assert.Nil(t, err)
}
