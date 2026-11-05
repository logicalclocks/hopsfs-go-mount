// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package hopsfsmount

import (
	"os"
	"syscall"
	"testing"
	"time"

	"bazil.org/fuse"
	"github.com/golang/mock/gomock"
	"github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
)

// A file the mount knows that the backend no longer has is the one ENOENT on
// a known file. The sidecar logs at warning level by default, so anything
// quieter would leave no trace of it.
func TestAttrWarnsWhenAKnownFileIsGoneFromTheBackend(t *testing.T) {
	hooks := logrus.StandardLogger().ReplaceHooks(make(logrus.LevelHooks))
	t.Cleanup(func() { logrus.StandardLogger().ReplaceHooks(hooks) })
	// The suite's integration tests leave the logger at ERROR, below which a
	// warning never reaches a hook.
	level := logrus.GetLevel()
	logrus.SetLevel(logrus.WarnLevel)
	t.Cleanup(func() { logrus.SetLevel(level) })
	logs := logtest.NewGlobal()

	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	hdfsAccessor.EXPECT().TryReserve().Return(true).AnyTimes()
	fs, _ := NewFileSystem([]HdfsAccessor{hdfsAccessor}, "/", []string{"*"}, false, DelaySyncUntilClose, NewDefaultRetryPolicy(mockClock), mockClock)
	root, _ := fs.Root()
	file := root.(*DirINode).addOrUpdateChildInodeAttrs("unit_test", "gone", Attrs{Name: "gone", Mode: os.FileMode(0644), Expires: mockClock.Now()}).(*FileINode)
	mockClock.NotifyTimeElapsed(time.Second)

	hdfsAccessor.EXPECT().Stat("/gone").Return(Attrs{}, syscall.ENOENT)
	var attr fuse.Attr
	assert.Equal(t, syscall.ENOENT, file.Attr(nil, &attr))

	var warnings []string
	for _, entry := range logs.AllEntries() {
		if entry.Level == logrus.WarnLevel {
			warnings = append(warnings, entry.Message)
		}
	}
	assert.Equal(t, []string{"Known file not found on backend"}, warnings)
}

// reserveLikeTheAccessor makes a mock connector grant a reservation only
// while none is outstanding, as HdfsAccessorImpl does, and returns what a
// mocked call runs to take the reservation up.
func reserveLikeTheAccessor(connector *MockHdfsAccessor) (takeUp func()) {
	reserved := false
	connector.EXPECT().TryReserve().DoAndReturn(func() bool {
		if reserved {
			return false
		}
		reserved = true
		return true
	}).AnyTimes()
	return func() { reserved = false }
}

// failOnExtraConnector gives the file system room for one more connector and
// fails the test if it is added: with the only connector reserved and not
// yet called, a second request would add it.
func failOnExtraConnector(t *testing.T, mockCtrl *gomock.Controller) FileSystemOption {
	extra := NewMockHdfsAccessor(mockCtrl)
	extra.EXPECT().TryReserve().Return(true).AnyTimes()
	extra.EXPECT().OpenRead(gomock.Any()).Return(&MockReadSeekCloserWithPseudoRandomContent{FileSize: 5, ReaderStats: &ReaderStats{}}, nil).AnyTimes()
	extra.EXPECT().StatFs().Return(FsInfo{}, nil).AnyTimes()
	return WithExtraConnectors(1, func() (HdfsAccessor, error) {
		t.Error("a connector was added while the only one was reserved by a request that had not called it")
		return extra, nil
	})
}

// An open that downloads the file into the staging cache takes a connector
// for its one call only: one taken before the cache lookup and left unused
// while the download takes another would hold the open until the
// reservation lapses, or add a connector.
func TestOpenThroughTheStagingCacheTakesOneConnector(t *testing.T) {
	originalCache := StagingCache
	t.Cleanup(func() { StagingCache = originalCache })
	StagingCache = newTestCache(t, 10)

	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	connector := NewMockHdfsAccessor(mockCtrl)
	takeUp := reserveLikeTheAccessor(connector)
	connector.EXPECT().OpenRead("/cached").DoAndReturn(func(string) (ReadSeekCloser, error) {
		takeUp()
		return &MockReadSeekCloserWithPseudoRandomContent{FileSize: 5, ReaderStats: &ReaderStats{}}, nil
	})
	fs, _ := NewFileSystem([]HdfsAccessor{connector}, "/", []string{"*"}, false, DelaySyncUntilClose, NewDefaultRetryPolicy(mockClock), mockClock,
		failOnExtraConnector(t, mockCtrl))
	root, _ := fs.Root()
	file := root.(*DirINode).addOrUpdateChildInodeAttrs("unit_test", "cached", Attrs{Name: "cached", Mode: os.FileMode(0644), Size: 5}).(*FileINode)

	h, err := file.Open(nil, &fuse.OpenRequest{Flags: fuse.OpenReadOnly}, &fuse.OpenResponse{})
	assert.Nil(t, err)
	_, staged := h.(*FileHandle).File.fileProxy.(*LocalRWFileProxy)
	assert.True(t, staged, "the open did not download the file into the staging cache")
}

// A flush of a new file uploads through the writer its create kept, and
// takes a connector only for the stat after the upload: one taken before the
// upload would keep every other request off it, or add a connector, for as
// long as the upload runs.
func TestFlushOfANewFileHoldsNoConnectorDuringTheUpload(t *testing.T) {
	originalCache := StagingCache
	t.Cleanup(func() { StagingCache = originalCache })
	StagingCache = newTestCache(t, 10)

	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	connector := NewMockHdfsAccessor(mockCtrl)
	takeUp := reserveLikeTheAccessor(connector)
	fileName := "/flushedFile"
	var fs *FileSystem
	requestedDuringUpload := false
	writer := NewMockHdfsWriter(mockCtrl)
	writer.EXPECT().Write(gomock.Any()).DoAndReturn(func(p []byte) (int, error) {
		if !requestedDuringUpload {
			requestedDuringUpload = true
			_, err := fs.getDFSConnector().StatFs()
			assert.Nil(t, err)
		}
		return len(p), nil
	}).AnyTimes()
	writer.EXPECT().Close().Return(nil).AnyTimes()
	connector.EXPECT().CreateFileWithGroup(fileName, gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(string, os.FileMode, bool, string) (HdfsWriter, error) { takeUp(); return writer, nil }).AnyTimes()
	connector.EXPECT().Remove(fileName).DoAndReturn(func(string) error { takeUp(); return nil }).AnyTimes()
	connector.EXPECT().Stat(fileName).DoAndReturn(func(string) (Attrs, error) { takeUp(); return Attrs{Name: fileName}, nil }).AnyTimes()
	connector.EXPECT().Chown(fileName, gomock.Any(), gomock.Any()).DoAndReturn(func(string, string, string) error { takeUp(); return nil }).AnyTimes()
	connector.EXPECT().StatFs().DoAndReturn(func() (FsInfo, error) { takeUp(); return FsInfo{}, nil }).AnyTimes()
	fs, _ = NewFileSystem([]HdfsAccessor{connector}, "/", []string{"*"}, false, DelaySyncUntilClose, NewDefaultRetryPolicy(mockClock), mockClock,
		failOnExtraConnector(t, mockCtrl))

	root, _ := fs.Root()
	_, h, err := root.(*DirINode).Create(nil, &fuse.CreateRequest{Name: fileName,
		Flags: fuse.OpenReadWrite | fuse.OpenCreate, Mode: os.FileMode(0644)}, &fuse.CreateResponse{})
	assert.Nil(t, err)
	fileHandle := h.(*FileHandle)
	assert.Nil(t, fileHandle.Write(nil, &fuse.WriteRequest{Data: []byte("hello world"), Offset: 0}, &fuse.WriteResponse{}))
	assert.Nil(t, fileHandle.Flush(nil, nil))
	assert.True(t, requestedDuringUpload, "the flush did not upload through the writer")
	assert.Nil(t, fileHandle.Release(nil, nil))
}
