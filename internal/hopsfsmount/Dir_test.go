// Copyright (c) Microsoft. All rights reserved.
// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package hopsfsmount

import (
	"syscall"

	"bazil.org/fuse"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"os"
	"testing"
	"time"
)

// Testing whether attributes are cached
func TestAttributeCaching(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	fs, _ := NewFileSystem([]HdfsAccessor{hdfsAccessor}, "/", []string{"*"}, false, DelaySyncUntilClose, NewDefaultRetryPolicy(mockClock), mockClock)
	root, _ := fs.Root()
	hdfsAccessor.EXPECT().Stat("/testDir").Return(Attrs{Name: "testDir", Mode: os.ModeDir | 0757, Expires: fs.Clock.Now().Add(CacheAttrsTimeDuration)}, nil)
	dir, err := root.(*DirINode).Lookup(nil, "testDir")
	assert.Nil(t, err)
	// Second call to Lookup(), shouldn't re-issue Stat() on backend
	dir1, err1 := root.(*DirINode).Lookup(nil, "testDir")
	assert.Nil(t, err1)
	assert.Equal(t, dir, dir1) // must return the same entry w/o doing Stat on the backend

	// Retrieving attributes from cache
	var attr fuse.Attr
	assert.Nil(t, dir.Attr(nil, &attr))
	assert.Equal(t, os.ModeDir|0757, attr.Mode)

	mockClock.NotifyTimeElapsed(2 * time.Second)
	assert.Nil(t, dir.Attr(nil, &attr))
	assert.Equal(t, os.ModeDir|0757, attr.Mode)

	// Lookup should be stil done from cache
	dir1, err1 = root.(*DirINode).Lookup(nil, "testDir")
	assert.Nil(t, err1)

	// After 30+31=61 seconds, attempt to query attributes should re-issue a Stat() request to the backend
	// this time returing different attributes (555 instead of 757)
	hdfsAccessor.EXPECT().Stat("/testDir").Return(Attrs{Name: "testDir", Mode: os.ModeDir | 0555}, nil)
	mockClock.NotifyTimeElapsed(4 * time.Second)
	assert.Nil(t, dir.Attr(nil, &attr))
	assert.Equal(t, os.ModeDir|0555, attr.Mode)
	dir1, err1 = root.(*DirINode).Lookup(nil, "testDir")
	assert.Nil(t, err1)
	assert.Equal(t, dir, dir1)
}

// Testing whether '-allowedPrefixes' path filtering works for ReadDir
func TestReadDirWithFiltering(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	fs, _ := NewFileSystem([]HdfsAccessor{hdfsAccessor}, "/", []string{"foo", "bar"}, false, DelaySyncUntilClose, NewDefaultRetryPolicy(mockClock), mockClock)
	root, _ := fs.Root()
	hdfsAccessor.EXPECT().ReadDir("/").Return([]Attrs{
		{Name: "quz", Mode: os.ModeDir},
		{Name: "foo", Mode: os.ModeDir},
		{Name: "bar", Mode: os.ModeDir},
		{Name: "foobar", Mode: os.ModeDir},
		{Name: "baz", Mode: os.ModeDir},
	}, nil)
	dirents, err := root.(*DirINode).ReadDirAll(nil)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(dirents))
	assert.Equal(t, "foo", dirents[0].Name)
	assert.Equal(t, "bar", dirents[1].Name)
}

// Testing processing of .zip files if '-expandZips' isn't activated
func TestReadDirWithZipExpansionDisabled(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	fs, _ := NewFileSystem([]HdfsAccessor{hdfsAccessor}, "/", []string{"*"}, false, DelaySyncUntilClose, NewDefaultRetryPolicy(mockClock), mockClock)
	root, _ := fs.Root()
	hdfsAccessor.EXPECT().ReadDir("/").Return([]Attrs{
		{Name: "foo.zipx"},
		{Name: "dir.zip", Mode: os.ModeDir},
		{Name: "bar.zip"},
	}, nil)
	dirents, err := root.(*DirINode).ReadDirAll(nil)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(dirents))
	assert.Equal(t, "foo.zipx", dirents[0].Name)
	assert.Equal(t, "dir.zip", dirents[1].Name)
	assert.Equal(t, "bar.zip", dirents[2].Name)
}

// Testing whether '-allowedPrefixes' path filtering works for Lookup
func TestLookupWithFiltering(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	fs, _ := NewFileSystem([]HdfsAccessor{hdfsAccessor}, "/", []string{"foo", "bar"}, false, DelaySyncUntilClose, NewDefaultRetryPolicy(mockClock), mockClock)
	root, _ := fs.Root()
	hdfsAccessor.EXPECT().Stat("/foo").Return(Attrs{Name: "foo", Mode: os.ModeDir}, nil)
	_, err := root.(*DirINode).Lookup(nil, "foo")
	assert.Nil(t, err)
	_, err = root.(*DirINode).Lookup(nil, "qux")
	assert.Equal(t, syscall.ENOENT, err) // Not found error, since it is not in the allowed prefixes
}

// Testing Mkdir
func TestMkdir(t *testing.T) {
	dir := "/foo"
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	hdfsAccessor.EXPECT().Chown(dir, gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	fs, _ := NewFileSystem([]HdfsAccessor{hdfsAccessor}, "/", []string{"foo", "bar"}, false, DelaySyncUntilClose, NewDefaultRetryPolicy(mockClock), mockClock)
	root, _ := fs.Root()
	hdfsAccessor.EXPECT().Mkdir(dir, os.FileMode(0757)|os.ModeDir).Return(nil)
	node, err := root.(*DirINode).Mkdir(nil, &fuse.MkdirRequest{Name: "foo", Mode: os.FileMode(0757) | os.ModeDir})
	assert.Nil(t, err)
	assert.Equal(t, "foo", node.(*DirINode).Attrs.Name)
}

// Testing Chmod and Chown
func TestSetattr(t *testing.T) {
	dir := "/foo"
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	hdfsAccessor.EXPECT().Chown(dir, gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	fs, _ := NewFileSystem([]HdfsAccessor{hdfsAccessor}, "/", []string{"foo", "bar"}, false, DelaySyncUntilClose, NewDefaultRetryPolicy(mockClock), mockClock)
	root, _ := fs.Root()
	hdfsAccessor.EXPECT().Mkdir(dir, os.FileMode(0757)|os.ModeDir).Return(nil)
	node, _ := root.(*DirINode).Mkdir(nil, &fuse.MkdirRequest{Name: "foo", Mode: os.FileMode(0757) | os.ModeDir})
	hdfsAccessor.EXPECT().Chmod(dir, os.FileMode(0777)).Return(nil).AnyTimes()
	err := node.(*DirINode).Setattr(nil, &fuse.SetattrRequest{Mode: os.FileMode(0777), Valid: fuse.SetattrMode}, &fuse.SetattrResponse{})
	assert.Nil(t, err)
	assert.Equal(t, os.FileMode(0777), node.(*DirINode).Attrs.Mode)

	hdfsAccessor.EXPECT().Chown(dir, "root", gomock.Any()).Return(nil).AnyTimes()
	err = node.(*DirINode).Setattr(nil, &fuse.SetattrRequest{Uid: 0, Valid: fuse.SetattrUid}, &fuse.SetattrResponse{})
	assert.Nil(t, err)
	assert.Equal(t, uint32(0), node.(*DirINode).Attrs.Uid)
}
