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
	hdfsAccessor.EXPECT().IsAvailable().Return(true).AnyTimes()
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
	hdfsAccessor.EXPECT().IsAvailable().Return(true).AnyTimes()
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
	hdfsAccessor.EXPECT().IsAvailable().Return(true).AnyTimes()
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
	hdfsAccessor.EXPECT().IsAvailable().Return(true).AnyTimes()
	fs, _ := NewFileSystem([]HdfsAccessor{hdfsAccessor}, "/", []string{"foo", "bar"}, false, DelaySyncUntilClose, NewDefaultRetryPolicy(mockClock), mockClock)
	root, _ := fs.Root()
	hdfsAccessor.EXPECT().Stat("/foo").Return(Attrs{Name: "foo", Mode: os.ModeDir}, nil)
	_, err := root.(*DirINode).Lookup(nil, "foo")
	assert.Nil(t, err)
	_, err = root.(*DirINode).Lookup(nil, "qux")
	assert.Equal(t, syscall.ENOENT, err) // Not found error, since it is not in the allowed prefixes
}

// Testing negative lookup cache - ENOENT results are cached
func TestNegativeLookupCache(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	hdfsAccessor.EXPECT().IsAvailable().Return(true).AnyTimes()
	fs, _ := NewFileSystem([]HdfsAccessor{hdfsAccessor}, "/", []string{"*"}, false, DelaySyncUntilClose, NewDefaultRetryPolicy(mockClock), mockClock)
	root, _ := fs.Root()

	// First lookup should hit the backend and get ENOENT
	hdfsAccessor.EXPECT().Stat("/nonexistent").Return(Attrs{}, syscall.ENOENT)
	_, err := root.(*DirINode).Lookup(nil, "nonexistent")
	assert.Equal(t, syscall.ENOENT, err)

	// Second lookup should be served from negative cache (no backend Stat call)
	_, err = root.(*DirINode).Lookup(nil, "nonexistent")
	assert.Equal(t, syscall.ENOENT, err)
}

// Testing that negative cache entries expire after CacheAttrsTimeDuration
func TestNegativeLookupCacheExpiry(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	hdfsAccessor.EXPECT().IsAvailable().Return(true).AnyTimes()
	fs, _ := NewFileSystem([]HdfsAccessor{hdfsAccessor}, "/", []string{"*"}, false, DelaySyncUntilClose, NewDefaultRetryPolicy(mockClock), mockClock)
	root, _ := fs.Root()

	// First lookup - backend returns ENOENT
	hdfsAccessor.EXPECT().Stat("/vanishing").Return(Attrs{}, syscall.ENOENT)
	_, err := root.(*DirINode).Lookup(nil, "vanishing")
	assert.Equal(t, syscall.ENOENT, err)

	// Cached - no backend call
	_, err = root.(*DirINode).Lookup(nil, "vanishing")
	assert.Equal(t, syscall.ENOENT, err)

	// Advance time past TTL - should re-query backend, this time the file exists
	mockClock.NotifyTimeElapsed(CacheAttrsTimeDuration + time.Second)
	hdfsAccessor.EXPECT().Stat("/vanishing").Return(Attrs{Name: "vanishing", Mode: 0644}, nil)
	node, err := root.(*DirINode).Lookup(nil, "vanishing")
	assert.Nil(t, err)
	assert.NotNil(t, node)
}

// Testing that Create invalidates negative cache
func TestNegativeCacheInvalidatedByCreate(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	hdfsAccessor.EXPECT().IsAvailable().Return(true).AnyTimes()
	fs, _ := NewFileSystem([]HdfsAccessor{hdfsAccessor}, "/", []string{"*"}, false, DelaySyncUntilClose, NewDefaultRetryPolicy(mockClock), mockClock)
	root, _ := fs.Root()

	// Lookup returns ENOENT - populates negative cache
	hdfsAccessor.EXPECT().Stat("/newfile").Return(Attrs{}, syscall.ENOENT)
	_, err := root.(*DirINode).Lookup(nil, "newfile")
	assert.Equal(t, syscall.ENOENT, err)

	// Create the file - should invalidate the negative cache entry
	hdfswriter := NewMockHdfsWriter(mockCtrl)
	hdfswriter.EXPECT().Close().Return(nil).AnyTimes()
	hdfsAccessor.EXPECT().CreateFileWithGroup("/newfile", gomock.Any(), gomock.Any(), gomock.Any()).Return(hdfswriter, nil).AnyTimes()
	hdfsAccessor.EXPECT().Stat("/newfile").Return(Attrs{Name: "newfile", Mode: 0644}, nil).AnyTimes()
	hdfsAccessor.EXPECT().Chown("/newfile", gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	hdfsAccessor.EXPECT().Remove("/newfile").Return(nil).AnyTimes()
	hdfsAccessor.EXPECT().StatFs().Return(FsInfo{capacity: uint64(100), used: uint64(20), remaining: uint64(80)}, nil).AnyTimes()

	_, _, err = root.(*DirINode).Create(nil, &fuse.CreateRequest{Name: "newfile",
		Flags: fuse.OpenReadWrite | fuse.OpenCreate, Mode: os.FileMode(0644)}, &fuse.CreateResponse{})
	assert.Nil(t, err)

	// Subsequent lookup should find the file (not return ENOENT from cache)
	node, err := root.(*DirINode).Lookup(nil, "newfile")
	assert.Nil(t, err)
	assert.NotNil(t, node)
}

// Testing that Mkdir invalidates negative cache
func TestNegativeCacheInvalidatedByMkdir(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	hdfsAccessor.EXPECT().IsAvailable().Return(true).AnyTimes()
	fs, _ := NewFileSystem([]HdfsAccessor{hdfsAccessor}, "/", []string{"*"}, false, DelaySyncUntilClose, NewDefaultRetryPolicy(mockClock), mockClock)
	root, _ := fs.Root()

	// Lookup returns ENOENT - populates negative cache
	hdfsAccessor.EXPECT().Stat("/newdir").Return(Attrs{}, syscall.ENOENT)
	_, err := root.(*DirINode).Lookup(nil, "newdir")
	assert.Equal(t, syscall.ENOENT, err)

	// Mkdir the directory - should invalidate the negative cache entry
	hdfsAccessor.EXPECT().MkdirWithGroup("/newdir", os.ModeDir|os.FileMode(0755), gomock.Any()).Return(nil)
	hdfsAccessor.EXPECT().Chown("/newdir", gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	node, err := root.(*DirINode).Mkdir(nil, &fuse.MkdirRequest{Name: "newdir", Mode: os.ModeDir | os.FileMode(0755)})
	assert.Nil(t, err)
	assert.Equal(t, "newdir", node.(*DirINode).Attrs.Name)

	// Subsequent lookup should find the directory (not return ENOENT from cache)
	node2, err := root.(*DirINode).Lookup(nil, "newdir")
	assert.Nil(t, err)
	assert.NotNil(t, node2)
}

// Testing that Rename invalidates negative cache in destination directory
func TestNegativeCacheInvalidatedByRename(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	hdfsAccessor.EXPECT().IsAvailable().Return(true).AnyTimes()
	fs, _ := NewFileSystem([]HdfsAccessor{hdfsAccessor}, "/", []string{"*"}, false, DelaySyncUntilClose, NewDefaultRetryPolicy(mockClock), mockClock)
	root, _ := fs.Root()

	// Create a source file in the children cache
	rootDir := root.(*DirINode)
	rootDir.addOrUpdateChildInodeAttrs("test", "srcfile", Attrs{Name: "srcfile", Mode: 0644})

	// Lookup "dstfile" returns ENOENT - populates negative cache
	hdfsAccessor.EXPECT().Stat("/dstfile").Return(Attrs{}, syscall.ENOENT)
	_, err := rootDir.Lookup(nil, "dstfile")
	assert.Equal(t, syscall.ENOENT, err)

	// Rename srcfile -> dstfile should invalidate the negative cache for "dstfile"
	hdfsAccessor.EXPECT().Rename2("/srcfile", "/dstfile", gomock.Any()).Return(nil)
	err = rootDir.Rename(nil, &fuse.RenameRequest{OldName: "srcfile", NewName: "dstfile"}, rootDir)
	assert.Nil(t, err)

	// Subsequent lookup should find the renamed file (not return ENOENT from cache)
	node, err := rootDir.Lookup(nil, "dstfile")
	assert.Nil(t, err)
	assert.NotNil(t, node)
}

// Testing that non-ENOENT errors are NOT cached
func TestNegativeCacheOnlyForENOENT(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	hdfsAccessor.EXPECT().IsAvailable().Return(true).AnyTimes()
	fs, _ := NewFileSystem([]HdfsAccessor{hdfsAccessor}, "/", []string{"*"}, false, DelaySyncUntilClose, NewDefaultRetryPolicy(mockClock), mockClock)
	root, _ := fs.Root()

	// First lookup returns EIO (not ENOENT) - should NOT be cached
	hdfsAccessor.EXPECT().Stat("/ioerr").Return(Attrs{}, syscall.EIO)
	_, err := root.(*DirINode).Lookup(nil, "ioerr")
	assert.Equal(t, syscall.EIO, err)

	// Second lookup should hit backend again (not served from cache)
	hdfsAccessor.EXPECT().Stat("/ioerr").Return(Attrs{Name: "ioerr", Mode: 0644}, nil)
	node, err := root.(*DirINode).Lookup(nil, "ioerr")
	assert.Nil(t, err)
	assert.NotNil(t, node)
}

// Testing Mkdir
func TestMkdir(t *testing.T) {
	dir := "/foo"
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	hdfsAccessor.EXPECT().IsAvailable().Return(true).AnyTimes()
	hdfsAccessor.EXPECT().Chown(dir, gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	fs, _ := NewFileSystem([]HdfsAccessor{hdfsAccessor}, "/", []string{"foo", "bar"}, false, DelaySyncUntilClose, NewDefaultRetryPolicy(mockClock), mockClock)
	root, _ := fs.Root()
	hdfsAccessor.EXPECT().MkdirWithGroup(dir, os.FileMode(0757)|os.ModeDir, gomock.Any()).Return(nil)
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
	hdfsAccessor.EXPECT().IsAvailable().Return(true).AnyTimes()
	hdfsAccessor.EXPECT().Chown(dir, gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	fs, _ := NewFileSystem([]HdfsAccessor{hdfsAccessor}, "/", []string{"foo", "bar"}, false, DelaySyncUntilClose, NewDefaultRetryPolicy(mockClock), mockClock)
	root, _ := fs.Root()
	hdfsAccessor.EXPECT().MkdirWithGroup(dir, os.FileMode(0757)|os.ModeDir, gomock.Any()).Return(nil)
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
