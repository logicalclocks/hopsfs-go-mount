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
	hdfsAccessor.EXPECT().TryReserve().Return(true).AnyTimes()
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

// Attributes from a listing stay valid for cacheListingAttrsSecs, counted from
// the end of the listing, for files and directories alike.
func TestListingAttrsLastListingTTL(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	hdfsAccessor.EXPECT().TryReserve().Return(true).AnyTimes()
	fs, _ := NewFileSystem([]HdfsAccessor{hdfsAccessor}, "/", []string{"*"}, false, DelaySyncUntilClose, NewDefaultRetryPolicy(mockClock), mockClock)
	root, _ := fs.Root()
	// The accessor stamps every entry with the stat expiry, and this listing takes 10 s.
	hdfsAccessor.EXPECT().ReadDir("/").DoAndReturn(func(string) ([]Attrs, error) {
		statExpiry := mockClock.Now().Add(CacheAttrsTimeDuration)
		mockClock.NotifyTimeElapsed(10 * time.Second)
		return []Attrs{
			{Name: "dir1", Mode: os.ModeDir | 0755, Expires: statExpiry},
			{Name: "file1", Mode: 0644, Size: 1, Expires: statExpiry},
		}, nil
	})
	_, err := root.(*DirINode).ReadDirAll(nil)
	assert.Nil(t, err)
	dir, err := root.(*DirINode).Lookup(nil, "dir1")
	assert.Nil(t, err)
	file, err := root.(*DirINode).Lookup(nil, "file1")
	assert.Nil(t, err)

	// Within the window both are served from the listing: the mock fails the test on any Stat.
	mockClock.NotifyTimeElapsed(CacheListingAttrsDuration - time.Second)
	var attr fuse.Attr
	assert.Nil(t, dir.Attr(nil, &attr))
	assert.Equal(t, os.ModeDir|0755, attr.Mode)
	assert.Nil(t, file.Attr(nil, &attr))
	assert.Equal(t, uint64(1), attr.Size)

	hdfsAccessor.EXPECT().Stat("/dir1").Return(Attrs{Name: "dir1", Mode: os.ModeDir | 0700}, nil)
	hdfsAccessor.EXPECT().Stat("/file1").Return(Attrs{Name: "file1", Mode: 0644, Size: 2}, nil)
	mockClock.NotifyTimeElapsed(2 * time.Second)
	assert.Nil(t, dir.Attr(nil, &attr))
	assert.Equal(t, os.ModeDir|0700, attr.Mode)
	assert.Nil(t, file.Attr(nil, &attr))
	assert.Equal(t, uint64(2), attr.Size)
}

// Attributes a stat brings keep cacheAttrsTimeSecs, also for a node a listing created.
func TestStatAttrsKeepStatTTL(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	hdfsAccessor.EXPECT().TryReserve().Return(true).AnyTimes()
	fs, _ := NewFileSystem([]HdfsAccessor{hdfsAccessor}, "/", []string{"*"}, false, DelaySyncUntilClose, NewDefaultRetryPolicy(mockClock), mockClock)
	root, _ := fs.Root()
	hdfsAccessor.EXPECT().ReadDir("/").Return([]Attrs{{Name: "file1", Mode: 0644, Size: 1}}, nil)
	_, err := root.(*DirINode).ReadDirAll(nil)
	assert.Nil(t, err)
	file, err := root.(*DirINode).Lookup(nil, "file1")
	assert.Nil(t, err)

	mockClock.NotifyTimeElapsed(CacheListingAttrsDuration + time.Second)
	hdfsAccessor.EXPECT().Stat("/file1").Return(Attrs{Name: "file1", Mode: 0644, Size: 2, Expires: mockClock.Now().Add(CacheAttrsTimeDuration)}, nil)
	var attr fuse.Attr
	assert.Nil(t, file.Attr(nil, &attr))
	assert.Equal(t, uint64(2), attr.Size)

	mockClock.NotifyTimeElapsed(CacheAttrsTimeDuration - time.Second)
	assert.Nil(t, file.Attr(nil, &attr))
	assert.Equal(t, uint64(2), attr.Size)

	hdfsAccessor.EXPECT().Stat("/file1").Return(Attrs{Name: "file1", Mode: 0644, Size: 3}, nil)
	mockClock.NotifyTimeElapsed(2 * time.Second)
	assert.Nil(t, file.Attr(nil, &attr))
	assert.Equal(t, uint64(3), attr.Size)
}

// Testing whether '-allowedPrefixes' path filtering works for ReadDir
func TestReadDirWithFiltering(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	hdfsAccessor.EXPECT().TryReserve().Return(true).AnyTimes()
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
	hdfsAccessor.EXPECT().TryReserve().Return(true).AnyTimes()
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
	hdfsAccessor.EXPECT().TryReserve().Return(true).AnyTimes()
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
	hdfsAccessor.EXPECT().TryReserve().Return(true).AnyTimes()
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
	hdfsAccessor.EXPECT().TryReserve().Return(true).AnyTimes()
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
	hdfsAccessor.EXPECT().TryReserve().Return(true).AnyTimes()
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

// The kernel sends CREATE for a name its last lookup said was absent: this
// mount's negative cache, or another client created the file since. Without
// O_EXCL the open must succeed, but with the kernel's permission check on the
// file, which it never makes for a CREATE. ESTALE sends the kernel back to a
// fresh lookup, which must now find the file, and to an OPEN of it.
func TestCreateOfAnExistingFileSendsTheKernelBackToLookItUp(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	hdfsAccessor.EXPECT().TryReserve().Return(true).AnyTimes()
	fs, _ := NewFileSystem([]HdfsAccessor{hdfsAccessor}, "/", []string{"*"}, false, DelaySyncUntilClose, NewDefaultRetryPolicy(mockClock), mockClock)
	root, _ := fs.Root()

	// The lookup that told the kernel the name was absent.
	hdfsAccessor.EXPECT().Stat("/existing").Return(Attrs{}, syscall.ENOENT)
	_, err := root.(*DirINode).Lookup(nil, "existing")
	assert.Equal(t, syscall.ENOENT, err)

	hdfsAccessor.EXPECT().CreateFileWithGroup("/existing", gomock.Any(), false, gomock.Any()).Return(nil, syscall.EEXIST).Times(2)
	_, _, err = root.(*DirINode).Create(nil, &fuse.CreateRequest{Name: "existing",
		Flags: fuse.OpenReadWrite | fuse.OpenCreate | fuse.OpenExclusive, Mode: os.FileMode(0644)}, &fuse.CreateResponse{})
	assert.Equal(t, syscall.EEXIST, err)

	_, _, err = root.(*DirINode).Create(nil, &fuse.CreateRequest{Name: "existing",
		Flags: fuse.OpenReadWrite | fuse.OpenCreate | fuse.OpenTruncate, Mode: os.FileMode(0644)}, &fuse.CreateResponse{})
	assert.Equal(t, syscall.ESTALE, err)

	// The kernel's retried lookup reaches the backend, not the negative cache.
	hdfsAccessor.EXPECT().Stat("/existing").Return(Attrs{Name: "existing", Mode: os.FileMode(0444), Size: 5}, nil)
	node, err := root.(*DirINode).Lookup(nil, "existing")
	assert.Nil(t, err)
	if assert.NotNil(t, node) {
		assert.Equal(t, 0, node.(*FileINode).countActiveHandles())
	}
}

// A new file's DFS writer holds the file's lease and only Flush closes it,
// which never comes for a failed CREATE.
func TestCreateOfANewFileClosesItsWriterWhenTheFinalStatFails(t *testing.T) {
	originalCache := StagingCache
	t.Cleanup(func() { StagingCache = originalCache })
	StagingCache = nil

	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	hdfsAccessor.EXPECT().TryReserve().Return(true).AnyTimes()
	fs, _ := NewFileSystem([]HdfsAccessor{hdfsAccessor}, "/", []string{"*"}, false, DelaySyncUntilClose, NewDefaultRetryPolicy(mockClock), mockClock)
	root, _ := fs.Root()
	file := root.(*DirINode).addOrUpdateChildInodeAttrs("unit_test", "newfile", Attrs{Name: "newfile", Mode: os.FileMode(0644)}).(*FileINode)

	hdfswriter := NewMockHdfsWriter(mockCtrl)
	hdfswriter.EXPECT().Close().Return(nil)
	hdfsAccessor.EXPECT().CreateFileWithGroup("/newfile", gomock.Any(), false, gomock.Any()).Return(hdfswriter, nil)
	hdfsAccessor.EXPECT().Stat("/newfile").Return(Attrs{}, syscall.EIO)

	_, _, err := root.(*DirINode).Create(nil, &fuse.CreateRequest{Name: "newfile",
		Flags: fuse.OpenReadWrite | fuse.OpenCreate, Mode: os.FileMode(0644)}, &fuse.CreateResponse{})
	assert.Equal(t, syscall.EIO, err)
	assert.Equal(t, 0, file.countActiveHandles())
	assert.Nil(t, file.pendingDFSWriter)
}

// Testing that Mkdir invalidates negative cache
func TestNegativeCacheInvalidatedByMkdir(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	hdfsAccessor.EXPECT().TryReserve().Return(true).AnyTimes()
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
	hdfsAccessor.EXPECT().TryReserve().Return(true).AnyTimes()
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
	hdfsAccessor.EXPECT().TryReserve().Return(true).AnyTimes()
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
	hdfsAccessor.EXPECT().TryReserve().Return(true).AnyTimes()
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
	hdfsAccessor.EXPECT().TryReserve().Return(true).AnyTimes()
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

// newLookupPlusTestRoot lists listing at / through a mock accessor that
// expects no other call, so a LookupPlus that reached the NameNode fails the
// test. A test that lists again adds its own expectation.
func newLookupPlusTestRoot(t *testing.T, allowedPrefixes []string, listing []Attrs) (*DirINode, *MockClock, *MockHdfsAccessor) {
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	hdfsAccessor.EXPECT().TryReserve().Return(true).AnyTimes()
	fs, _ := NewFileSystem([]HdfsAccessor{hdfsAccessor}, "/", allowedPrefixes, false, DelaySyncUntilClose, NewDefaultRetryPolicy(mockClock), mockClock)
	root, _ := fs.Root()
	hdfsAccessor.EXPECT().ReadDir("/").Return(listing, nil)
	if _, err := root.(*DirINode).ReadDirAll(nil); err != nil {
		t.Fatal(err)
	}
	return root.(*DirINode), mockClock, hdfsAccessor
}

var lookupPlusTestListing = []Attrs{
	{Name: "dir1", Mode: os.ModeDir | 0755},
	{Name: "file1", Mode: 0644, Size: 1},
}

func TestLookupPlusNeverCallsBackend(t *testing.T) {
	root, mockClock, _ := newLookupPlusTestRoot(t, []string{"*"}, lookupPlusTestListing)
	var attr fuse.Attr
	_, ok := root.LookupPlus(nil, "dir1", &attr)
	assert.True(t, ok)
	assert.Equal(t, os.ModeDir|0755, attr.Mode)
	file, ok := root.LookupPlus(nil, "file1", &attr)
	assert.True(t, ok)
	assert.Equal(t, os.FileMode(0644), attr.Mode)
	assert.Equal(t, uint64(1), attr.Size)

	// Expired listing attributes are not answered: the kernel looks the names up.
	mockClock.NotifyTimeElapsed(CacheListingAttrsDuration + time.Second)
	_, ok = root.LookupPlus(nil, "dir1", &attr)
	assert.False(t, ok)
	_, ok = root.LookupPlus(nil, "file1", &attr)
	assert.False(t, ok)

	// An open file reports its staging file's size, as Attr does, however old the listing.
	staging, err := os.CreateTemp(t.TempDir(), "staging")
	assert.Nil(t, err)
	_, err = staging.Write([]byte("12345"))
	assert.Nil(t, err)
	fileNode := file.(*FileINode)
	fileNode.fileProxy = &LocalRWFileProxy{localFile: staging, file: fileNode}
	got, ok := root.LookupPlus(nil, "file1", &attr)
	assert.True(t, ok)
	assert.Equal(t, file, got)
	assert.Equal(t, uint64(5), attr.Size)

	// A staging file whose stat fails (the test closes it) is not answered.
	assert.Nil(t, staging.Close())
	_, ok = root.LookupPlus(nil, "file1", &attr)
	assert.False(t, ok)
}

func TestLookupPlusUnlistedOrFilteredName(t *testing.T) {
	root, _, _ := newLookupPlusTestRoot(t, []string{"foo"}, []Attrs{
		{Name: "foo", Mode: 0644, Size: 1},
		{Name: "bar", Mode: 0644, Size: 1},
	})
	var attr fuse.Attr
	_, ok := root.LookupPlus(nil, "foo", &attr)
	assert.True(t, ok)
	_, ok = root.LookupPlus(nil, "bar", &attr)
	assert.False(t, ok, "a name -allowedPrefixes filters out is not answered")
	_, ok = root.LookupPlus(nil, "never-listed", &attr)
	assert.False(t, ok)
}

// A child whose lock is held, as during a stat or an open that may wait on
// the NameNode, is not answered rather than waited for.
func TestLookupPlusBusyChildReturnsNoAttrs(t *testing.T) {
	root, _, _ := newLookupPlusTestRoot(t, []string{"*"}, lookupPlusTestListing)
	dir, _ := root.Lookup(nil, "dir1")
	file, _ := root.Lookup(nil, "file1")
	var attr fuse.Attr

	dir.(*DirINode).dirMutex.Lock()
	_, ok := root.LookupPlus(nil, "dir1", &attr)
	assert.False(t, ok)
	dir.(*DirINode).dirMutex.Unlock()

	file.(*FileINode).fileMutex.Lock()
	_, ok = root.LookupPlus(nil, "file1", &attr)
	assert.False(t, ok)
	file.(*FileINode).fileMutex.Unlock()

	file.(*FileINode).fileHandleMutex.Lock()
	_, ok = root.LookupPlus(nil, "file1", &attr)
	assert.False(t, ok)
	file.(*FileINode).fileHandleMutex.Unlock()

	_, ok = root.LookupPlus(nil, "dir1", &attr)
	assert.True(t, ok)
	_, ok = root.LookupPlus(nil, "file1", &attr)
	assert.True(t, ok)
}

// A listing rewrites a child's Attrs while LookupPlus copies them. Run with
// -race: both sides must hold the parent's childrenMutex.
func TestLookupPlusConcurrentListing(t *testing.T) {
	root, _, hdfsAccessor := newLookupPlusTestRoot(t, []string{"*"}, lookupPlusTestListing)
	hdfsAccessor.EXPECT().ReadDir("/").Return(lookupPlusTestListing, nil).AnyTimes()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 500; i++ {
			if _, err := root.ReadDirAll(nil); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	var attr fuse.Attr
	for i := 0; i < 500; i++ {
		root.LookupPlus(nil, "file1", &attr)
		root.LookupPlus(nil, "dir1", &attr)
	}
	<-done
}
