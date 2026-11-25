// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package hopsfsmount

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"bazil.org/fuse"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func listedFiles(n int, clock *MockClock) []Attrs {
	all := make([]Attrs, n)
	for i := range all {
		all[i] = fileAttrs(fmt.Sprintf("f%05d", i), clock)
	}
	return all
}

func TestListingCreatesNoNodesAndServesTheLookupsThatFollow(t *testing.T) {
	root, hdfsAccessor, clock := newLifecycleTestRoot(t)
	hdfsAccessor.EXPECT().ReadDir("/").Return(listedFiles(1000, clock), nil)

	dirents, err := root.ReadDirAll(nil)

	assert.Nil(t, err)
	assert.Len(t, dirents, 1000)
	assert.Equal(t, 0, childCount(root), "a listed entry gets a node only when it is looked up")

	// No Stat is expected on the mock: the attributes come from the listing.
	node, err := root.Lookup(nil, "f00042")
	assert.Nil(t, err)
	assert.Equal(t, "f00042", node.(*FileINode).Attrs.Name)
	assert.Equal(t, 1, childCount(root))

	// After the node retires the same listing still answers.
	node.(*FileINode).RetireLocked()
	again, err := root.Lookup(nil, "f00042")
	assert.Nil(t, err)
	assert.NotSame(t, node, again)
}

func TestListingExpiresWithTheAttributeLifetime(t *testing.T) {
	root, hdfsAccessor, clock := newLifecycleTestRoot(t)
	hdfsAccessor.EXPECT().ReadDir("/").Return(listedFiles(3, clock), nil)
	root.ReadDirAll(nil)

	clock.NotifyTimeElapsed(CacheAttrsTimeDuration + time.Second)

	hdfsAccessor.EXPECT().Stat("/f00001").Return(fileAttrs("f00001", clock), nil)
	_, err := root.Lookup(nil, "f00001")
	assert.Nil(t, err)
	bytes, entries := root.FileSystem.listings.usage()
	assert.Equal(t, int64(0), bytes)
	assert.Equal(t, int64(0), entries)
}

func TestRemovedEntryIsNotServedFromTheListing(t *testing.T) {
	root, hdfsAccessor, clock := newLifecycleTestRoot(t)
	hdfsAccessor.EXPECT().ReadDir("/").Return(listedFiles(3, clock), nil)
	root.ReadDirAll(nil)

	hdfsAccessor.EXPECT().Remove("/f00001").Return(nil)
	assert.Nil(t, root.Remove(nil, &fuse.RemoveRequest{Name: "f00001"}))

	_, err := root.Lookup(nil, "f00001")
	assert.Equal(t, syscall.ENOENT, err)
	_, entries := root.FileSystem.listings.usage()
	assert.Equal(t, int64(2), entries)
}

func TestRenamedEntryIsNotServedFromTheListingUnderItsOldName(t *testing.T) {
	root, hdfsAccessor, clock := newLifecycleTestRoot(t)
	hdfsAccessor.EXPECT().ReadDir("/").Return(listedFiles(3, clock), nil)
	root.ReadDirAll(nil)

	hdfsAccessor.EXPECT().Rename2("/f00001", "/f00002", gomock.Any()).Return(nil)
	assert.Nil(t, root.Rename(nil, &fuse.RenameRequest{OldName: "f00001", NewName: "f00002"}, root))

	_, err := root.Lookup(nil, "f00001")
	assert.Equal(t, syscall.ENOENT, err)
	// The destination's listed attributes described the overwritten file.
	hdfsAccessor.EXPECT().Stat("/f00002").Return(fileAttrs("f00002", clock), nil)
	_, err = root.Lookup(nil, "f00002")
	assert.Nil(t, err)
}

func TestListingCacheStaysWithinItsBudgetOldestFirst(t *testing.T) {
	clock := &MockClock{}
	now := clock.Now()
	perEntry := int64(listingEntryOverhead + len("f00000"))
	cache := newListingCache(perEntry * 25)
	dirs := []*baseDirNode{{}, {}, {}}

	for _, dir := range dirs {
		cache.store(dir, listedFiles(10, clock), now, 0)
	}

	bytes, entries := cache.usage()
	assert.Equal(t, perEntry*20, bytes)
	assert.Equal(t, int64(20), entries)
	assert.Nil(t, dirs[0].listing, "the oldest listing is evicted first")
	assert.NotNil(t, dirs[1].listing)
	assert.NotNil(t, dirs[2].listing)

	cache.drop(dirs[1])
	cache.drop(dirs[2])
	bytes, entries = cache.usage()
	assert.Equal(t, int64(0), bytes)
	assert.Equal(t, int64(0), entries)
	assert.Equal(t, 0, cache.order.Len())
}

func TestListingLargerThanTheBudgetIsNotCached(t *testing.T) {
	clock := &MockClock{}
	cache := newListingCache(1024)
	dir := &baseDirNode{}

	cache.store(dir, listedFiles(100, clock), clock.Now(), 0)

	assert.Nil(t, dir.listing)
	bytes, _ := cache.usage()
	assert.Equal(t, int64(0), bytes)
}

func TestExpiredListingsAreReleasedByALaterListing(t *testing.T) {
	clock := &MockClock{}
	cache := newListingCache(1 << 20)
	stale, fresh := &baseDirNode{}, &baseDirNode{}
	cache.store(stale, listedFiles(10, clock), clock.Now(), 0)

	clock.NotifyTimeElapsed(CacheAttrsTimeDuration + time.Second)
	cache.store(fresh, listedFiles(1, clock), clock.Now(), 0)

	assert.Nil(t, stale.listing, "a directory that is never visited again must not keep its listing")
	_, entries := cache.usage()
	assert.Equal(t, int64(1), entries)
}

func TestRetiredDirectoryReleasesItsCaches(t *testing.T) {
	root, hdfsAccessor, clock := newLifecycleTestRoot(t)
	hdfsAccessor.EXPECT().Stat("/d").Return(Attrs{Name: "d", Mode: os.ModeDir | 0755, Expires: clock.Now().Add(CacheAttrsTimeDuration)}, nil)
	node, _ := root.Lookup(nil, "d")
	dir := node.(*DirINode)
	hdfsAccessor.EXPECT().ReadDir("/d").Return(listedFiles(5, clock), nil)
	dir.ReadDirAll(nil)
	hdfsAccessor.EXPECT().Stat("/d/missing").Return(Attrs{}, syscall.ENOENT)
	dir.Lookup(nil, "missing")
	assert.Equal(t, int64(1), root.FileSystem.negativeEntries.Load())

	dir.RetireLocked()

	_, entries := root.FileSystem.listings.usage()
	assert.Equal(t, int64(0), entries)
	assert.Equal(t, int64(0), root.FileSystem.negativeEntries.Load())
}

func TestRetiredDirectoryTakesNoNegativeEntries(t *testing.T) {
	root, hdfsAccessor, clock := newLifecycleTestRoot(t)
	hdfsAccessor.EXPECT().Stat("/d").Return(Attrs{Name: "d", Mode: os.ModeDir | 0755, Expires: clock.Now().Add(CacheAttrsTimeDuration)}, nil)
	node, _ := root.Lookup(nil, "d")
	dir := node.(*DirINode)
	dir.RetireLocked()

	// A lookup that was still running when the kernel forgot the directory.
	hdfsAccessor.EXPECT().Stat("/d/missing").Return(Attrs{}, syscall.ENOENT)
	_, err := dir.Lookup(nil, "missing")

	assert.Equal(t, syscall.ENOENT, err)
	assert.Equal(t, int64(0), root.FileSystem.negativeEntries.Load())
}

func TestInvalidatedEntryKeepsItsSlotAccounted(t *testing.T) {
	clock := &MockClock{}
	cache := newListingCache(1 << 20)
	dir := &baseDirNode{}
	cache.store(dir, listedFiles(3, clock), clock.Now(), 0)
	before, _ := cache.usage()

	cache.invalidate(dir, "f00001")

	after, entries := cache.usage()
	assert.Equal(t, before-int64(len("f00001")), after)
	assert.Equal(t, int64(2), entries)
	cache.drop(dir)
	after, _ = cache.usage()
	assert.Equal(t, int64(0), after)
}

func TestEntryThatChangedTypeIsNotServedFromTheListing(t *testing.T) {
	root, hdfsAccessor, clock := newLifecycleTestRoot(t)
	hdfsAccessor.EXPECT().Stat("/f00001").Return(fileAttrs("f00001", clock), nil)
	node, _ := root.Lookup(nil, "f00001")
	clock.NotifyTimeElapsed(CacheAttrsTimeDuration - time.Second)
	hdfsAccessor.EXPECT().ReadDir("/").Return(listedFiles(3, clock), nil)
	root.ReadDirAll(nil)

	// The node's attributes expire first; another client replaced the file
	// with a directory.
	clock.NotifyTimeElapsed(2 * time.Second)
	asDir := Attrs{Name: "f00001", Mode: os.ModeDir | 0755, Expires: clock.Now().Add(CacheAttrsTimeDuration)}
	hdfsAccessor.EXPECT().Stat("/f00001").Return(asDir, nil).Times(2)
	var attr fuse.Attr
	assert.Equal(t, syscall.ESTALE, node.Attr(nil, &attr))

	again, err := root.Lookup(nil, "f00001")
	assert.Nil(t, err)
	assert.IsType(t, &DirINode{}, again)
}

func TestNegativeCacheIsCappedAcrossTheMount(t *testing.T) {
	root, hdfsAccessor, _ := newLifecycleTestRoot(t)
	defer func(max int) { NegativeCacheMaxEntries = max }(NegativeCacheMaxEntries)
	NegativeCacheMaxEntries = 2

	for _, name := range []string{"a", "b", "c"} {
		hdfsAccessor.EXPECT().Stat("/"+name).Return(Attrs{}, syscall.ENOENT)
		root.Lookup(nil, name)
	}

	assert.Equal(t, int64(2), root.FileSystem.negativeEntries.Load())
	// "c" did not fit, so it is asked for again; "a" is answered from the cache.
	hdfsAccessor.EXPECT().Stat("/c").Return(Attrs{}, syscall.ENOENT)
	root.Lookup(nil, "c")
	_, err := root.Lookup(nil, "a")
	assert.Equal(t, syscall.ENOENT, err)
}

func TestAttrReplyIsValidOnlyForWhatIsLeftOfTheAttributes(t *testing.T) {
	root, hdfsAccessor, clock := newLifecycleTestRoot(t)
	hdfsAccessor.EXPECT().Stat("/f").Return(fileAttrs("f", clock), nil)
	node, _ := root.Lookup(nil, "f")

	clock.NotifyTimeElapsed(CacheAttrsTimeDuration - time.Second)
	var attr fuse.Attr
	assert.Nil(t, node.Attr(nil, &attr))

	assert.Equal(t, time.Second, attr.Valid)
}

func TestCgroupMemoryLimitParsing(t *testing.T) {
	dir := t.TempDir()
	write := func(content string) string {
		file := filepath.Join(dir, "limit")
		assert.Nil(t, os.WriteFile(file, []byte(content), 0644))
		return file
	}

	limit, ok := readCgroupMemoryLimit(write("1073741824\n"))
	assert.True(t, ok)
	assert.Equal(t, int64(1073741824), limit)

	for _, noLimit := range []string{"max\n", "9223372036854771712\n", "", "garbage"} {
		_, ok = readCgroupMemoryLimit(write(noLimit))
		assert.False(t, ok, "%q is not a limit", noLimit)
	}
	_, ok = readCgroupMemoryLimit(filepath.Join(dir, "absent"))
	assert.False(t, ok)
}

func TestListingThatPredatesAChangeIsNotKept(t *testing.T) {
	clock := &MockClock{}
	cache := newListingCache(1 << 20)
	dir := &baseDirNode{}

	gen := cache.generation(dir)
	listed := listedFiles(3, clock) // what the backend returned before the change
	cache.invalidate(dir, "f00001")
	cache.store(dir, listed, clock.Now(), gen)

	assert.Nil(t, dir.listing)
	cache.store(dir, listed, clock.Now(), cache.generation(dir))
	assert.NotNil(t, dir.listing)
}

func TestEntryFoundAbsentIsNotServedFromTheListing(t *testing.T) {
	root, hdfsAccessor, clock := newLifecycleTestRoot(t)
	hdfsAccessor.EXPECT().Stat("/f00001").Return(fileAttrs("f00001", clock), nil)
	node, _ := root.Lookup(nil, "f00001")
	clock.NotifyTimeElapsed(CacheAttrsTimeDuration - time.Second)
	hdfsAccessor.EXPECT().ReadDir("/").Return(listedFiles(3, clock), nil)
	root.ReadDirAll(nil)

	// The node's attributes expire first; another client removed the file.
	clock.NotifyTimeElapsed(2 * time.Second)
	hdfsAccessor.EXPECT().Stat("/f00001").Return(Attrs{}, syscall.ENOENT)
	var attr fuse.Attr
	assert.Equal(t, syscall.ENOENT, node.Attr(nil, &attr))

	_, err := root.Lookup(nil, "f00001")
	assert.Equal(t, syscall.ENOENT, err)
}
