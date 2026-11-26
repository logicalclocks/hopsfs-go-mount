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
	"github.com/stretchr/testify/assert"
)

func newLifecycleTestRoot(t *testing.T) (*DirINode, *MockHdfsAccessor, *MockClock) {
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	hdfsAccessor.EXPECT().TryReserve().Return(true).AnyTimes()
	fs, _ := NewFileSystem([]HdfsAccessor{hdfsAccessor}, "/", []string{"*"}, false, DelaySyncUntilClose, NewDefaultRetryPolicy(mockClock), mockClock)
	root, _ := fs.Root()
	return root.(*DirINode), hdfsAccessor, mockClock
}

func fileAttrs(name string, clock *MockClock) Attrs {
	return Attrs{Name: name, Mode: 0644, Expires: clock.Now().Add(CacheAttrsTimeDuration)}
}

func childCount(dir *DirINode) int {
	dir.lockChildrenMutex()
	defer dir.unlockChildrenMutex()
	return len(dir.children)
}

func TestRetiredNodeLeavesItsParentAndIsRefusedRegistration(t *testing.T) {
	root, hdfsAccessor, clock := newLifecycleTestRoot(t)
	hdfsAccessor.EXPECT().Stat("/f").Return(fileAttrs("f", clock), nil)
	node, err := root.Lookup(nil, "f")
	assert.Nil(t, err)
	file := node.(*FileINode)
	assert.True(t, file.RegisterLocked())

	file.RetireLocked()

	assert.Equal(t, 0, childCount(root))
	assert.False(t, file.RegisterLocked(), "a retired node must refuse a late registration")

	// The retried lookup builds a new identity from the backend.
	hdfsAccessor.EXPECT().Stat("/f").Return(fileAttrs("f", clock), nil)
	again, err := root.Lookup(nil, "f")
	assert.Nil(t, err)
	assert.NotSame(t, file, again)
}

func TestLateRetirementOfAReplacedNodeKeepsTheNewOne(t *testing.T) {
	root, hdfsAccessor, clock := newLifecycleTestRoot(t)
	hdfsAccessor.EXPECT().Stat("/f").Return(fileAttrs("f", clock), nil)
	old, _ := root.Lookup(nil, "f")

	// Deleted and recreated under the same name before the kernel forgets the old node.
	hdfsAccessor.EXPECT().Remove("/f").Return(nil)
	assert.Nil(t, root.Remove(nil, &fuse.RemoveRequest{Name: "f"}))
	clock.NotifyTimeElapsed(CacheAttrsTimeDuration + time.Second)
	hdfsAccessor.EXPECT().Stat("/f").Return(fileAttrs("f", clock), nil)
	recreated, err := root.Lookup(nil, "f")
	assert.Nil(t, err)
	assert.NotSame(t, old, recreated)

	old.(*FileINode).RetireLocked()

	cached, err := root.Lookup(nil, "f")
	assert.Nil(t, err)
	assert.Same(t, recreated, cached)
}

func TestMkdirReturnsANewNodeForANameAlreadyCached(t *testing.T) {
	root, hdfsAccessor, clock := newLifecycleTestRoot(t)
	hdfsAccessor.EXPECT().Stat("/d").Return(Attrs{Name: "d", Mode: os.ModeDir | 0755, Expires: clock.Now().Add(CacheAttrsTimeDuration)}, nil)
	old, _ := root.Lookup(nil, "d")
	old.(*DirINode).RetireLocked()
	root.adoptChildInode("unit_test", "d", old) // as if a lookup had selected it just before it retired

	hdfsAccessor.EXPECT().MkdirWithGroup("/d", gomock.Any(), gomock.Any()).Return(nil)
	made, err := root.Mkdir(nil, &fuse.MkdirRequest{Name: "d", Mode: os.ModeDir | 0755})

	assert.Nil(t, err)
	assert.NotSame(t, old, made)
	assert.True(t, made.(*DirINode).RegisterLocked())
}

func TestAttrRefreshDoesNotPutARetiredNodeBack(t *testing.T) {
	root, hdfsAccessor, clock := newLifecycleTestRoot(t)
	hdfsAccessor.EXPECT().Stat("/f").Return(fileAttrs("f", clock), nil)
	node, _ := root.Lookup(nil, "f")
	file := node.(*FileINode)
	file.RetireLocked()

	clock.NotifyTimeElapsed(CacheAttrsTimeDuration + time.Second)
	refreshed := fileAttrs("f", clock)
	refreshed.Size = 7
	hdfsAccessor.EXPECT().Stat("/f").Return(refreshed, nil)
	var attr fuse.Attr
	assert.Nil(t, file.Attr(nil, &attr))

	assert.Equal(t, uint64(7), attr.Size)
	assert.Equal(t, 0, childCount(root))
}

func TestAttrRefreshOfAFileGoneFromTheBackendDetachesOnlyItself(t *testing.T) {
	root, hdfsAccessor, clock := newLifecycleTestRoot(t)
	hdfsAccessor.EXPECT().Stat("/f").Return(fileAttrs("f", clock), nil)
	node, _ := root.Lookup(nil, "f")
	other := root.cacheChildInode(root, "unit_test", "g", fileAttrs("g", clock), false)

	clock.NotifyTimeElapsed(CacheAttrsTimeDuration + time.Second)
	hdfsAccessor.EXPECT().Stat("/f").Return(Attrs{}, syscall.ENOENT)
	var attr fuse.Attr
	assert.Equal(t, syscall.ENOENT, node.Attr(nil, &attr))

	assert.Nil(t, root.getChildInode("unit_test", "f"))
	assert.Same(t, other, root.getChildInode("unit_test", "g"))
}

func TestRenamedNodeRetiresFromItsNewParent(t *testing.T) {
	root, hdfsAccessor, clock := newLifecycleTestRoot(t)
	dirAttrs := Attrs{Name: "d", Mode: os.ModeDir | 0755, Expires: clock.Now().Add(CacheAttrsTimeDuration)}
	hdfsAccessor.EXPECT().Stat("/d").Return(dirAttrs, nil)
	dstNode, _ := root.Lookup(nil, "d")
	dst := dstNode.(*DirINode)
	hdfsAccessor.EXPECT().Stat("/f").Return(fileAttrs("f", clock), nil)
	node, _ := root.Lookup(nil, "f")
	file := node.(*FileINode)

	hdfsAccessor.EXPECT().Rename2("/f", "/d/g", gomock.Any()).Return(nil)
	assert.Nil(t, root.Rename(nil, &fuse.RenameRequest{OldName: "f", NewName: "g"}, dst))

	assert.Same(t, file, dst.getChildInode("unit_test", "g"), "rename keeps the node's identity")
	assert.Equal(t, "/d/g", file.AbsolutePath())
	assert.Nil(t, root.getChildInode("unit_test", "f"))

	file.RetireLocked()
	assert.Equal(t, 0, childCount(dst))
}

func TestRenameDoesNotRepublishARetiredNode(t *testing.T) {
	root, hdfsAccessor, clock := newLifecycleTestRoot(t)
	hdfsAccessor.EXPECT().Stat("/f").Return(fileAttrs("f", clock), nil)
	node, _ := root.Lookup(nil, "f")
	file := node.(*FileINode)
	file.RetireLocked()
	root.adoptChildInode("unit_test", "f", file) // selected by the rename just before it retired

	hdfsAccessor.EXPECT().Rename2("/f", "/g", gomock.Any()).Return(nil)
	assert.Nil(t, root.Rename(nil, &fuse.RenameRequest{OldName: "f", NewName: "g"}, root))

	assert.Nil(t, root.getChildInode("unit_test", "g"))
}

func TestChildRetiresAfterItsParent(t *testing.T) {
	root, hdfsAccessor, clock := newLifecycleTestRoot(t)
	hdfsAccessor.EXPECT().Stat("/d").Return(Attrs{Name: "d", Mode: os.ModeDir | 0755, Expires: clock.Now().Add(CacheAttrsTimeDuration)}, nil)
	dirNode, _ := root.Lookup(nil, "d")
	dir := dirNode.(*DirINode)
	hdfsAccessor.EXPECT().Stat("/d/f").Return(fileAttrs("f", clock), nil)
	node, _ := dir.Lookup(nil, "f")

	dir.RetireLocked()
	node.(*FileINode).RetireLocked()

	assert.Equal(t, 0, childCount(root))
	assert.Equal(t, 0, childCount(dir))
	assert.Equal(t, "/d/f", node.(*FileINode).AbsolutePath())
}

func TestAttrRefreshThatRacesARenameKeepsTheMovedNode(t *testing.T) {
	root, hdfsAccessor, clock := newLifecycleTestRoot(t)
	hdfsAccessor.EXPECT().Stat("/f").Return(fileAttrs("f", clock), nil)
	node, _ := root.Lookup(nil, "f")
	clock.NotifyTimeElapsed(CacheAttrsTimeDuration + time.Second)

	// The refresh read the old path, and the rename completes before the
	// backend answers it.
	hdfsAccessor.EXPECT().Rename2("/f", "/g", gomock.Any()).Return(nil)
	hdfsAccessor.EXPECT().Stat("/f").DoAndReturn(func(string) (Attrs, error) {
		assert.Nil(t, root.Rename(nil, &fuse.RenameRequest{OldName: "f", NewName: "g"}, root))
		return Attrs{}, syscall.ENOENT
	})
	var attr fuse.Attr
	assert.Equal(t, syscall.ENOENT, node.Attr(nil, &attr))

	assert.Same(t, node, root.getChildInode("unit_test", "g"))
}
