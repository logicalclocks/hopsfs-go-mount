// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package hopsfsmount

import (
	"fmt"
	"os"
	"syscall"
	"testing"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/stretchr/testify/assert"
)

type invalidatedEntry struct {
	parent fs.Node
	name   string
}

// recordingInvalidator stands in for the FUSE server: it records the entries
// it is asked to drop and, like the kernel, answers each with a FORGET unless
// the entry is in keep.
type recordingInvalidator struct {
	asked []invalidatedEntry
	keep  map[string]bool
}

func (r *recordingInvalidator) InvalidateEntry(parent fs.Node, name string) error {
	r.asked = append(r.asked, invalidatedEntry{parent, name})
	if r.keep[name] {
		return nil
	}
	dir := parent.(*DirINode)
	dir.lockChildrenMutex()
	child := dir.children[name]
	dir.unlockChildrenMutex()
	child.(*FileINode).RetireLocked()
	return nil
}

func (r *recordingInvalidator) names() []string {
	names := make([]string, len(r.asked))
	for i, entry := range r.asked {
		names[i] = entry.name
	}
	return names
}

// registerFiles looks up and registers n files in root, as n entry replies would.
func registerFiles(t *testing.T, root *DirINode, hdfsAccessor *MockHdfsAccessor, clock *MockClock, n int) []*FileINode {
	files := make([]*FileINode, n)
	for i := range files {
		name := fmt.Sprintf("f%d", i)
		hdfsAccessor.EXPECT().Stat("/"+name).Return(fileAttrs(name, clock), nil)
		node, err := root.Lookup(nil, name)
		assert.Nil(t, err)
		files[i] = node.(*FileINode)
		assert.True(t, files[i].RegisterLocked())
	}
	return files
}

func TestEvictorDropsTheOldestFilesAboveTheLimit(t *testing.T) {
	root, hdfsAccessor, clock := newLifecycleTestRoot(t)
	registerFiles(t, root, hdfsAccessor, clock, 5)
	server := &recordingInvalidator{}

	root.FileSystem.evictFileNodes(server, 3)

	assert.Equal(t, []string{"f0", "f1"}, server.names())
	assert.Same(t, root, server.asked[0].parent)
	assert.Equal(t, 3, childCount(root))
	assert.Equal(t, 3, root.FileSystem.fileNodes.files.Len())

	root.FileSystem.evictFileNodes(server, 3)
	assert.Len(t, server.asked, 2, "at the limit nothing more is dropped")
}

func TestEvictorSkipsOpenFiles(t *testing.T) {
	root, hdfsAccessor, clock := newLifecycleTestRoot(t)
	files := registerFiles(t, root, hdfsAccessor, clock, 3)
	files[0].activeHandles = []*FileHandle{{}}
	server := &recordingInvalidator{}

	root.FileSystem.evictFileNodes(server, 1)

	assert.Equal(t, []string{"f1"}, server.names())
}

func TestEvictorOffersAKeptFileAgainOnlyAfterTheOthers(t *testing.T) {
	root, hdfsAccessor, clock := newLifecycleTestRoot(t)
	registerFiles(t, root, hdfsAccessor, clock, 3)
	server := &recordingInvalidator{keep: map[string]bool{"f0": true}}

	root.FileSystem.evictFileNodes(server, 2)
	root.FileSystem.evictFileNodes(server, 2)

	assert.Equal(t, []string{"f0", "f1"}, server.names())
}

func TestEvictorFollowsARenamedFile(t *testing.T) {
	root, hdfsAccessor, clock := newLifecycleTestRoot(t)
	files := registerFiles(t, root, hdfsAccessor, clock, 1)
	assert.True(t, files[0].life.move(files[0], root, "renamed"))
	server := &recordingInvalidator{}

	root.FileSystem.evictFileNodes(server, 0)

	assert.Equal(t, []string{"renamed"}, server.names())
}

// A directory must not take over a name while the kernel is being asked to
// drop the file under it: the request would hit the directory.
func TestDirectoryCannotTakeANameBeingEvicted(t *testing.T) {
	root, hdfsAccessor, clock := newLifecycleTestRoot(t)
	registerFiles(t, root, hdfsAccessor, clock, 1)
	root.FileSystem.evicting.set(root, "f0")

	_, err := root.Mkdir(nil, &fuse.MkdirRequest{Name: "f0"})
	assert.Equal(t, syscall.ESTALE, err)

	hdfsAccessor.EXPECT().Stat("/d").Return(Attrs{Name: "d", Mode: os.ModeDir | 0755, Expires: clock.Now().Add(CacheAttrsTimeDuration)}, nil)
	_, err = root.Lookup(nil, "d")
	assert.Nil(t, err)
	assert.Equal(t, syscall.ESTALE, root.Rename(nil, &fuse.RenameRequest{OldName: "d", NewName: "f0"}, root))

	root.FileSystem.evicting.set(root, "gone")
	hdfsAccessor.EXPECT().Stat("/gone").Return(Attrs{Name: "gone", Mode: os.ModeDir | 0755}, nil)
	_, err = root.Lookup(nil, "gone")
	assert.Equal(t, syscall.ESTALE, err)

	virtual := &VirtualDirINode{baseDirNode: baseDirNode{FileSystem: root.FileSystem}}
	root.FileSystem.evicting.set(virtual, "leaf")
	_, err = root.FileSystem.evicting.refuseDirectory(virtual, "leaf", &DirINode{})
	assert.Equal(t, syscall.ESTALE, err)
	file := &FileINode{}
	kept, err := root.FileSystem.evicting.refuseDirectory(virtual, "leaf", file)
	assert.Nil(t, err)
	assert.Same(t, file, kept)
}

func TestEvictorSkipsANameThatNoLongerHoldsTheFile(t *testing.T) {
	root, hdfsAccessor, clock := newLifecycleTestRoot(t)
	files := registerFiles(t, root, hdfsAccessor, clock, 1)
	root.forgetChild("f0", files[0])
	server := &recordingInvalidator{}

	root.FileSystem.evictFileNodes(server, 0)

	assert.Empty(t, server.asked)
	assert.False(t, root.FileSystem.evicting.names(root, "f0"))
}

func TestMountsDoNotShareEvictionState(t *testing.T) {
	rootA, accessorA, clockA := newLifecycleTestRoot(t)
	rootB, accessorB, clockB := newLifecycleTestRoot(t)
	registerFiles(t, rootA, accessorA, clockA, 3)
	registerFiles(t, rootB, accessorB, clockB, 1)
	server := &recordingInvalidator{}

	rootB.FileSystem.evictFileNodes(server, 1)

	assert.Empty(t, server.asked, "a mount under its limit drops nothing, whatever another mount holds")
}

func TestEvictorLeavesANameADirectoryOperationHasClaimed(t *testing.T) {
	root, hdfsAccessor, clock := newLifecycleTestRoot(t)
	registerFiles(t, root, hdfsAccessor, clock, 1)
	server := &recordingInvalidator{}

	assert.True(t, root.FileSystem.evicting.claim(root, "f0"))
	root.FileSystem.evictFileNodes(server, 0)
	assert.Empty(t, server.asked)

	root.FileSystem.evicting.release(root, "f0")
	root.FileSystem.evictFileNodes(server, 0)
	assert.Equal(t, []string{"f0"}, server.names())
	assert.Empty(t, root.FileSystem.evicting.binding)
}
