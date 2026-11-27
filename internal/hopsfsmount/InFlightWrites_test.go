// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package hopsfsmount

import (
	"reflect"
	"testing"
	"time"
)

func TestWaitForWritesNoneRunning(t *testing.T) {
	filesystem := &FileSystem{}
	start := time.Now()
	if pending := filesystem.WaitForWrites(time.Second); pending != nil {
		t.Fatalf("pending %v, want none", pending)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("returned after %s with nothing running", elapsed)
	}
}

func TestWaitForWritesReturnsWhenTheLastEnds(t *testing.T) {
	filesystem := &FileSystem{}
	endFlush := filesystem.trackWrite(Flush, "/a")
	endRelease := filesystem.trackWrite(Close, "/b")
	go func() {
		time.Sleep(100 * time.Millisecond)
		endFlush()
		time.Sleep(100 * time.Millisecond)
		endRelease()
	}()
	start := time.Now()
	if pending := filesystem.WaitForWrites(5 * time.Second); pending != nil {
		t.Fatalf("pending %v, want none", pending)
	}
	elapsed := time.Since(start)
	if elapsed < 200*time.Millisecond || elapsed > 2*time.Second {
		t.Fatalf("returned after %s, want right after the second end at 200 ms", elapsed)
	}
}

func TestWaitForWritesTimesOutWithThePending(t *testing.T) {
	filesystem := &FileSystem{}
	endFlush := filesystem.trackWrite(Flush, "/slow")
	defer endFlush()
	filesystem.trackWrite(Write, "/done")()
	start := time.Now()
	pending := filesystem.WaitForWrites(200 * time.Millisecond)
	if want := []string{"flush /slow"}; !reflect.DeepEqual(pending, want) {
		t.Fatalf("pending %v, want %v", pending, want)
	}
	if elapsed := time.Since(start); elapsed < 200*time.Millisecond || elapsed > 2*time.Second {
		t.Fatalf("returned after %s, want the 200 ms bound", elapsed)
	}
}

// A dirty file with no handler running keeps the wait open until its flush
// clears it, and a timeout names it.
func TestWaitForWritesWaitsForAnUnflushedFile(t *testing.T) {
	filesystem := &FileSystem{SrcDir: "/base"}
	root := newDirINode(filesystem, nil, Attrs{Name: ""})
	file := &FileINode{FileSystem: filesystem, Parent: root, Attrs: Attrs{Name: "f"}}
	file.markDirty()

	if pending := filesystem.WaitForWrites(100 * time.Millisecond); !reflect.DeepEqual(pending, []string{"unflushed /base/f"}) {
		t.Fatalf("pending %v, want [unflushed /base/f]", pending)
	}

	go func() {
		time.Sleep(100 * time.Millisecond)
		file.isDirty = false
		filesystem.setOwed(file, false)
	}()
	start := time.Now()
	if pending := filesystem.WaitForWrites(5 * time.Second); pending != nil {
		t.Fatalf("pending %v, want none", pending)
	}
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond || elapsed > 2*time.Second {
		t.Fatalf("returned after %s, want right after the flush at 100 ms", elapsed)
	}
}
