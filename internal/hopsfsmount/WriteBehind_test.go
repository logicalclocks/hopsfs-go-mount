// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package hopsfsmount

// Integration tests of write-behind against a real HopsFS (NAMENODE_ADDRESS, as the other mount
// tests), in a directory the test user may write: HOPSFS_TEST_DIR (for example
// /Projects/<project>/Resources). Each test works in its own subdirectory and removes it.
// External writes go through a second client with its own connection.

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/colinmarc/hdfs/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type wbEnv struct {
	t   *testing.T
	mnt string       // mount point; srcDir is "/", so a HopsFS path p is at mnt+p
	dir string       // this test's HopsFS directory
	fs  *FileSystem  // the mount's file system
	ext *hdfs.Client // another HopsFS client
}

func (e *wbEnv) local(name string) string  { return e.mnt + path.Join(e.dir, name) }
func (e *wbEnv) remote(name string) string { return path.Join(e.dir, name) }

func withWriteBehind(t *testing.T, fn func(e *wbEnv)) {
	base := strings.TrimSpace(os.Getenv("HOPSFS_TEST_DIR"))
	if base == "" {
		t.Fatal("HOPSFS_TEST_DIR names a HopsFS directory the test user may write, e.g. /Projects/<project>/Resources")
	}
	enabled, maxFiles, wait, workers := WriteBehindEnabled, WriteBehindMaxFiles, WriteBehindWait, WriteBehindWorkers
	WriteBehindEnabled = true
	t.Cleanup(func() {
		WriteBehindEnabled, WriteBehindMaxFiles, WriteBehindWait, WriteBehindWorkers = enabled, maxFiles, wait, workers
		wbStageHook = nil
	})

	ext := externalClient(t)
	dir := path.Join(base, uniqueTestName("hable158"))
	require.NoError(t, ext.Mkdir(dir, 0755))
	t.Cleanup(func() { ext.RemoveAll(dir) })

	withMountFS(t, "/", false, func(mnt string, _ HdfsAccessor, fileSystem *FileSystem) {
		require.NotNil(t, fileSystem.WB, "write-behind is on")
		fn(&wbEnv{t: t, mnt: mnt, dir: dir, fs: fileSystem, ext: ext})
	})
}

func externalClient(t *testing.T) *hdfs.Client {
	require.NoError(t, InitConnectionUser())
	tlsEnabled, err := testTLSEnabled()
	require.NoError(t, err)
	acc, err := NewHdfsAccessor(testNameNodeAddress(), WallClock{}, TLSConfig{TLS: tlsEnabled, RootCABundle: RootCABundle, ClientCertificate: ClientCertificate, ClientKey: ClientKey})
	require.NoError(t, err)
	require.NoError(t, acc.EnsureConnected())
	return acc.(*HdfsAccessorImpl).MetadataClient
}

func (e *wbEnv) readRemote(name string) string {
	b, err := e.ext.ReadFile(e.remote(name))
	require.NoError(e.t, err)
	return string(b)
}

func (e *wbEnv) writeLocal(name, content string) {
	require.NoError(e.t, os.WriteFile(e.local(name), []byte(content), 0644))
}

func (e *wbEnv) writeExternal(name, content string) {
	w, err := e.ext.Create(e.remote(name))
	if errors.Is(err, os.ErrExist) {
		require.NoError(e.t, e.ext.Remove(e.remote(name)))
		w, err = e.ext.Create(e.remote(name))
	}
	require.NoError(e.t, err)
	_, err = w.Write([]byte(content))
	require.NoError(e.t, err)
	require.NoError(e.t, w.Close())
}

func (e *wbEnv) appendExternal(name, content string) {
	w, err := e.ext.Append(e.remote(name))
	require.NoError(e.t, err)
	_, err = w.Write([]byte(content))
	require.NoError(e.t, err)
	require.NoError(e.t, w.Close())
}

func (e *wbEnv) drain() []Unresolved {
	out, timedOut := e.fs.WB.Drain(e.dir, 60*time.Second)
	require.False(e.t, timedOut, "drain timed out")
	return out
}

// conflictFiles lists our copies HopsFS kept beside name after a conflict.
func (e *wbEnv) conflictFiles(name string) []string {
	infos, err := e.ext.ReadDir(e.dir)
	require.NoError(e.t, err)
	var out []string
	for _, fi := range infos {
		if strings.HasPrefix(fi.Name(), name+".conflict-") {
			out = append(out, fi.Name())
		}
	}
	return out
}

// hold makes the worker wait at stage of the upload of name until the returned release runs.
// Callers defer release: a held worker keeps the file's dataMutex, and the unmount at the end of
// the test would wait for it. release may run more than once.
func (e *wbEnv) hold(stage, name string) (reached <-chan struct{}, release func()) {
	target := e.remote(name)
	at := make(chan struct{})
	gate := make(chan struct{})
	var once sync.Once
	wbStageHook = func(s, p string) {
		if s == stage && p == target {
			once.Do(func() {
				close(at)
				<-gate
			})
		}
	}
	var released sync.Once
	return at, func() { released.Do(func() { close(gate) }) }
}

func waitFor(t *testing.T, ch <-chan struct{}) {
	select {
	case <-ch:
	case <-time.After(60 * time.Second):
		t.Fatal("the upload did not reach the held stage")
	}
}

func TestWriteBehindCloseThenDrainLands(t *testing.T) {
	withWriteBehind(t, func(e *wbEnv) {
		e.writeLocal("a.txt", "hello")
		assert.Empty(t, e.drain())
		assert.Equal(t, "hello", e.readRemote("a.txt"))
		// Our upload temp is gone and never listed.
		names, err := os.ReadDir(e.local(""))
		require.NoError(t, err)
		for _, n := range names {
			assert.False(t, isWBTemp(n.Name()), n.Name())
		}
	})
}

func TestWriteBehindPendingFileIsListedAndReadable(t *testing.T) {
	workers := WriteBehindWorkers
	WriteBehindWorkers = 1
	t.Cleanup(func() { WriteBehindWorkers = workers })
	withWriteBehind(t, func(e *wbEnv) {
		// The one worker is held on another file, so p.txt is queued: listed and read from its
		// staged copy. (A file a worker is uploading waits for that upload, which holds its data lock.)
		reached, release := e.hold("before-publish", "busy.txt")
		defer release()
		e.writeLocal("busy.txt", "b")
		waitFor(t, reached)
		e.writeLocal("p.txt", "pending")
		names, err := os.ReadDir(e.local(""))
		require.NoError(t, err)
		var listed []string
		for _, n := range names {
			listed = append(listed, n.Name())
		}
		assert.Contains(t, listed, "p.txt")
		b, err := os.ReadFile(e.local("p.txt"))
		require.NoError(t, err)
		assert.Equal(t, "pending", string(b))
		_, err = e.ext.Stat(e.remote("p.txt"))
		assert.True(t, errors.Is(err, os.ErrNotExist), "not on HopsFS before its upload: %v", err)
		release()
		assert.Empty(t, e.drain())
		assert.Equal(t, "pending", e.readRemote("p.txt"))
	})
}

func TestWriteBehindFsyncBeforeClose(t *testing.T) {
	withWriteBehind(t, func(e *wbEnv) {
		f, err := os.Create(e.local("f.txt"))
		require.NoError(t, err)
		defer f.Close()
		_, err = f.WriteString("synced")
		require.NoError(t, err)
		require.NoError(t, f.Sync())
		assert.Equal(t, "synced", e.readRemote("f.txt"))
	})
}

func TestWriteBehindTwoWritesInARow(t *testing.T) {
	withWriteBehind(t, func(e *wbEnv) {
		e.writeLocal("r.txt", "one")
		assert.Empty(t, e.drain())
		e.writeLocal("r.txt", "two")
		assert.Empty(t, e.drain())
		assert.Equal(t, "two", e.readRemote("r.txt"))
		assert.Empty(t, e.conflictFiles("r.txt"))
	})
}

func TestWriteBehindTwoFsyncsOnOneHandle(t *testing.T) {
	withWriteBehind(t, func(e *wbEnv) {
		f, err := os.Create(e.local("h.txt"))
		require.NoError(t, err)
		defer f.Close()
		_, err = f.WriteString("A")
		require.NoError(t, err)
		require.NoError(t, f.Sync())
		_, err = f.WriteString("B")
		require.NoError(t, err)
		require.NoError(t, f.Sync())
		assert.Equal(t, "AB", e.readRemote("h.txt"))
		assert.Empty(t, e.conflictFiles("h.txt"))
	})
}

// A worker stopped after the temp is complete and before the rename leaves the public path as
// it was: the old version, whole.
func TestWriteBehindStoppedBeforeRenameLeavesOldVersion(t *testing.T) {
	withWriteBehind(t, func(e *wbEnv) {
		e.writeExternal("k.txt", "old")
		reached, release := e.hold("before-publish", "k.txt")
		defer release()
		e.writeLocal("k.txt", "new")
		waitFor(t, reached)
		assert.Equal(t, "old", e.readRemote("k.txt"))
		_, err := e.ext.Stat(e.remote(wbTempName("k.txt")))
		assert.NoError(t, err, "the complete temp is hidden beside the file")
		release()
		assert.Empty(t, e.drain())
		assert.Equal(t, "new", e.readRemote("k.txt"))
	})
}

func TestWriteBehindExternalWriteBeforeCheckIsAConflict(t *testing.T) {
	withWriteBehind(t, func(e *wbEnv) {
		e.writeExternal("x.txt", "v1")
		_, err := os.ReadFile(e.local("x.txt"))
		require.NoError(t, err)
		reached, release := e.hold("before-publish", "x.txt")
		defer release()
		e.writeLocal("x.txt", "ours")
		waitFor(t, reached)
		e.writeExternal("x.txt", "theirs")
		release()
		out := e.drain()
		require.Len(t, out, 1)
		assert.Contains(t, out[0].Error, "conflict")
		assert.Equal(t, "theirs", e.readRemote("x.txt"))
		conflicts := e.conflictFiles("x.txt")
		require.Len(t, conflicts, 1)
		assert.Equal(t, "ours", e.readRemote(conflicts[0]))
	})
}

func TestWriteBehindExternalAppendBeforeCheckIsAConflict(t *testing.T) {
	withWriteBehind(t, func(e *wbEnv) {
		e.writeExternal("ap.txt", "v1")
		_, err := os.ReadFile(e.local("ap.txt"))
		require.NoError(t, err)
		reached, release := e.hold("before-publish", "ap.txt")
		defer release()
		e.writeLocal("ap.txt", "ours")
		waitFor(t, reached)
		e.appendExternal("ap.txt", "+X")
		release()
		require.Len(t, e.drain(), 1)
		assert.Equal(t, "v1+X", e.readRemote("ap.txt"))
		require.Len(t, e.conflictFiles("ap.txt"), 1)
	})
}

func TestWriteBehindNewFileCreatedExternallyIsAConflict(t *testing.T) {
	withWriteBehind(t, func(e *wbEnv) {
		reached, release := e.hold("before-publish", "n.txt")
		defer release()
		e.writeLocal("n.txt", "ours")
		waitFor(t, reached)
		e.writeExternal("n.txt", "theirs")
		e.appendExternal("n.txt", "+X")
		release()
		require.Len(t, e.drain(), 1)
		assert.Equal(t, "theirs+X", e.readRemote("n.txt"))
		conflicts := e.conflictFiles("n.txt")
		require.Len(t, conflicts, 1)
		assert.Equal(t, "ours", e.readRemote(conflicts[0]))
	})
}

// The gate's sequence: hold between the temp's complete and the base read, an external client
// creates the path and appends X, then the same handle writes again. X never disappears.
func TestWriteBehindExternalAppendNeverAdoptedAsBase(t *testing.T) {
	withWriteBehind(t, func(e *wbEnv) {
		reached, release := e.hold("temp-complete", "g.txt")
		defer release()
		f, err := os.Create(e.local("g.txt"))
		require.NoError(t, err)
		defer f.Close()
		_, err = f.WriteString("A")
		require.NoError(t, err)
		synced := make(chan error, 1)
		go func() { synced <- f.Sync() }()
		waitFor(t, reached)
		e.writeExternal("g.txt", "theirs")
		e.appendExternal("g.txt", "X")
		release()
		assert.Error(t, <-synced, "the first upload conflicts")
		_, err = f.WriteString("B")
		require.NoError(t, err)
		assert.Error(t, f.Sync(), "the conflict stays until resolved")
		assert.Equal(t, "theirsX", e.readRemote("g.txt"))
	})
}

func TestWriteBehindErrorStaysUntilResolved(t *testing.T) {
	withWriteBehind(t, func(e *wbEnv) {
		ro := e.remote("ro")
		require.NoError(t, e.ext.Mkdir(ro, 0755))
		_, err := os.ReadDir(e.local("ro"))
		require.NoError(t, err)
		// The temp's create fails in a directory the user may not write.
		require.NoError(t, e.ext.Chmod(ro, 0555))
		e.writeLocal("ro/e.txt", "data")
		require.Len(t, e.drain(), 1)
		// The next op on the path reports the error once (open or stat, whichever comes first);
		// reading it changes nothing: fsync and drain keep failing.
		var f *os.File
		for i := 0; i < 2 && f == nil; i++ {
			f, _ = os.OpenFile(e.local("ro/e.txt"), os.O_RDWR, 0)
		}
		require.NotNil(t, f)
		assert.Error(t, f.Sync())
		f.Close()
		assert.Len(t, e.drain(), 1)
		// A later upload resolves it.
		require.NoError(t, e.ext.Chmod(ro, 0755))
		e.writeLocal("ro/e.txt", "data2")
		assert.Empty(t, e.drain())
		assert.Equal(t, "data2", e.readRemote("ro/e.txt"))
	})
}

func TestWriteBehindDiscardKeepsHopsFS(t *testing.T) {
	withWriteBehind(t, func(e *wbEnv) {
		e.writeExternal("d.txt", "v1")
		_, err := os.ReadFile(e.local("d.txt"))
		require.NoError(t, err)
		reached, release := e.hold("before-publish", "d.txt")
		defer release()
		e.writeLocal("d.txt", "ours")
		waitFor(t, reached)
		e.writeExternal("d.txt", "theirs")
		release()
		require.Len(t, e.drain(), 1)
		file, err := e.fs.fileNode(e.remote("d.txt"))
		require.NoError(t, err)
		// Release of the last handle reaches the mount after close returns.
		require.Eventually(t, func() bool { return e.fs.WB.Discard(file) == nil }, 10*time.Second, 50*time.Millisecond)
		assert.Empty(t, e.drain())
		b, err := os.ReadFile(e.local("d.txt"))
		require.NoError(t, err)
		assert.Equal(t, "theirs", string(b))
	})
}

func TestWriteBehindFullQueueFailsCloseWithinItsWait(t *testing.T) {
	withWriteBehind(t, func(e *wbEnv) {
		WriteBehindMaxFiles = 1
		WriteBehindWait = time.Second
		reached, release := e.hold("before-publish", "q1.txt")
		defer release()
		e.writeLocal("q1.txt", "1")
		waitFor(t, reached)
		started := time.Now()
		f, err := os.Create(e.local("q2.txt"))
		require.NoError(t, err)
		_, err = f.WriteString("2")
		require.NoError(t, err)
		err = f.Close()
		assert.Error(t, err, "close fails once the queue stays full past its wait")
		assert.Less(t, time.Since(started), 10*time.Second)
		b, err := os.ReadFile(e.local("q2.txt"))
		require.NoError(t, err)
		assert.Equal(t, "2", string(b), "the bytes stay staged")
		release()
		out := e.drain()
		require.Len(t, out, 1)
		assert.Equal(t, e.remote("q2.txt"), out[0].Path)
	})
}

func TestWriteBehindStopFencesWrites(t *testing.T) {
	withWriteBehind(t, func(e *wbEnv) {
		f, err := os.Create(e.local("s.txt"))
		require.NoError(t, err)
		_, err = f.WriteString("open at stop")
		require.NoError(t, err)
		out, timedOut := e.fs.WB.Stop(60 * time.Second)
		assert.False(t, timedOut)
		assert.Empty(t, out)
		assert.Equal(t, "open at stop", e.readRemote("s.txt"), "a file open at stop is uploaded")
		_, err = os.Create(e.local("t.txt"))
		assert.True(t, errors.Is(err, syscall.EROFS), "%v", err)
		f.Close()
	})
}

func TestWriteBehindUnlinkOfAFileNeverLanded(t *testing.T) {
	workers := WriteBehindWorkers
	WriteBehindWorkers = 1
	t.Cleanup(func() { WriteBehindWorkers = workers })
	withWriteBehind(t, func(e *wbEnv) {
		// The one worker is held on another file, so u.txt is queued and not started.
		reached, release := e.hold("before-publish", "busy.txt")
		defer release()
		e.writeLocal("busy.txt", "b")
		waitFor(t, reached)
		e.writeLocal("u.txt", "u")
		require.NoError(t, os.Remove(e.local("u.txt")))
		release()
		assert.Empty(t, e.drain())
		_, err := e.ext.Stat(e.remote("u.txt"))
		assert.True(t, errors.Is(err, os.ErrNotExist), "%v", err)
	})
}

func TestWriteBehindRenameOfAnOpenFile(t *testing.T) {
	withWriteBehind(t, func(e *wbEnv) {
		f, err := os.Create(e.local("tmp.txt"))
		require.NoError(t, err)
		_, err = f.WriteString("saved")
		require.NoError(t, err)
		require.NoError(t, os.Rename(e.local("tmp.txt"), e.local("final.txt")))
		f.Close()
		assert.Empty(t, e.drain())
		assert.Equal(t, "saved", e.readRemote("final.txt"))
	})
}

func TestWriteBehindControlSocketDrain(t *testing.T) {
	withWriteBehind(t, func(e *wbEnv) {
		sock := filepath.Join(t.TempDir(), "control.sock")
		l, err := ServeControlSocket(e.fs, sock)
		require.NoError(t, err)
		defer l.Close()
		e.writeLocal("c.txt", "c")
		c, err := net.Dial("unix", sock)
		require.NoError(t, err)
		defer c.Close()
		fmt.Fprintf(c, "drain 60 %s\n", e.dir)
		var lines []string
		sc := bufio.NewScanner(c)
		for sc.Scan() {
			lines = append(lines, sc.Text())
		}
		assert.Equal(t, []string{"exit 0"}, lines)
		assert.Equal(t, "c", e.readRemote("c.txt"))
	})
}

// A write that reached the mount while an upload held its file, then a stop: the write is either
// refused or its bytes are on HopsFS (or named unresolved) after the stop; never accepted and lost.
func TestWriteBehindStopDuringUploadNeverLosesAnAcceptedWrite(t *testing.T) {
	withWriteBehind(t, func(e *wbEnv) {
		first, err := os.Create(e.local("w.txt"))
		require.NoError(t, err)
		second, err := os.OpenFile(e.local("w.txt"), os.O_RDWR, 0)
		require.NoError(t, err)
		defer second.Close()
		_, err = first.WriteString("A")
		require.NoError(t, err)
		reached, release := e.hold("before-publish", "w.txt")
		defer release()
		require.NoError(t, first.Close()) // queues the upload, which holds the file
		waitFor(t, reached)

		wrote := make(chan error, 1)
		go func() { _, err := second.WriteAt([]byte("B"), 1); wrote <- err }()
		time.Sleep(300 * time.Millisecond) // the write waits inside the mount for the file
		type stopResult struct {
			out      []Unresolved
			timedOut bool
		}
		stopped := make(chan stopResult, 1)
		go func() { out, timedOut := e.fs.WB.Stop(60 * time.Second); stopped <- stopResult{out, timedOut} }()
		time.Sleep(300 * time.Millisecond)
		release()

		werr := <-wrote
		stop := <-stopped
		content := e.readRemote("w.txt")
		if werr != nil {
			assert.True(t, errors.Is(werr, syscall.EROFS), "%v", werr)
			assert.Equal(t, "A", content)
			return
		}
		named := false
		for _, u := range stop.out {
			named = named || u.Path == e.remote("w.txt")
		}
		assert.True(t, content == "AB" || named, "an accepted write is on HopsFS or named: content %q, stop %+v", content, stop)
	})
}

func TestWriteBehindChmodOfAnOpenNewFile(t *testing.T) {
	withWriteBehind(t, func(e *wbEnv) {
		f, err := os.Create(e.local("m.txt"))
		require.NoError(t, err)
		defer f.Close() // a failure must not leave the mount busy at unmount
		_, err = f.WriteString("x")
		require.NoError(t, err)
		require.NoError(t, f.Chmod(0600))
		require.NoError(t, f.Close())
		assert.Empty(t, e.drain())
		fi, err := e.ext.Stat(e.remote("m.txt"))
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0600), fi.Mode().Perm())
		assert.Equal(t, "x", e.readRemote("m.txt"))
	})
}

// A file over the download-cache limit opens read-only from HopsFS; its first write or truncate
// loads it, and the upload replaces it as an edit, not as a new file that conflicts.
func TestWriteBehindFirstChangeOfAnUncachedFile(t *testing.T) {
	limit := StagingCacheMaxDownloadSize
	StagingCacheMaxDownloadSize = 4
	t.Cleanup(func() { StagingCacheMaxDownloadSize = limit })
	withWriteBehind(t, func(e *wbEnv) {
		e.writeExternal("w.txt", "0123456789")
		e.writeExternal("t.txt", "0123456789")

		f, err := os.OpenFile(e.local("w.txt"), os.O_RDWR, 0)
		require.NoError(t, err)
		defer f.Close()
		_, err = f.WriteAt([]byte("AB"), 0)
		require.NoError(t, err)
		require.NoError(t, f.Close())

		g, err := os.OpenFile(e.local("t.txt"), os.O_RDWR, 0)
		require.NoError(t, err)
		defer g.Close()
		require.NoError(t, g.Truncate(4))
		require.NoError(t, g.Close())

		assert.Empty(t, e.drain())
		assert.Equal(t, "AB23456789", e.readRemote("w.txt"))
		assert.Equal(t, "0123", e.readRemote("t.txt"))
		assert.Empty(t, e.conflictFiles("w.txt"))
		assert.Empty(t, e.conflictFiles("t.txt"))
	})
}
