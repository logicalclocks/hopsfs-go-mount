// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package hopsfsmount

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/colinmarc/hdfs/v2"
	"hopsworks.ai/hopsfsmount/internal/hopsfsmount/logger"
)

// Write-behind: close returns once a file is staged on local disk, and a pool of workers
// uploads it. fsync waits for the file's upload and is the durability point; Drain is the
// handoff point for readers outside this mount.
//
// Every upload is written to a hidden temp beside the file, its (fileId, length, mtime) read
// there, then one rename publishes it: with no replace for a file HopsFS did not have, with
// overwrite for an existing one once its current status still equals the base the staged
// content was loaded from. Anything else is a conflict: the other version stays and ours goes
// to <name>.conflict-<UTC time>. An external write that lands between that check and the
// rename is replaced: HopsFS has no compare-and-swap rename.
//
// Lock order: wbMu is the lowest lock. A goroutine holding a file's dataMutex, fileMutex or
// fileHandleMutex may take wbMu; one holding wbMu takes no other lock.

// wbStageHook, when set (tests only), runs at each stage of an upload: "temp-complete" after
// the temp is complete and before its status is read, "before-publish" before the check and
// the rename.
var wbStageHook func(stage, path string)

func wbStage(stage, p string) {
	if hook := wbStageHook; hook != nil {
		hook(stage, p)
	}
}

type wbState int

const (
	wbLocal     wbState = iota // written but not closed: nothing queued yet
	wbQueued                   // a generation waits for a worker
	wbUploading                // a worker uploads a generation
	wbFailed                   // the last upload failed; the staged bytes stay
	wbConflict                 // HopsFS had another version; ours is in conflictPath
)

func (s wbState) String() string {
	return [...]string{"local", "queued", "uploading", "failed", "conflict"}[s]
}

type wbEntry struct {
	path         string
	file         *FileINode
	state        wbState
	staged       uint64 // generation of the staged content, +1 on each close or fsync of a dirty file
	landed       uint64 // last generation on HopsFS
	base         *Attrs // HopsFS status the staged content was loaded from; nil: HopsFS has no such file
	bytes        int64  // staged size, counted against WriteBehindMaxBytes while queued or unresolved
	counted      bool
	err          error
	reported     bool
	conflictPath string
}

// unresolved: the last upload failed or conflicted, and no later one landed.
func (e *wbEntry) unresolved() bool { return e.state == wbFailed || e.state == wbConflict }

type WriteBehind struct {
	fs      *FileSystem
	mu      sync.Mutex
	cond    *sync.Cond
	entries map[string]*wbEntry
	queue   []*wbEntry
	files   int
	bytes   int64
	fenced  bool
	closed  bool
}

func NewWriteBehind(fs *FileSystem) *WriteBehind {
	if WriteBehindTempTag == "" {
		WriteBehindTempTag, _ = os.Hostname()
	}
	wb := &WriteBehind{fs: fs, entries: map[string]*wbEntry{}}
	wb.cond = sync.NewCond(&wb.mu)
	for i := 0; i < WriteBehindWorkers; i++ {
		go wb.worker()
	}
	logger.Info(fmt.Sprintf("Write-behind on: %d workers, at most %d files and %d bytes pending, close waits at most %v for room",
		WriteBehindWorkers, WriteBehindMaxFiles, WriteBehindMaxBytes, WriteBehindWait), nil)
	return wb
}

// The hidden temp an upload of name goes to; one per path and mount, reused after a crash.
func wbTempName(name string) string {
	return "." + name + ".hopsfs-" + WriteBehindTempTag
}

// A temp of any mount: listings hide them.
func isWBTemp(name string) bool {
	return strings.HasPrefix(name, ".") && strings.Contains(name, ".hopsfs-")
}

// Registers a file created through this mount (HopsFS does not have it until its first
// upload), unless writes are fenced. Caller holds file.dataMutex.
func (wb *WriteBehind) created(file *FileINode) error {
	wb.mu.Lock()
	defer wb.mu.Unlock()
	if wb.fenced {
		return syscall.EROFS
	}
	p := file.AbsolutePath()
	wb.entries[p] = &wbEntry{path: p, file: file, state: wbLocal}
	return nil
}

// Admits a change to file's content: refused once writes are fenced, else the file is registered
// before it changes, so a stop's snapshot holds every change it let in. Caller holds file.dataMutex.
func (wb *WriteBehind) admit(file *FileINode) error {
	wb.mu.Lock()
	defer wb.mu.Unlock()
	if wb.fenced {
		return syscall.EROFS
	}
	p := file.AbsolutePath()
	if _, ok := wb.entries[p]; !ok {
		wb.entries[p] = &wbEntry{path: p, file: file, state: wbLocal, base: file.loadedBase, landed: 1, staged: 1}
	}
	return nil
}

func (wb *WriteBehind) isFenced() bool {
	wb.mu.Lock()
	defer wb.mu.Unlock()
	return wb.fenced
}

// Whether p's staged copy must stay because something of it is not on HopsFS. A registered entry
// with nothing pending (a change admitted that never happened) is dropped. Callers closing the
// last handle hold the file's dataMutex, which guards dirty.
func (wb *WriteBehind) retain(p string, dirty bool) bool {
	wb.mu.Lock()
	defer wb.mu.Unlock()
	e, ok := wb.entries[p]
	if !ok {
		return false
	}
	if !dirty && e.state == wbLocal && e.landed == e.staged && e.landed > 0 {
		delete(wb.entries, p)
		return false
	}
	return true
}

func (wb *WriteBehind) pending(p string) bool {
	wb.mu.Lock()
	defer wb.mu.Unlock()
	_, ok := wb.entries[p]
	return ok
}

// Queues the staged content of file as a new generation and returns it. Waits at most
// WriteBehindWait for room; past it the entry is failed with the bytes kept staged.
// Caller holds file.dataMutex.
func (wb *WriteBehind) enqueue(file *FileINode, size int64) (uint64, error) {
	return wb.enqueueBy(file, size, time.Now().Add(WriteBehindWait))
}

func (wb *WriteBehind) enqueueBy(file *FileINode, size int64, deadline time.Time) (uint64, error) {
	wb.mu.Lock()
	defer wb.mu.Unlock()
	p := file.AbsolutePath()
	e, ok := wb.entries[p]
	if !ok {
		e = &wbEntry{path: p, file: file, base: file.loadedBase, landed: 1, staged: 1}
		wb.entries[p] = e
	}
	for !e.counted && (wb.files+1 > WriteBehindMaxFiles || wb.bytes+size > WriteBehindMaxBytes) {
		if time.Now().After(deadline) {
			e.staged++
			// close returns this error, so it counts as reported; fsync and drains still fail.
			e.state, e.err, e.reported = wbFailed, errors.New("write-behind queue full"), true
			wb.count(e, size)
			logger.Error("Write-behind queue full: the file stays staged", logger.Fields{Operation: Flush, Path: p, Bytes: size})
			wb.cond.Broadcast()
			return e.staged, syscall.EIO
		}
		wb.waitUntil(deadline)
	}
	e.staged++
	wb.count(e, size)
	if e.state != wbQueued && e.state != wbUploading {
		wb.queue = append(wb.queue, e)
	}
	if e.state != wbUploading {
		e.state = wbQueued
	}
	wb.cond.Broadcast()
	return e.staged, nil
}

// Re-queues a failed entry's staged generation (fsync of a failed file). Caller holds wb.mu.
func (wb *WriteBehind) retry(e *wbEntry) {
	e.state, e.err, e.reported = wbQueued, nil, false
	wb.queue = append(wb.queue, e)
	wb.cond.Broadcast()
}

func (wb *WriteBehind) count(e *wbEntry, size int64) {
	if e.counted {
		wb.bytes += size - e.bytes
	} else {
		wb.files++
		wb.bytes += size
		e.counted = true
	}
	e.bytes = size
}

func (wb *WriteBehind) uncount(e *wbEntry) {
	if e.counted {
		wb.files--
		wb.bytes -= e.bytes
		e.counted = false
	}
}

// sync.Cond has no timed wait: a timer broadcasts at the deadline.
func (wb *WriteBehind) waitUntil(deadline time.Time) {
	t := time.AfterFunc(time.Until(deadline), func() {
		wb.mu.Lock()
		wb.cond.Broadcast()
		wb.mu.Unlock()
	})
	wb.cond.Wait()
	t.Stop()
}

// Waits until generation gen of file landed, or failed. Used by fsync.
func (wb *WriteBehind) await(p string, gen uint64) error {
	wb.mu.Lock()
	defer wb.mu.Unlock()
	for {
		e, ok := wb.entries[p]
		if !ok {
			return nil // landed and released
		}
		if e.landed >= gen {
			return nil
		}
		if e.unresolved() {
			e.reported = true
			return syscall.EIO
		}
		wb.cond.Wait()
	}
}

// Fsync of file: queue its dirty content (or retry a failed upload) and wait for it.
// Caller holds file.dataMutex and releases it before waiting.
func (wb *WriteBehind) fsyncGen(file *FileINode) (uint64, bool) {
	wb.mu.Lock()
	defer wb.mu.Unlock()
	e, ok := wb.entries[file.AbsolutePath()]
	if !ok {
		return 0, false
	}
	if e.state == wbFailed {
		wb.retry(e)
	}
	return e.staged, true
}

// The error of an unresolved path, once (open, stat, write, rename return it); nil otherwise.
func (wb *WriteBehind) takeError(p string) error {
	wb.mu.Lock()
	defer wb.mu.Unlock()
	if e, ok := wb.entries[p]; ok && e.unresolved() && !e.reported {
		e.reported = true
		return syscall.EIO
	}
	return nil
}

func (wb *WriteBehind) worker() {
	for {
		wb.mu.Lock()
		for len(wb.queue) == 0 && !wb.closed {
			wb.cond.Wait()
		}
		if wb.closed {
			wb.mu.Unlock()
			return
		}
		e := wb.queue[0]
		wb.queue = wb.queue[1:]
		if e.state != wbQueued || wb.entries[e.path] != e {
			wb.mu.Unlock()
			continue
		}
		e.state = wbUploading
		base := e.base
		wb.mu.Unlock()

		wb.upload(e, base)
	}
}

// Uploads the staged content of e holding the file's dataMutex, so the bytes sent are exactly
// one generation; a writer of that file waits, as it does under a synchronous flush.
func (wb *WriteBehind) upload(e *wbEntry, base *Attrs) {
	file := e.file
	file.lockData()
	wb.mu.Lock()
	gen := e.staged
	wb.mu.Unlock()

	tuple, conflictPath, err := wb.publish(file, e.path, base, gen)

	wb.mu.Lock()
	switch {
	case err != nil:
		e.state, e.err, e.reported = wbFailed, err, false
		logger.Error("Write-behind upload failed: the file stays staged", logger.Fields{Operation: Flush, Path: e.path, Error: err})
	case conflictPath != "":
		e.state, e.err, e.reported, e.conflictPath = wbConflict, fmt.Errorf("conflict: HopsFS has another version, ours is in %s", conflictPath), false, conflictPath
		logger.Error("Write-behind conflict: HopsFS keeps the other version", logger.Fields{Operation: Flush, Path: e.path, To: conflictPath})
	default:
		e.landed, e.base = gen, &tuple
		if e.staged > gen {
			e.state = wbQueued
			wb.queue = append(wb.queue, e)
		} else if e.state == wbUploading {
			e.state = wbLocal
		}
	}
	landed := err == nil && conflictPath == "" && e.staged == gen
	if landed {
		wb.uncount(e)
	}
	wb.cond.Broadcast()
	wb.mu.Unlock()

	if landed {
		file.loadedBase = &tuple
		wb.releaseIfClean(file, e)
	}
	file.unlockData()

	if landed {
		file.lockFile()
		file.Attrs.Inode, file.Attrs.Size, file.Attrs.Mtime = tuple.Inode, tuple.Size, tuple.Mtime
		file.unlockFile()
	}
}

// Drops a landed entry whose file is not being written, and releases the staging file the way
// closing the last handle does. Caller holds file.dataMutex.
func (wb *WriteBehind) releaseIfClean(file *FileINode, e *wbEntry) {
	if file.isDirty {
		return
	}
	file.lockFileHandles()
	defer file.unlockFileHandles()
	wb.mu.Lock()
	if wb.entries[e.path] == e && e.state == wbLocal && e.landed == e.staged {
		delete(wb.entries, e.path)
	}
	wb.mu.Unlock()
	if len(file.activeHandles) == 0 {
		file.Attrs.Size, file.Attrs.Mtime = e.base.Size, e.base.Mtime
		file.closeStaging()
	}
}

// Writes the staged content to the temp, reads its tuple, and publishes it. Returns the new
// base, or the conflict file our bytes went to, or the error.
func (wb *WriteBehind) publish(file *FileINode, p string, base *Attrs, gen uint64) (Attrs, string, error) {
	file.lockFileHandles()
	lrwfp, ok := file.fileProxy.(*LocalRWFileProxy)
	file.unlockFileHandles()
	if !ok {
		return Attrs{}, "", fmt.Errorf("no staged copy of %s", p)
	}
	temp := path.Join(path.Dir(p), wbTempName(path.Base(p)))

	var tuple Attrs
	op := wb.fs.RetryPolicy.StartOperation()
	for {
		var err error
		tuple, err = wb.writeTemp(file, lrwfp, temp, p)
		if err == nil {
			break
		}
		if IsSuccessOrNonRetriableError(err) || !op.ShouldRetry("write-behind upload of %s: %s", p, err) {
			return Attrs{}, "", err
		}
	}

	wbStage("before-publish", p)
	accessor := wb.fs.getDFSConnector()
	if base != nil {
		current, err := accessor.Stat(p)
		if err != nil && err != syscall.ENOENT {
			return Attrs{}, "", err
		}
		if err == syscall.ENOENT || !sameVersion(current, *base) {
			return wb.toConflict(accessor, p, temp, tuple, gen)
		}
		if err := accessor.Rename2(temp, p, hdfs.RENAME_OPTION_NONE); err != nil {
			return Attrs{}, "", err
		}
		return tuple, "", nil
	}

	if err := accessor.Rename2(temp, p, hdfs.RENAME_NOREPLACE); err != nil {
		// A retried rename whose first call landed answers "exists": then the path holds ours.
		if err == syscall.EEXIST {
			if current, statErr := accessor.Stat(p); statErr == nil && sameVersion(current, tuple) {
				return tuple, "", nil
			}
			return wb.toConflict(accessor, p, temp, tuple, gen)
		}
		return Attrs{}, "", err
	}
	return tuple, "", nil
}

func (wb *WriteBehind) toConflict(accessor HdfsAccessor, p, temp string, tuple Attrs, gen uint64) (Attrs, string, error) {
	conflict := fmt.Sprintf("%s.conflict-%s-%d", p, wb.fs.Clock.Now().UTC().Format("20060102T150405.000Z"), gen)
	if err := accessor.Rename2(temp, conflict, hdfs.RENAME_NOREPLACE); err != nil {
		return Attrs{}, "", fmt.Errorf("conflict on %s, and moving ours to %s failed: %w", p, conflict, err)
	}
	return tuple, conflict, nil
}

func (wb *WriteBehind) writeTemp(file *FileINode, lrwfp *LocalRWFileProxy, temp, p string) (Attrs, error) {
	accessor := wb.fs.getDFSConnector()
	w, err := accessor.CreateFileWithGroup(temp, file.Attrs.Mode, true, file.Attrs.DFSGroupName)
	if err != nil {
		return Attrs{}, err
	}
	stat, err := lrwfp.localFile.Stat()
	if err != nil {
		w.Close()
		return Attrs{}, err
	}
	b := make([]byte, 65536)
	r := io.NewSectionReader(lrwfp.localFile, 0, stat.Size())
	for {
		nr, rerr := r.Read(b)
		if nr > 0 {
			if _, werr := w.Write(b[:nr]); werr != nil {
				w.Close()
				return Attrs{}, werr
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			w.Close()
			return Attrs{}, rerr
		}
	}
	if err := w.Close(); err != nil {
		return Attrs{}, err
	}
	wbStage("temp-complete", p)
	// Nobody else writes the temp, so this is exactly our bytes; a rename keeps all three.
	return accessor.Stat(temp)
}

func sameVersion(a, b Attrs) bool {
	return a.Inode == b.Inode && a.Size == b.Size && a.Mtime.Equal(b.Mtime)
}

// Unresolved is one path whose upload did not land, for drain answers and logs.
type Unresolved struct {
	Path  string
	Error string
}

// Waits until no entry under prefix (a path, or a directory and everything below it) has a
// queued or uploading generation, or timeout passes. Files open and never closed are not
// waited for. Returns the unresolved entries and whether the timeout passed.
func (wb *WriteBehind) Drain(prefix string, timeout time.Duration) ([]Unresolved, bool) {
	deadline := time.Now().Add(timeout)
	wb.mu.Lock()
	defer wb.mu.Unlock()
	for {
		busy := false
		var out []Unresolved
		for p, e := range wb.entries {
			if !inScope(p, prefix) {
				continue
			}
			switch e.state {
			case wbQueued, wbUploading:
				busy = true
			case wbFailed, wbConflict:
				out = append(out, Unresolved{Path: p, Error: e.err.Error()})
			}
		}
		if !busy {
			sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
			return out, false
		}
		if time.Now().After(deadline) {
			for p, e := range wb.entries {
				if inScope(p, prefix) && (e.state == wbQueued || e.state == wbUploading) {
					out = append(out, Unresolved{Path: p, Error: "still " + e.state.String()})
				}
			}
			sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
			return out, true
		}
		wb.waitUntil(deadline)
	}
}

// lockBy takes m, or gives up at deadline.
func lockBy(m *sync.Mutex, deadline time.Time) bool {
	for !m.TryLock() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(10 * time.Millisecond)
	}
	return true
}

func inScope(p, prefix string) bool {
	return prefix == "" || prefix == "/" || p == prefix || strings.HasPrefix(p, strings.TrimSuffix(prefix, "/")+"/")
}

// Stop fences writes, queues every file written and not closed, and drains everything, all
// within one timeout. The SIGTERM path and the control socket's stop both run it.
//
// Every change is admitted under the file's dataMutex and registered before it happens (admit,
// created), and the fence is set under wb.mu with the snapshot. So the snapshot holds every file
// a change was let into; taking each file's dataMutex waits for a change still running, and any
// change after the fence is refused with EROFS.
func (wb *WriteBehind) Stop(timeout time.Duration) ([]Unresolved, bool) {
	deadline := time.Now().Add(timeout)
	wb.mu.Lock()
	wb.fenced = true
	seen := map[*FileINode]bool{}
	var files []*FileINode
	for _, e := range wb.entries {
		if !seen[e.file] {
			seen[e.file] = true
			files = append(files, e.file)
		}
	}
	wb.mu.Unlock()
	logger.Info(fmt.Sprintf("Write-behind stop: writes fenced, %d files to check", len(files)), nil)

	var busy []Unresolved
	for _, file := range files {
		if !lockBy(&file.dataMutex, deadline) {
			busy = append(busy, Unresolved{Path: file.AbsolutePath(), Error: "still being written at the stop deadline"})
			continue
		}
		if file.isDirty {
			if size, err := file.stagedSize(); err == nil {
				if _, err := wb.enqueueBy(file, size, deadline); err == nil {
					file.isDirty = false
				}
			}
		}
		file.unlockData()
	}
	out, timedOut := wb.Drain("", time.Until(deadline))
	if len(busy) > 0 {
		out, timedOut = append(out, busy...), true
		sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	}
	for _, u := range out {
		logger.Error("Write-behind stop: not on HopsFS", logger.Fields{Operation: Flush, Path: u.Path, Error: u.Error})
	}
	return out, timedOut
}

// Drops the pending entry of p and reloads its base from HopsFS: HopsFS wins. Refused while
// the file is open.
func (wb *WriteBehind) Discard(file *FileINode) error {
	file.lockData()
	defer file.unlockData()
	file.lockFileHandles()
	defer file.unlockFileHandles()
	if len(file.activeHandles) > 0 {
		return syscall.EBUSY
	}
	p := file.AbsolutePath()
	wb.mu.Lock()
	if e, ok := wb.entries[p]; ok {
		if e.state == wbQueued || e.state == wbUploading {
			wb.mu.Unlock()
			return syscall.EBUSY
		}
		wb.uncount(e)
		delete(wb.entries, p)
		wb.cond.Broadcast()
	}
	wb.mu.Unlock()

	file.isDirty = false
	if file.fileProxy != nil {
		file.fileProxy.Close()
		file.fileProxy = nil
	}
	if StagingCache != nil {
		StagingCache.Remove(p)
	}
	current, err := wb.fs.getDFSConnector().Stat(p)
	switch {
	case err == nil:
		file.loadedBase = &current
	case err == syscall.ENOENT:
		file.loadedBase = nil
	default:
		return err
	}
	file.InvalidateMetadataCache()
	logger.Info("Write-behind discard: HopsFS version kept", logger.Fields{Operation: Flush, Path: p})
	return nil
}

// Drops the entry of a file unlinked through the mount. Returns whether HopsFS never had it
// (created here, never landed), so the caller skips the HopsFS remove.
func (wb *WriteBehind) unlinked(p string) bool {
	wb.mu.Lock()
	defer wb.mu.Unlock()
	e, ok := wb.entries[p]
	if !ok {
		return false
	}
	neverLanded := e.base == nil && e.landed == 0
	wb.uncount(e)
	delete(wb.entries, p)
	wb.cond.Broadcast()
	return neverLanded
}

// Waits for p's queued or uploading generation, so an unlink or rename sees HopsFS current.
func (wb *WriteBehind) settle(prefix string) error {
	out, timedOut := wb.Drain(prefix, WriteBehindWait)
	if timedOut {
		return syscall.EBUSY
	}
	if len(out) > 0 {
		return syscall.EIO
	}
	return nil
}

// Moves entries of oldPath (a file, or a directory and everything below it) to newPath.
func (wb *WriteBehind) renamed(oldPath, newPath string) {
	wb.mu.Lock()
	defer wb.mu.Unlock()
	for p, e := range wb.entries {
		if inScope(p, oldPath) {
			np := newPath + strings.TrimPrefix(p, oldPath)
			delete(wb.entries, p)
			e.path = np
			wb.entries[np] = e
		}
	}
}

// Files under dir that HopsFS does not have yet, for directory listings.
func (wb *WriteBehind) newChildren(dir string) []*wbEntry {
	wb.mu.Lock()
	defer wb.mu.Unlock()
	var out []*wbEntry
	for p, e := range wb.entries {
		if path.Dir(p) == dir && e.base == nil && e.landed == 0 {
			out = append(out, e)
		}
	}
	return out
}

func (file *FileINode) stagedSize() (int64, error) {
	lrwfp, ok := file.fileProxy.(*LocalRWFileProxy)
	if !ok {
		return 0, os.ErrNotExist
	}
	st, err := lrwfp.localFile.Stat()
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}

// Drops a file HopsFS never had (created here, never landed) unless a worker is uploading it.
// Returns whether it did.
func (wb *WriteBehind) cancelNew(p string) bool {
	wb.mu.Lock()
	defer wb.mu.Unlock()
	e, ok := wb.entries[p]
	if !ok || e.base != nil || e.landed != 0 || e.state == wbUploading || e.state == wbConflict {
		return false
	}
	wb.uncount(e)
	delete(wb.entries, p)
	wb.cond.Broadcast()
	return true
}

// Queues the file's content when it was written and not closed.
func (file *FileINode) queueIfDirty() error {
	file.lockData()
	defer file.unlockData()
	if !file.isDirty {
		return nil
	}
	size, err := file.stagedSize()
	if err != nil {
		return err
	}
	file.isDirty = false
	_, err = file.FileSystem.WB.enqueue(file, size)
	return err
}
