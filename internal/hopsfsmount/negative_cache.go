// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package hopsfsmount

import (
	"container/list"
	"sync"
	"time"
)

// negativeEntry is one name found absent, as the mount-wide queue holds it.
type negativeEntry struct {
	dir    *baseDirNode
	name   string
	expiry time.Time
}

// negativeQuota bounds the names in the negative caches of all directories.
// Every entry has the same lifetime, so the queue is in expiry order and the
// expired ones are at its front, whichever directory they belong to.
//
// Lock order: a directory's childrenMutex, then mu. takeExpired returns with
// no lock held so that its caller can take each owner's childrenMutex.
type negativeQuota struct {
	mu    sync.Mutex
	queue list.List // of negativeEntry, oldest first
}

// reserve queues an entry and returns its handle, or nil when the mount holds
// NegativeCacheMaxEntries names already.
func (q *negativeQuota) reserve(entry negativeEntry) *list.Element {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.queue.Len() >= NegativeCacheMaxEntries {
		return nil
	}
	return q.queue.PushBack(entry)
}

// release gives a slot back. Releasing a handle twice is harmless.
func (q *negativeQuota) release(handle *list.Element) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.queue.Remove(handle)
}

// takeExpired removes the entries that expired before now from the queue and
// returns their handles: the caller deletes each from its directory.
func (q *negativeQuota) takeExpired(now time.Time) []*list.Element {
	q.mu.Lock()
	defer q.mu.Unlock()
	var expired []*list.Element
	for front := q.queue.Front(); front != nil; front = q.queue.Front() {
		if !now.After(front.Value.(negativeEntry).expiry) {
			break
		}
		q.queue.Remove(front)
		expired = append(expired, front)
	}
	return expired
}

func (q *negativeQuota) len() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return int64(q.queue.Len())
}

// expireNegativeEntries drops the expired names of every directory. Must be
// called with no childrenMutex held.
func (filesystem *FileSystem) expireNegativeEntries(now time.Time) {
	for _, handle := range filesystem.negatives.takeExpired(now) {
		entry := handle.Value.(negativeEntry)
		entry.dir.lockChildrenMutex()
		// The name may have been removed and cached again since.
		if entry.dir.negativeCache[entry.name] == handle {
			delete(entry.dir.negativeCache, entry.name)
		}
		entry.dir.unlockChildrenMutex()
	}
}
