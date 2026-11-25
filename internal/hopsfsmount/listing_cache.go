// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package hopsfsmount

import (
	"container/list"
	"sync"
	"time"
)

// listingEntryOverhead is the accounted cost of one listed entry besides its
// strings: the Attrs value (152 B) and its map slot, rounded up.
const listingEntryOverhead = 256

// dirListing is the attributes of one directory's entries as its last
// ReadDir returned them.
type dirListing struct {
	dir     *baseDirNode
	entries map[string]Attrs
	bytes   int64
	expires time.Time
	elem    *list.Element
}

// listingCache keeps what ReadDir learned about a directory's entries, so the
// lookups that follow a listing need no backend Stat and no node exists for
// an entry until it is looked up. It holds at most maxBytes, evicting whole
// listings oldest first, and a listing lives for CacheAttrsTimeDuration.
//
// mu is a leaf lock: nothing else is taken while it is held.
type listingCache struct {
	mu       sync.Mutex
	maxBytes int64
	bytes    int64
	entries  int64
	order    *list.List // of *dirListing, oldest listing first
}

func newListingCache(maxBytes int64) *listingCache {
	return &listingCache{maxBytes: maxBytes, order: list.New()}
}

// generation returns a token for store: taken before the backend is asked
// for dir's entries, it lets store tell whether an entry changed meanwhile.
func (c *listingCache) generation(dir *baseDirNode) uint64 {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return dir.listingGen
}

func listingEntryBytes(a Attrs) int64 {
	return int64(listingEntryOverhead + len(a.Name) + len(a.DFSUserName) + len(a.DFSGroupName))
}

// store replaces dir's listing with all, which the backend returned after
// generation gave gen. Nothing is kept when an entry was invalidated since,
// because all may predate that change, or when the listing is larger than the
// whole budget: the lookups that follow then go to the backend instead.
func (c *listingCache) store(dir *baseDirNode, all []Attrs, now time.Time, gen uint64) {
	if c == nil || c.maxBytes <= 0 || CacheAttrsTimeDuration <= 0 {
		return
	}
	var bytes int64
	for _, a := range all {
		bytes += listingEntryBytes(a)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.removeLocked(dir)
	// An empty listing answers no lookup and would be held at no charge.
	if len(all) == 0 || bytes > c.maxBytes || dir.listingGen != gen {
		return
	}
	entries := make(map[string]Attrs, len(all))
	for _, a := range all {
		entries[a.Name] = a
	}
	for front := c.order.Front(); front != nil; front = c.order.Front() {
		oldest := front.Value.(*dirListing)
		if !now.After(oldest.expires) && c.bytes+bytes <= c.maxBytes {
			break
		}
		c.removeLocked(oldest.dir)
	}
	l := &dirListing{dir: dir, entries: entries, bytes: bytes, expires: now.Add(CacheAttrsTimeDuration)}
	l.elem = c.order.PushBack(l)
	dir.listing = l
	c.bytes += bytes
	c.entries += int64(len(entries))
}

// lookup returns the listed attributes of name in dir, if dir has a listing
// that has not expired.
func (c *listingCache) lookup(dir *baseDirNode, name string, now time.Time) (Attrs, bool) {
	if c == nil {
		return Attrs{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	l := dir.listing
	if l == nil {
		return Attrs{}, false
	}
	if now.After(l.expires) {
		c.removeLocked(dir)
		return Attrs{}, false
	}
	attrs, ok := l.entries[name]
	return attrs, ok
}

// invalidate forgets what the listing of dir says about name, after a
// mutation of that entry through this mount.
func (c *listingCache) invalidate(dir *baseDirNode, name string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	dir.listingGen++
	l := dir.listing
	if l == nil {
		return
	}
	if a, ok := l.entries[name]; ok {
		delete(l.entries, name)
		// The map keeps the deleted entry's slot until the listing goes, so
		// only the strings are given back here.
		size := listingEntryBytes(a) - listingEntryOverhead
		l.bytes -= size
		c.bytes -= size
		c.entries--
	}
}

func (c *listingCache) drop(dir *baseDirNode) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removeLocked(dir)
}

func (c *listingCache) removeLocked(dir *baseDirNode) {
	l := dir.listing
	if l == nil {
		return
	}
	c.order.Remove(l.elem)
	c.bytes -= l.bytes
	c.entries -= int64(len(l.entries))
	dir.listing = nil
}

func (c *listingCache) usage() (bytes, entries int64) {
	if c == nil {
		return 0, 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bytes, c.entries
}
