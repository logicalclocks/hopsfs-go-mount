// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package hopsfsmount

import (
	"container/list"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
	"hopsworks.ai/hopsfsmount/internal/hopsfsmount/logger"
)

// StagingFileCache manages cached staging files using LRU eviction.
// When files are written and closed, their local staging copies are kept
// in this cache for faster reopening instead of downloading from DFS again.
type StagingFileCache struct {
	mu           sync.Mutex
	maxEntries   int
	entries      map[string]*CacheEntry
	entriesStats map[string]*EntryStats
	lruList      *list.List

	// Global cache stats
	globalStatsEnabled bool
	globalHits         atomic.Int64
	globalMisses       atomic.Int64
	globalCachedBytes  atomic.Int64

	diskUsageExceeded atomic.Bool
	monitorStopOnce   sync.Once
	monitorStop       chan struct{}
	monitorDone       chan struct{}
}

// CacheEntry represents a cached staging file
type CacheEntry struct {
	hdfsPath   string
	handle     *os.File
	size       int64
	mtime      time.Time // modification time when cached, used to detect upstream changes
	lruElement *list.Element
}

// EntryStats tracks access statistics for a cached file path.
// Stats persist across Get/Put cycles until the entry is evicted.
type EntryStats struct {
	HitCount   int
	LastAccess time.Time
}

// CacheStats contains global cache statistics.
type CacheStats struct {
	Hits    int64
	Misses  int64
	Entries int
	Bytes   int64
}

// Cache is the global staging file cache instance, initialized in config.go if caching is enabled
var StagingCache *StagingFileCache

// NewStagingFileCache creates a new cache with the given maximum number of entries.
// When the cache is full, the least recently used entry is evicted.
func NewStagingFileCache(maxEntries int) *StagingFileCache {
	cache := &StagingFileCache{
		maxEntries:   maxEntries,
		entries:      make(map[string]*CacheEntry),
		entriesStats: make(map[string]*EntryStats),
		lruList:      list.New(),
	}

	cache.startDiskUsageMonitor(StagingCacheDiskUsageCheckInterval)
	if StagingCacheStatsReportingInterval > 0 {
		cache.globalStatsEnabled = true
		cache.startStatsReporter(StagingCacheStatsReportingInterval)
	}
	return cache
}

// Get retrieves a cached file for the given HDFS path.
// The upstreamSize and upstreamMtime parameters are the current metadata from HopsFS,
// used to validate that the cached file hasn't been modified by another client.
// Returns the file handle and true if found and valid, or (nil, false) if not cached or stale.
// If the cache entry is stale (metadata mismatch), it is automatically removed.
// Moves the entry to the front of the LRU list on successful access.
// Note: The returned handle is removed from the cache - caller takes ownership.
func (c *StagingFileCache) Get(hdfsPath string, upstreamSize int64, upstreamMtime time.Time) (*os.File, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[hdfsPath]
	if !ok {
		if c.globalStatsEnabled {
			c.globalMisses.Add(1)
		}
		logger.Debug("Cache miss for staging file", logger.Fields{
			Operation: cache,
			Path:      hdfsPath,
		})
		return nil, false
	}

	// Validate cache entry against upstream metadata
	// If size or mtime differs, the file was modified by another client
	if entry.size != upstreamSize || !entry.mtime.Equal(upstreamMtime) {
		if c.globalStatsEnabled {
			c.globalMisses.Add(1)
		}
		logger.Debug(fmt.Sprintf("Cached staging file is stale, invalidating. cached[size=%d, mtime=%v] upstream[size=%d, mtime=%v]",
			entry.size, entry.mtime, upstreamSize, upstreamMtime), logger.Fields{
			Operation: cache,
			Path:      hdfsPath,
		})
		c.removeEntry(hdfsPath)
		return nil, false
	}

	// Remove from cache - caller takes ownership of the handle
	handle := entry.handle
	c.lruList.Remove(entry.lruElement)
	delete(c.entries, hdfsPath)

	// Update stats
	c.globalCachedBytes.Add(-entry.size)
	hitCount := c.recordHit(hdfsPath)

	// Seek to beginning for the caller
	_, _ = handle.Seek(0, 0)

	logger.Debug("Cache hit for staging file", logger.Fields{
		Operation: cache,
		Path:      hdfsPath,
		CacheHits: hitCount,
	})

	return handle, true
}

// Put adds a staging file to the cache, taking ownership of the file handle.
// If the cache is full, the least recently used entry is evicted first.
// If an entry already exists for this path, it is replaced.
// The mtime parameter should be the modification time from HopsFS, used
// to detect if the file was modified by another client.
func (c *StagingFileCache) Put(hdfsPath string, handle *os.File, size int64, mtime time.Time) {
	if c.maxEntries <= 0 {
		c.discardHandle(hdfsPath, handle)
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if entry already exists
	if _, ok := c.entries[hdfsPath]; ok {
		c.removeEntry(hdfsPath)
	}

	if !c.ShouldCache(size, hdfsPath, false) {
		c.discardHandle(hdfsPath, handle)
		return
	}

	// Evict oldest entries if cache is full
	for len(c.entries) >= c.maxEntries {
		c.evictOldest()
	}

	entry := &CacheEntry{
		hdfsPath: hdfsPath,
		handle:   handle,
		size:     size,
		mtime:    mtime,
	}
	entry.lruElement = c.lruList.PushFront(entry)
	c.entries[hdfsPath] = entry

	// Update global stats
	c.globalCachedBytes.Add(size)

	// Initialize stats for new paths (preserve existing stats from previous Get/Put cycles)
	if c.entriesStats[hdfsPath] == nil {
		c.entriesStats[hdfsPath] = &EntryStats{}
	}

	logger.Debug("Added staging file to cache", logger.Fields{
		Operation: cache,
		Path:      hdfsPath,
		FileSize:  size,
		Entries:   len(c.entries),
	})
}

// Remove explicitly removes an entry from the cache.
// This should be called when a file is deleted in DFS.
func (c *StagingFileCache) Remove(hdfsPath string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.removeEntry(hdfsPath)
}

// Rename transfers a cache entry from oldPath to newPath.
// If the entry doesn't exist for oldPath, this is a no-op.
// If an entry already exists for newPath, it is replaced.
func (c *StagingFileCache) Rename(oldPath, newPath string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[oldPath]
	if !ok {
		// No cache entry for old path, nothing to transfer
		logger.Debug("Cache rename: no entry for old path", logger.Fields{
			Operation: cache,
			From:      oldPath,
			To:        newPath,
		})
		return
	}

	// Remove any existing entry at the new path
	if _, exists := c.entries[newPath]; exists {
		c.removeEntry(newPath)
	}

	// Update the entry's hdfsPath and move to new key
	delete(c.entries, oldPath)
	entry.hdfsPath = newPath
	c.entries[newPath] = entry

	// Transfer stats from old path to new path
	if stats, hasStats := c.entriesStats[oldPath]; hasStats {
		delete(c.entriesStats, oldPath)
		c.entriesStats[newPath] = stats
	}

	// Move to front of LRU (most recently used)
	c.lruList.MoveToFront(entry.lruElement)

	logger.Debug("Cache entry renamed", logger.Fields{
		Operation: cache,
		From:      oldPath,
		To:        newPath,
	})
}

// Must be called with mutex held.
func (c *StagingFileCache) recordHit(hdfsPath string) int {
	if c.globalStatsEnabled {
		c.globalHits.Add(1)
	}

	stats := c.entriesStats[hdfsPath]
	if stats == nil {
		stats = &EntryStats{}
		c.entriesStats[hdfsPath] = stats
	}

	stats.HitCount++
	stats.LastAccess = time.Now()

	return stats.HitCount
}

// removeEntry removes an entry without locking (internal use only)
func (c *StagingFileCache) removeEntry(hdfsPath string) {
	entry, ok := c.entries[hdfsPath]
	if !ok {
		return
	}

	// Remove from LRU list
	c.lruList.Remove(entry.lruElement)

	// Update global stats
	c.globalCachedBytes.Add(-entry.size)

	// Delete the local file and close the handle
	c.discardHandle(hdfsPath, entry.handle)

	// Remove from map
	delete(c.entries, hdfsPath)

	// Log stats on eviction and cleanup
	hitCount := 0
	if stats, hasStats := c.entriesStats[hdfsPath]; hasStats {
		hitCount = stats.HitCount
		delete(c.entriesStats, hdfsPath)
	}

	logger.Debug("Removed staging file from cache", logger.Fields{
		Operation: cache,
		Path:      hdfsPath,
		CacheHits: hitCount,
	})
}

// evictOldest removes the least recently used entry from the cache.
// Must be called with mutex held.
func (c *StagingFileCache) evictOldest() {
	oldest := c.lruList.Back()
	if oldest == nil {
		return
	}

	entry := oldest.Value.(*CacheEntry)
	c.removeEntry(entry.hdfsPath)
}

// Clear removes all entries from the cache.
// This should be called during shutdown.
func (c *StagingFileCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for hdfsPath := range c.entries {
		c.removeEntry(hdfsPath)
	}

	// Clear any remaining entriesStats (e.g., for entries that were Get'd but not yet Put back)
	for hdfsPath := range c.entriesStats {
		delete(c.entriesStats, hdfsPath)
	}

	logger.Debug("Cleared staging file cache", logger.Fields{
		Operation: cache,
	})
}

// Shutdown stops background goroutines and clears the cache.
func (c *StagingFileCache) Shutdown() {
	c.stopBackgroundWorkers()
	c.Clear()
	logger.Info("Staging file cache shutdown complete", nil)
}

// Size returns the current number of entries in the cache.
func (c *StagingFileCache) Size() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

// Contains checks if a path exists in the cache (without validation).
func (c *StagingFileCache) Contains(hdfsPath string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.entries[hdfsPath]
	return ok
}

// GetAndResetStats returns cache statistics and resets hit/miss counters.
func (c *StagingFileCache) GetAndResetStats() CacheStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return CacheStats{
		Hits:    c.globalHits.Swap(0),
		Misses:  c.globalMisses.Swap(0),
		Entries: len(c.entries),
		Bytes:   c.globalCachedBytes.Load(),
	}
}

// ShouldCache returns true if a file should be cached locally.
func (c *StagingFileCache) ShouldCache(fileSize int64, path string, applyDownloadLimit bool) bool {
	if applyDownloadLimit && StagingCacheMaxDownloadSize > 0 && fileSize > StagingCacheMaxDownloadSize {
		logger.Debug("File too large for download caching", logger.Fields{
			Operation: cache,
			Path:      path,
			FileSize:  fileSize,
		})
		return false
	}

	// Check if file exceeds max cacheable size
	if StagingCacheMaxFileSize > 0 && fileSize > StagingCacheMaxFileSize {
		logger.Debug("File too large for caching", logger.Fields{
			Operation: cache,
			Path:      path,
			FileSize:  fileSize,
		})
		return false
	}

	if c.diskUsageExceeded.Load() {
		logger.Debug("Disk usage too high for caching", logger.Fields{
			Operation: cache,
			Path:      path,
		})
		return false
	}

	return true
}

func (c *StagingFileCache) discardHandle(hdfsPath string, handle *os.File) {
	localPath := handle.Name()
	if err := handle.Close(); err != nil {
		logger.Warn("Failed to close staging file handle", logger.Fields{
			Operation: cache,
			Path:      hdfsPath,
			Error:     err,
		})
	}
	// Try to remove the file. Normally staging files are unlinked at creation,
	// so this will return "not found" which we ignore. If the file still exists
	// (unexpected), this cleans it up.
	if localPath != "" {
		if err := os.Remove(localPath); err != nil && !os.IsNotExist(err) {
			logger.Warn("Failed to remove staging file", logger.Fields{
				Operation: cache,
				Path:      hdfsPath,
				TmpFile:   localPath,
				Error:     err,
			})
		}
	}
}

func (c *StagingFileCache) startDiskUsageMonitor(interval time.Duration) {
	c.monitorStop = make(chan struct{})
	c.monitorDone = make(chan struct{})
	c.updateDiskUsageFlag()
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		defer close(c.monitorDone)
		for {
			select {
			case <-ticker.C:
				c.updateDiskUsageFlag()
			case <-c.monitorStop:
				return
			}
		}
	}()
}

// stopBackgroundWorkers stops all background goroutines.
func (c *StagingFileCache) stopBackgroundWorkers() {
	c.monitorStopOnce.Do(func() {
		if c.monitorStop == nil {
			return
		}
		close(c.monitorStop)
		<-c.monitorDone
	})
}

// startStatsReporter starts a goroutine that logs and resets global cache statistics periodically.
func (c *StagingFileCache) startStatsReporter(interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				stats := c.GetAndResetStats()
				total := stats.Hits + stats.Misses
				hitRatio := float64(0)
				if total > 0 {
					hitRatio = float64(stats.Hits) / float64(total) * 100
				}
				logger.Info(fmt.Sprintf(
					"Cache stats: hits=%d, misses=%d, hit_ratio=%.1f%%, cached_files=%d, cached_bytes=%d",
					stats.Hits, stats.Misses, hitRatio, stats.Entries, stats.Bytes), logger.Fields{
					Operation: cache,
				})
			case <-c.monitorStop:
				return
			}
		}
	}()
}

// hitRatio returns the current cache hit ratio as a percentage.
func (c *StagingFileCache) hitRatio() float64 {
	hits := c.globalHits.Load()
	misses := c.globalMisses.Load()
	total := hits + misses
	if total == 0 {
		return 0
	}
	return float64(hits) / float64(total) * 100
}

func (c *StagingFileCache) updateDiskUsageFlag() {
	excessBytes, err := diskUsageExcess(StagingDir, StagingCacheMaxDiskUsage)
	if err != nil {
		logger.Warn("Failed to check disk usage for caching", logger.Fields{
			Operation: cache,
			Path:      StagingDir,
			Error:     err,
		})
		return
	}
	c.diskUsageExceeded.Store(excessBytes > 0)

	if excessBytes > 0 {
		c.evictUntilBelowThreshold(excessBytes)
	}
}

// evictUntilBelowThreshold removes cached entries until disk usage falls below
// the threshold or the cache is empty. excessBytes is the amount over threshold.
func (c *StagingFileCache) evictUntilBelowThreshold(excessBytes int64) {
	cachedBytes := c.globalCachedBytes.Load()
	if cachedBytes <= excessBytes {
		// Even clearing entire cache won't cover the excess, clear it all
		c.Clear()
		return
	}

	// Add 5% of total storage as buffer to avoid thrashing
	var buffer int64
	var stat unix.Statfs_t
	if err := unix.Statfs(StagingDir, &stat); err == nil {
		buffer = int64(stat.Blocks*uint64(stat.Bsize)) / 20
	}
	bytesToFree := excessBytes + buffer
	var freedBytes int64

	c.mu.Lock()
	defer c.mu.Unlock()

	for freedBytes < bytesToFree && len(c.entries) > 0 {
		oldest := c.lruList.Back()
		if oldest == nil {
			return
		}
		entry := oldest.Value.(*CacheEntry)
		freedBytes += entry.size
		c.removeEntry(entry.hdfsPath)
	}
}

// diskUsageExcess returns how many bytes over the threshold the disk usage is.
// Returns 0 if usage is below threshold.
func diskUsageExcess(path string, threshold float64) (excessBytes int64, err error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return 0, err
	}
	totalBytes := stat.Blocks * uint64(stat.Bsize)
	if totalBytes == 0 {
		return 0, nil
	}
	availableBytes := stat.Bavail * uint64(stat.Bsize)
	usedBytes := totalBytes - availableBytes
	targetUsedBytes := uint64(float64(totalBytes) * threshold)

	if usedBytes > targetUsedBytes {
		return int64(usedBytes - targetUsedBytes), nil
	}
	return 0, nil
}

// GetOrLoad tries to get a file from cache, or downloads it to cache if not found.
// Returns an *os.File handle if successful (either from cache or freshly downloaded), or (nil, nil) if caching is not possible.
// Returns (nil, error) if staging file creation fails.
func (c *StagingFileCache) GetOrLoad(file *FileINode, hdfsAccessor HdfsAccessor, operation string) (*os.File, error) {
	absPath := file.AbsolutePath()

	// Check if file exists in cache before making expensive Stat call
	if c.Contains(absPath) {
		upstreamInfo, err := hdfsAccessor.Stat(absPath)
		if err != nil {
			logger.Warn("Failed to stat file for cache validation, skipping cache", logger.Fields{
				Operation: cache,
				Path:      absPath,
				Error:     err,
			})
			return nil, nil
		}

		// Update file.Attrs with upstream metadata so closeStaging can use correct mtime for caching
		file.Attrs.Size = upstreamInfo.Size
		file.Attrs.Mtime = upstreamInfo.Mtime

		if cachedFile, ok := c.Get(absPath, int64(upstreamInfo.Size), upstreamInfo.Mtime); ok {
			return cachedFile, nil
		}
	} else {
		if c.globalStatsEnabled {
			c.globalMisses.Add(1)
		}
		logger.Debug("Cache miss for staging file", logger.Fields{
			Operation: cache,
			Path:      absPath,
		})
	}

	// Not in cache or stale - check if we should download
	if !c.ShouldCache(int64(file.Attrs.Size), absPath, true) {
		return nil, nil
	}

	// Download to staging file
	stagingFile, err := file.newStagingFile(operation, true)
	return stagingFile, err
}
