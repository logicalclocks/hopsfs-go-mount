// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package hopsfsmount

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func setDiskUsageThreshold(t *testing.T, threshold float64) {
	t.Helper()
	original := StagingCacheMaxDiskUsage
	StagingCacheMaxDiskUsage = threshold
	t.Cleanup(func() {
		StagingCacheMaxDiskUsage = original
	})
}

func newTestCache(t *testing.T, maxEntries int) *StagingFileCache {
	t.Helper()
	cache := NewStagingFileCache(maxEntries)
	t.Cleanup(func() {
		cache.stopBackgroundWorkers()
	})
	return cache
}

// TestStagingFileCachePutAndGet tests basic put and get operations
func TestStagingFileCachePutAndGet(t *testing.T) {
	cache := newTestCache(t, 10)

	// Create a temporary file to cache
	tmpFile, err := os.CreateTemp("", "stagingFileCache_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFilePath := tmpFile.Name()
	defer func() { _ = os.Remove(tmpFilePath) }()

	testData := "test data content"
	_, _ = tmpFile.WriteString(testData)

	hdfsPath := "/test/file.txt"
	fileInfo, _ := tmpFile.Stat()
	mtime := time.Now()

	// Put the file in cache (takes ownership of handle)
	cache.Put(hdfsPath, tmpFile, fileInfo.Size(), mtime)
	assert.Equal(t, 1, cache.Size())

	// Get with matching metadata should succeed and return the handle
	// Note: Get removes the entry from cache
	file, ok := cache.Get(hdfsPath, fileInfo.Size(), mtime)
	assert.True(t, ok)
	assert.NotNil(t, file)
	assert.Equal(t, 0, cache.Size(), "Get should remove entry from cache")

	// Verify the file content matches what we wrote (Get seeks to beginning)
	content := make([]byte, len(testData))
	n, err := file.Read(content)
	assert.NoError(t, err)
	assert.Equal(t, len(testData), n)
	assert.Equal(t, testData, string(content))
	_ = file.Close()

	// Get with non-existent path should fail
	_, ok = cache.Get("/nonexistent/path", 100, mtime)
	assert.False(t, ok)
}

// TestStagingFileCacheStaleEntry tests that stale entries are invalidated
func TestStagingFileCacheStaleEntry(t *testing.T) {
	cache := newTestCache(t, 10)

	hdfsPath := "/test/stale.txt"
	cachedSize := int64(16) // "original content"
	cachedMtime := time.Now()

	// Create first file and put in cache
	tmpFile1, err := os.CreateTemp("", "stagingFileCache_stale_*")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	_, _ = tmpFile1.WriteString("original content")
	os.Remove(tmpFile1.Name())

	cache.Put(hdfsPath, tmpFile1, cachedSize, cachedMtime)
	assert.Equal(t, 1, cache.Size())

	// Get with different size should fail (stale) and remove entry
	_, ok := cache.Get(hdfsPath, cachedSize+100, cachedMtime)
	assert.False(t, ok)
	assert.Equal(t, 0, cache.Size())

	// Create second file and re-add for mtime test
	tmpFile2, err := os.CreateTemp("", "stagingFileCache_stale2_*")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	os.Remove(tmpFile2.Name())
	_, _ = tmpFile2.WriteString("original content")

	cache.Put(hdfsPath, tmpFile2, cachedSize, cachedMtime)
	assert.Equal(t, 1, cache.Size())

	// Get with different mtime should fail (stale) and remove entry
	differentMtime := cachedMtime.Add(time.Hour)
	_, ok = cache.Get(hdfsPath, cachedSize, differentMtime)
	assert.False(t, ok)
	assert.Equal(t, 0, cache.Size())
}

// TestStagingFileCacheLRUEviction tests that LRU eviction works correctly
func TestStagingFileCacheLRUEviction(t *testing.T) {
	maxEntries := 3
	cache := newTestCache(t, maxEntries)

	mtime := time.Now()

	// Helper to create and add a file to cache
	createAndCache := func(hdfsPath string, content string) {
		tmpFile, err := os.CreateTemp("", "stagingFileCache_lru_*")
		if err != nil {
			t.Fatalf("Failed to create temp file: %v", err)
		}
		os.Remove(tmpFile.Name())
		_, _ = tmpFile.WriteString(content)
		fileInfo, _ := tmpFile.Stat()
		cache.Put(hdfsPath, tmpFile, fileInfo.Size(), mtime)
	}

	// Add entries up to capacity
	for i := 0; i < maxEntries; i++ {
		hdfsPath := fmt.Sprintf("/test/file%d.txt", i)
		createAndCache(hdfsPath, fmt.Sprintf("content %d", i))
	}
	assert.Equal(t, maxEntries, cache.Size())

	// Add one more entry - should evict the oldest (file0)
	createAndCache("/test/file_new.txt", "new content")
	assert.Equal(t, maxEntries, cache.Size())

	// file0 should be evicted (was the oldest)
	_, ok := cache.Get("/test/file0.txt", int64(9), mtime)
	assert.False(t, ok)

	// file1 should still be present (Get removes it from cache)
	file, ok := cache.Get("/test/file1.txt", int64(9), mtime)
	assert.True(t, ok)
	if file != nil {
		_ = file.Close()
	}
	assert.Equal(t, 2, cache.Size(), "Get should remove entry from cache")
}

// TestStagingFileCacheRemove tests explicit removal of entries
func TestStagingFileCacheRemove(t *testing.T) {
	cache := newTestCache(t, 10)

	// Create a temp file
	tmpFile, err := os.CreateTemp("", "stagingFileCache_remove_*")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFilePath := tmpFile.Name()
	_, _ = tmpFile.WriteString("test content")

	hdfsPath := "/test/remove.txt"
	mtime := time.Now()

	// Put takes ownership of the handle
	cache.Put(hdfsPath, tmpFile, 12, mtime)
	assert.Equal(t, 1, cache.Size())

	// Remove the entry (closes handle and deletes file)
	cache.Remove(hdfsPath)
	assert.Equal(t, 0, cache.Size())

	// File should be deleted after Remove
	_, err = os.Stat(tmpFilePath)
	assert.True(t, os.IsNotExist(err))

	// Removing non-existent entry should be safe
	cache.Remove("/nonexistent/path")
}

// TestStagingFileCacheRename tests renaming cache entries
func TestStagingFileCacheRename(t *testing.T) {
	cache := newTestCache(t, 10)

	// Create a temp file
	tmpFile, err := os.CreateTemp("", "stagingFileCache_rename_*")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	_, _ = tmpFile.WriteString("rename test")

	oldPath := "/test/old.txt"
	newPath := "/test/new.txt"
	mtime := time.Now()

	cache.Put(oldPath, tmpFile, 11, mtime)
	assert.Equal(t, 1, cache.Size())

	// Rename the entry
	cache.Rename(oldPath, newPath)
	assert.Equal(t, 1, cache.Size())

	// Old path should not exist in cache (but shouldn't remove the entry since it was renamed)
	_, ok := cache.Get(oldPath, 11, mtime)
	assert.False(t, ok)
	// Cache size should still be 1 (entry was renamed, not removed)
	assert.Equal(t, 1, cache.Size())

	// New path should work
	file, ok := cache.Get(newPath, 11, mtime)
	assert.True(t, ok)
	if file != nil {
		_ = file.Close()
	}
}

// TestStagingFileCacheRenameWithExistingTarget tests rename when target already exists
func TestStagingFileCacheRenameWithExistingTarget(t *testing.T) {
	cache := newTestCache(t, 10)

	// Create temp files
	tmpFile1, err := os.CreateTemp("", "stagingFileCache_rename1_*")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	os.Remove(tmpFile1.Name())
	_, _ = tmpFile1.WriteString("source content")

	tmpFile2, err := os.CreateTemp("", "stagingFileCache_rename2_*")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	os.Remove(tmpFile2.Name())
	_, _ = tmpFile2.WriteString("target content")

	sourcePath := "/test/source.txt"
	targetPath := "/test/target.txt"
	mtime := time.Now()

	// Add both entries (cache takes ownership of handles)
	cache.Put(sourcePath, tmpFile1, 14, mtime)
	cache.Put(targetPath, tmpFile2, 14, mtime)
	assert.Equal(t, 2, cache.Size())

	// Rename source to target - should replace target (closes old target handle)
	cache.Rename(sourcePath, targetPath)
	assert.Equal(t, 1, cache.Size())

	// Target path should point to source's file
	file, ok := cache.Get(targetPath, 14, mtime)
	assert.True(t, ok)
	if file != nil {
		_ = file.Close()
	}
}

// TestStagingFileCacheRenameNonExistent tests rename when source doesn't exist
func TestStagingFileCacheRenameNonExistent(t *testing.T) {
	cache := newTestCache(t, 10)

	// Renaming non-existent entry should be safe (no-op)
	cache.Rename("/nonexistent/source", "/nonexistent/target")
	assert.Equal(t, 0, cache.Size())
}

// TestStagingFileCacheClear tests clearing all entries
func TestStagingFileCacheClear(t *testing.T) {
	cache := newTestCache(t, 10)

	// Create temp files and add to cache
	tmpFilePaths := make([]string, 5)
	for i := 0; i < 5; i++ {
		tmpFile, err := os.CreateTemp("", fmt.Sprintf("stagingFileCache_clear_%d_*", i))
		if err != nil {
			t.Fatalf("Failed to create temp file: %v", err)
		}
		_, _ = tmpFile.WriteString(fmt.Sprintf("content %d", i))
		tmpFilePaths[i] = tmpFile.Name()

		hdfsPath := fmt.Sprintf("/test/file%d.txt", i)
		cache.Put(hdfsPath, tmpFile, 9, time.Now())
	}

	assert.Equal(t, 5, cache.Size())

	// Clear cache (closes all handles and deletes files)
	cache.Clear()
	assert.Equal(t, 0, cache.Size())

	// Files should be deleted after Clear
	for _, path := range tmpFilePaths {
		_, err := os.Stat(path)
		assert.True(t, os.IsNotExist(err), "File should be deleted after Clear")
	}
}

// TestStagingFileCacheUpdateExisting tests updating an existing entry
func TestStagingFileCacheUpdateExisting(t *testing.T) {
	cache := newTestCache(t, 10)

	hdfsPath := "/test/update.txt"
	mtime1 := time.Now()
	mtime2 := mtime1.Add(time.Hour)

	// Create first file and add to cache
	tmpFile1, err := os.CreateTemp("", "stagingFileCache_update1_*")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	os.Remove(tmpFile1.Name())
	_, _ = tmpFile1.WriteString("original")

	cache.Put(hdfsPath, tmpFile1, 8, mtime1)
	assert.Equal(t, 1, cache.Size())

	// Create second file and update
	tmpFile2, err := os.CreateTemp("", "stagingFileCache_update2_*")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	os.Remove(tmpFile2.Name())
	_, _ = tmpFile2.WriteString("updated")

	cache.Put(hdfsPath, tmpFile2, 7, mtime2)
	assert.Equal(t, 1, cache.Size()) // Size should still be 1

	// Should retrieve the updated version
	file, ok := cache.Get(hdfsPath, 7, mtime2)
	assert.True(t, ok)
	if file != nil {
		content := make([]byte, 7)
		_, _ = file.Read(content)
		assert.Equal(t, "updated", string(content))
		_ = file.Close()
	}

	// Old metadata should not match (and should remove the entry since it's stale)
	_, ok = cache.Get(hdfsPath, 8, mtime1)
	assert.False(t, ok)
}

// TestStagingFileCacheWithHopsFS tests the cache integration with actual HopsFS operations
func TestStagingFileCacheWithHopsFS(t *testing.T) {
	// Save original cache and restore after test
	originalCache := StagingCache
	defer func() {
		if StagingCache != nil {
			StagingCache.Clear()
		}
		StagingCache = originalCache
	}()

	// Enable local cache for this test
	StagingCache = newTestCache(t, 5)

	withMount(t, "/", DelaySyncUntilClose, func(mountPoint string, hdfsAccessor HdfsAccessor) {
		testFile := filepath.Join(mountPoint, "cache_test_file.txt")
		_ = os.Remove(testFile) // Clean up from previous runs

		testData := "Hello, this is test data for cache testing!"

		// Create a file - it should be cached when closed
		if err := createFile(testFile, testData); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}
		defer func() {
			_ = os.Remove(testFile)
		}()

		// Wait for Release to populate the cache (Release is async in FUSE).
		time.Sleep(100 * time.Millisecond)

		// File should be in cache after creation and close
		assert.Equal(t, 1, StagingCache.Size(), "File should be cached after creation")

		// Read the file
		content, err := os.ReadFile(testFile)
		if err != nil {
			t.Fatalf("Failed to read test file: %v", err)
		}
		assert.Equal(t, testData, string(content))

		// Modify the file with new content
		modifiedData := "Modified content!"
		if err := createFile(testFile, modifiedData); err != nil {
			t.Fatalf("Failed to modify test file: %v", err)
		}

		// Verify cache has the modified file
		hdfsPath := "/cache_test_file.txt"

		// Wait for Release to populate the cache (Release is async in FUSE).
		time.Sleep(100 * time.Millisecond)

		assert.True(t, StagingCache.Contains(hdfsPath), "Modified file should be in cache")
		assert.Equal(t, 1, StagingCache.Size(), "Cache should have 1 entry after modification")

		// Read again - should get the new content
		content, err = os.ReadFile(testFile)
		if err != nil {
			t.Fatalf("Failed to read modified file: %v", err)
		}
		assert.Equal(t, modifiedData, string(content))
	})
}

// TestStagingFileCacheMaxFileSizeLimit ensures oversized files are not cached.
func TestStagingFileCacheMaxFileSizeLimit(t *testing.T) {
	originalCache := StagingCache
	originalMaxFileSize := StagingCacheMaxFileSize
	defer func() {
		if StagingCache != nil {
			StagingCache.Clear()
		}
		StagingCache = originalCache
		StagingCacheMaxFileSize = originalMaxFileSize
	}()

	StagingCacheMaxFileSize = 10
	StagingCache = newTestCache(t, 10)

	withMount(t, "/", DelaySyncUntilClose, func(mountPoint string, hdfsAccessor HdfsAccessor) {
		small1 := filepath.Join(mountPoint, "small_cache_1.txt")
		small2 := filepath.Join(mountPoint, "small_cache_2.txt")
		big1 := filepath.Join(mountPoint, "big_cache_1.txt")
		big2 := filepath.Join(mountPoint, "big_cache_2.txt")
		_ = os.Remove(small1)
		_ = os.Remove(small2)
		_ = os.Remove(big1)
		_ = os.Remove(big2)

		defer func() {
			_ = os.Remove(small1)
			_ = os.Remove(small2)
			_ = os.Remove(big1)
			_ = os.Remove(big2)
		}()

		smallData := "12345"
		bigData := strings.Repeat("a", int(StagingCacheMaxFileSize)+5)

		if err := createFile(small1, smallData); err != nil {
			t.Fatalf("Failed to create small1: %v", err)
		}
		if err := createFile(big1, bigData); err != nil {
			t.Fatalf("Failed to create big1: %v", err)
		}
		if err := createFile(small2, smallData); err != nil {
			t.Fatalf("Failed to create small2: %v", err)
		}
		if err := createFile(big2, bigData); err != nil {
			t.Fatalf("Failed to create big2: %v", err)
		}

		waitForCacheSize := func(expected int) {
			deadline := time.Now().Add(1 * time.Second)
			for time.Now().Before(deadline) && StagingCache.Size() != expected {
				time.Sleep(10 * time.Millisecond)
			}
		}

		waitForCacheSize(2)
		assert.Equal(t, 2, StagingCache.Size(), "Only small files should be cached")

		// Grow a cached file beyond the limit and ensure it is removed from cache.
		if err := createFile(small1, bigData); err != nil {
			t.Fatalf("Failed to grow small1: %v", err)
		}
		if err := createFile(big1, bigData+"more"); err != nil {
			t.Fatalf("Failed to modify big1: %v", err)
		}

		waitForCacheSize(1)
		assert.Equal(t, 1, StagingCache.Size(), "Only one small file should remain cached")

		checkCached := func(localPath, hdfsPath string, expect bool) {
			info, err := os.Stat(localPath)
			if err != nil {
				t.Fatalf("Failed to stat %s: %v", localPath, err)
			}
			cachedFile, cacheHit := StagingCache.Get(hdfsPath, info.Size(), info.ModTime())
			if cachedFile != nil {
				_ = cachedFile.Close()
			}
			assert.Equal(t, expect, cacheHit, "Unexpected cache state for %s", hdfsPath)
		}

		checkCached(small1, "/small_cache_1.txt", false)
		checkCached(small2, "/small_cache_2.txt", true)
		checkCached(big1, "/big_cache_1.txt", false)
		checkCached(big2, "/big_cache_2.txt", false)
	})
}

// TestStagingFileCacheDiskUsageThreshold verifies caching is blocked when disk usage exceeds the threshold.
func TestStagingFileCacheDiskUsageThreshold(t *testing.T) {
	setDiskUsageThreshold(t, 1.0)
	originalStagingDir := StagingDir
	defer func() {
		StagingDir = originalStagingDir
	}()

	StagingDir = t.TempDir()
	cache := newTestCache(t, 2)
	cache.stopBackgroundWorkers()

	StagingCacheMaxDiskUsage = 1.0
	cache.updateDiskUsageFlag()
	tmpFile1, err := os.CreateTemp(StagingDir, "stagingFileCache_disk_1_*")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	_, _ = tmpFile1.WriteString("one")
	info1, _ := tmpFile1.Stat()
	cache.Put("/disk/one.txt", tmpFile1, info1.Size(), time.Now())
	assert.Equal(t, 1, cache.Size())

	StagingCacheMaxDiskUsage = 0.0
	cache.updateDiskUsageFlag()
	tmpFile2, err := os.CreateTemp(StagingDir, "stagingFileCache_disk_2_*")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	_, _ = tmpFile2.WriteString("two")
	info2, _ := tmpFile2.Stat()
	cache.Put("/disk/two.txt", tmpFile2, info2.Size(), time.Now())
	assert.Equal(t, 0, cache.Size())

	_, err = os.Stat(tmpFile1.Name())
	assert.True(t, os.IsNotExist(err))
	_, err = os.Stat(tmpFile2.Name())
	assert.True(t, os.IsNotExist(err))

	StagingCacheMaxDiskUsage = 1.0
	cache.updateDiskUsageFlag()
	tmpFile3, err := os.CreateTemp(StagingDir, "stagingFileCache_disk_3_*")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	_, _ = tmpFile3.WriteString("three")
	info3, _ := tmpFile3.Stat()
	cache.Put("/disk/three.txt", tmpFile3, info3.Size(), time.Now())
	assert.Equal(t, 1, cache.Size())
	cache.Clear()
}

// TestStagingFileCacheMultipleChmodOperations tests multiple chmod operations
// with cached file reads/writes interleaved, verifying cache hits after chmod
func TestStagingFileCacheMultipleChmodOperations(t *testing.T) {
	// Save original cache and restore after test
	originalCache := StagingCache
	defer func() {
		if StagingCache != nil {
			StagingCache.Clear()
		}
		StagingCache = originalCache
	}()

	// Enable local cache for this test
	StagingCache = newTestCache(t, 10)

	withMount(t, "/", DelaySyncUntilClose, func(mountPoint string, hdfsAccessor HdfsAccessor) {
		testFile := filepath.Join(mountPoint, "multi_chmod_cache_test.txt")
		hdfsPath := "/multi_chmod_cache_test.txt"
		_ = os.Remove(testFile) // Clean up from previous runs

		testData := "Initial content"

		// Create file
		if err := createFile(testFile, testData); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}
		defer func() {
			_ = os.Remove(testFile)
		}()

		// Wait for Release to populate the cache (Release is async in FUSE)
		time.Sleep(100 * time.Millisecond)

		// Verify file is cached after creation
		assert.Equal(t, 1, StagingCache.Size(), "File should be cached after creation")

		permissions := []os.FileMode{0644, 0755, 0600, 0444, 0644}

		for i, perm := range permissions {
			// Change permissions
			err := os.Chmod(testFile, perm)
			if err != nil {
				t.Fatalf("Failed to chmod file to %o: %v", perm, err)
			}

			// Verify permissions
			fileInfo, err := os.Stat(testFile)
			if err != nil {
				t.Fatalf("Failed to stat file after chmod to %o: %v", perm, err)
			}
			assert.Equal(t, perm, fileInfo.Mode().Perm(), "Permission mismatch at iteration %d", i)

			assert.True(t, StagingCache.Contains(hdfsPath), "Cache should still have entry after chmod to %o at iteration %d", perm, i)

			// Read the file
			content, err := os.ReadFile(testFile)
			if err != nil {
				t.Fatalf("Failed to read file with permission %o: %v", perm, err)
			}
			assert.Equal(t, testData, string(content), "Content mismatch at iteration %d", i)

			// Write if we have write permission (not 0444)
			if perm != 0444 {
				newData := fmt.Sprintf("Content after chmod %o", perm)
				file, err := os.OpenFile(testFile, os.O_WRONLY|os.O_TRUNC, perm)
				if err != nil {
					t.Fatalf("Failed to open file for writing with permission %o: %v", perm, err)
				}
				_, err = file.WriteString(newData)
				if err != nil {
					_ = file.Close()
					t.Fatalf("Failed to write to file with permission %o: %v", perm, err)
				}
				err = file.Close()
				if err != nil {
					t.Fatalf("Failed to close file: %v", err)
				}

				// Wait for Release to update the cache
				time.Sleep(100 * time.Millisecond)

				testData = newData // Update expected content for next iteration
			}
		}

		// Verify cache still has 1 entry at the end
		assert.Equal(t, 1, StagingCache.Size(), "Cache should have 1 entry after all operations")
	})

}
