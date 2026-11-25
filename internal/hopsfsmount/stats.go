// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package hopsfsmount

import (
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"hopsworks.ai/hopsfsmount/internal/hopsfsmount/logger"
)

// Backend calls made for directory listings and for entry attributes.
var readDirCalls, statCalls atomic.Int64

// StartStatsReporter logs the mount's node and cache counters every interval
// until stop is closed. It logs at warn level so the line shows at the level
// sidecars run with; the reporter is off unless an interval is configured.
func (filesystem *FileSystem) StartStatsReporter(interval time.Duration, stop <-chan struct{}) {
	if interval <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				logger.Warn(filesystem.statsLine(), nil)
			}
		}
	}()
}

func (filesystem *FileSystem) statsLine() string {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	listingBytes, listingEntries := filesystem.listings.usage()
	return fmt.Sprintf("Mount stats: registeredNodes=%d refusedRegistrations=%d invalidatedEntries=%d listingBytes=%d listingEntries=%d negativeEntries=%d readDirCalls=%d statCalls=%d heapInuse=%d goroutines=%d",
		registeredNodes.Load(), refusedRegistrations.Load(), invalidatedEntries.Load(), listingBytes, listingEntries, filesystem.negativeEntries.Load(),
		readDirCalls.Load(), statCalls.Load(), mem.HeapInuse, runtime.NumGoroutine())
}

// cgroupMemoryLimitFiles are where a container sees its own memory limit:
// cgroup v2, then v1.
var cgroupMemoryLimitFiles = []string{
	"/sys/fs/cgroup/memory.max",
	"/sys/fs/cgroup/memory/memory.limit_in_bytes",
}

// SetMemoryLimitFromCgroup gives the Go runtime a soft memory limit of 85% of
// the container's memory limit, so the garbage collector works harder as the
// mount nears the point where the kernel would kill it. A GOMEMLIMIT in the
// environment wins, and with no readable finite limit nothing is set.
func SetMemoryLimitFromCgroup() {
	if os.Getenv("GOMEMLIMIT") != "" {
		logger.Info("Memory limit taken from GOMEMLIMIT", nil)
		return
	}
	for _, file := range cgroupMemoryLimitFiles {
		limit, ok := readCgroupMemoryLimit(file)
		if !ok {
			continue
		}
		soft := limit / 100 * 85
		debug.SetMemoryLimit(soft)
		logger.Info(fmt.Sprintf("Go memory limit set to %d bytes, 85%% of the %d in %s", soft, limit, file), nil)
		return
	}
	logger.Info("No container memory limit found, Go memory limit not set", nil)
}

// readCgroupMemoryLimit returns the limit in file, and false when the file
// is missing, unparsable or holds no real limit: "max" on cgroup v2, a value
// near the top of int64 on v1.
func readCgroupMemoryLimit(file string) (int64, bool) {
	data, err := os.ReadFile(file)
	if err != nil {
		return 0, false
	}
	limit, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil || limit <= 0 || limit >= 1<<60 {
		return 0, false
	}
	return limit, true
}
