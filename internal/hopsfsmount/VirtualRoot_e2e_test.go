// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

// End-to-end tests for the virtual-directory feature. These tests require a
// running HopsFS cluster reachable at testNameNodeAddress(). They create real
// backend state before mounting and clean it up via t.Cleanup. No mocks.
//
// Coverage areas:
//   - basic visibility and allowlist semantics
//   - real-entry collision precedence
//   - mutation rejection on synthetic nodes
//   - operations inside a real leaf
//   - rename edge cases at the virtual boundary
//   - multi-level path structure

package hopsfsmount

import (
	"errors"
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"bazil.org/fuse/fs/fstestutil"
	"hopsworks.ai/hopsfsmount/internal/hopsfsmount/logger"
)

// ============================================================================
// Helpers — backend setup, mount, assertions
// ============================================================================

// connectBackend opens a direct HdfsAccessor to the test cluster. Used for
// backend setup and cleanup performed OUTSIDE the FUSE mount.
func connectBackend(t testing.TB) HdfsAccessor {
	t.Helper()
	logLevel := strings.TrimSpace(os.Getenv("HOPSFS_TEST_LOG_LEVEL"))
	if logLevel == "" {
		logLevel = "ERROR"
	}
	logger.InitLogger(logLevel, false, "")
	if err := InitConnectionUser(); err != nil {
		t.Fatalf("InitConnectionUser: %v", err)
	}
	tlsEnabled, err := testTLSEnabled()
	if err != nil {
		t.Fatalf("testTLSEnabled: %v", err)
	}
	accessor, _ := NewHdfsAccessor(testNameNodeAddress(), WallClock{}, TLSConfig{
		TLS:               tlsEnabled,
		RootCABundle:      RootCABundle,
		ClientCertificate: ClientCertificate,
		ClientKey:         ClientKey,
	})
	if err := accessor.EnsureConnected(); err != nil {
		t.Fatalf("backend connection failed (is HopsFS at %s reachable?): %v", testNameNodeAddress(), err)
	}
	return accessor
}

// uniqueBackendBase returns a unique HopsFS path for test isolation. Each test
// gets its own subtree so concurrent runs and prior failures cannot interfere.
func uniqueBackendBase(prefix string) string {
	return fmt.Sprintf("/tmp_e2e_vd/%s_%d_%d", prefix, time.Now().UnixNano(), rand.Int63())
}

// ensureBackendDir creates fullPath in the backend, building parent directories
// as needed (mkdir -p). If cleanupRoot is non-empty, registers a t.Cleanup that
// recursively removes cleanupRoot at end of test. Pass cleanupRoot only on the
// FIRST ensureBackendDir of a test (typically the test's unique base); subsequent
// calls within the same subtree pass "" to avoid duplicate cleanup hooks.
func ensureBackendDir(t testing.TB, accessor HdfsAccessor, fullPath string, cleanupRoot string) {
	t.Helper()
	// Ensure /tmp_e2e_vd exists (best-effort, ignore error).
	_ = accessor.Mkdir("/tmp_e2e_vd", 0755)

	parts := strings.Split(strings.TrimPrefix(fullPath, "/"), "/")
	current := ""
	for _, part := range parts {
		if part == "" {
			continue
		}
		current = current + "/" + part
		if _, err := accessor.Stat(current); err != nil {
			if err := accessor.Mkdir(current, 0755); err != nil {
				t.Fatalf("Failed to create backend dir %s: %v", current, err)
			}
		}
	}
	if cleanupRoot != "" {
		t.Cleanup(func() {
			removeBackendRecursive(accessor, cleanupRoot)
		})
	}
}

// removeBackendRecursive removes a backend subtree. Best-effort; ignores errors
// because cleanup runs after the test has already produced its verdict.
func removeBackendRecursive(accessor HdfsAccessor, path string) {
	entries, err := accessor.ReadDir(path)
	if err == nil {
		for _, a := range entries {
			child := path + "/" + a.Name
			if a.Mode&os.ModeDir != 0 {
				removeBackendRecursive(accessor, child)
			} else {
				_ = accessor.Remove(child)
			}
		}
	}
	_ = accessor.Remove(path)
}

// withVirtualMount mounts the filesystem with the given srcDir and virtual
// directory configurations, then invokes fn with the mount point and the raw
// HdfsAccessor. Equivalent to withMount but passes WithVirtualDirectories.
func withVirtualMount(t testing.TB, srcDir string, virtualConfigs []VirtualDirectoryConfig, fn func(mountPoint string, accessor HdfsAccessor)) {
	t.Helper()

	if StagingCache == nil {
		StagingCacheMaxDiskUsage = 0.9
		StagingCache = NewStagingFileCache(10240)
		t.Cleanup(func() {
			if StagingCache != nil {
				StagingCache.Clear()
				StagingCache = nil
			}
		})
	}

	retryPolicy := NewDefaultRetryPolicy(WallClock{})
	retryPolicy.MaxAttempts = 1

	logLevel := strings.TrimSpace(os.Getenv("HOPSFS_TEST_LOG_LEVEL"))
	if logLevel == "" {
		logLevel = "ERROR"
	}
	logger.InitLogger(logLevel, false, "")
	if err := InitConnectionUser(); err != nil {
		t.Fatalf("InitConnectionUser: %v", err)
	}
	tlsEnabled, err := testTLSEnabled()
	if err != nil {
		t.Fatalf("testTLSEnabled: %v", err)
	}

	hdfsAccessor, _ := NewHdfsAccessor(testNameNodeAddress(), WallClock{}, TLSConfig{
		TLS:               tlsEnabled,
		RootCABundle:      RootCABundle,
		ClientCertificate: ClientCertificate,
		ClientKey:         ClientKey,
	})
	if err := hdfsAccessor.EnsureConnected(); err != nil {
		t.Fatalf("backend connection failed: %v", err)
	}

	ftHdfsAccessor := NewFaultTolerantHdfsAccessor(hdfsAccessor, retryPolicy)

	var opts []FileSystemOption
	if len(virtualConfigs) > 0 {
		opts = append(opts, WithVirtualDirectories(virtualConfigs))
	}

	fileSystem, err := NewFileSystem(
		[]HdfsAccessor{ftHdfsAccessor},
		srcDir,
		[]string{"*"},
		false,
		DelaySyncUntilClose,
		retryPolicy,
		WallClock{},
		opts...,
	)
	if err != nil {
		t.Fatalf("NewFileSystem: %v", err)
	}

	mountOptions := GetMountOptions(false)
	mnt, err := fstestutil.MountedT(t, fileSystem, nil, mountOptions...)
	if err != nil {
		t.Fatalf("MountedT: %v", err)
	}
	defer mnt.Close()
	disablePolling(mnt.Dir)
	fn(mnt.Dir, hdfsAccessor)
}

func direntNamesFromFileInfo(entries []fs.FileInfo) []string {
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

func containsName(names []string, target string) bool {
	for _, n := range names {
		if n == target {
			return true
		}
	}
	return false
}

func assertSyscallErr(t *testing.T, err error, want syscall.Errno) {
	t.Helper()
	if err == nil {
		t.Errorf("expected error %v, got nil", want)
		return
	}
	if !errors.Is(err, want) {
		t.Errorf("expected error %v, got: %v", want, err)
	}
}

// ============================================================================
// Tests — virtual directory feature (e2e)
// ============================================================================

// TestE2EVirtualRoot_AppearsAtMountRoot verifies the configured virtual
// directory name surfaces at the mount root as a navigable directory.
func TestE2EVirtualRoot_AppearsAtMountRoot(t *testing.T) {
	backend := connectBackend(t)
	base := uniqueBackendBase("appears")
	ensureBackendDir(t, backend, base+"/projA/dsA", base)

	configs := []VirtualDirectoryConfig{{
		Name:        "shared-datasets",
		Paths:       []string{"projA/dsA"},
		BackendRoot: base,
	}}

	withVirtualMount(t, "/", configs, func(mountPoint string, _ HdfsAccessor) {
		names := direntNamesFromFileInfo(listDir(t, mountPoint))
		if !containsName(names, "shared-datasets") {
			t.Fatalf("Expected 'shared-datasets' at mount root, got: %v", names)
		}
		info, err := os.Stat(filepath.Join(mountPoint, "shared-datasets"))
		if err != nil {
			t.Fatalf("Stat virtual root: %v", err)
		}
		if !info.IsDir() {
			t.Error("Expected virtual root to be a directory")
		}
	})
}

// TestE2EVirtualRoot_UnconfiguredPathsHidden verifies allowlist semantics:
// backend entries that exist under BackendRoot but are NOT listed in Paths
// must be invisible through the virtual layer.
func TestE2EVirtualRoot_UnconfiguredPathsHidden(t *testing.T) {
	backend := connectBackend(t)
	base := uniqueBackendBase("hidden")
	ensureBackendDir(t, backend, base+"/projA/dsA", base)
	ensureBackendDir(t, backend, base+"/projB/dsX", "")

	configs := []VirtualDirectoryConfig{{
		Name:        "shared-datasets",
		Paths:       []string{"projA/dsA"},
		BackendRoot: base,
	}}

	withVirtualMount(t, "/", configs, func(mountPoint string, _ HdfsAccessor) {
		virtualRoot := filepath.Join(mountPoint, "shared-datasets")
		names := direntNamesFromFileInfo(listDir(t, virtualRoot))
		if !containsName(names, "projA") {
			t.Errorf("Expected projA visible (it's a branch to a leaf), got: %v", names)
		}
		if containsName(names, "projB") {
			t.Errorf("projB must be invisible (not in Paths), got: %v", names)
		}
		if _, err := os.Stat(filepath.Join(virtualRoot, "projB")); err == nil {
			t.Error("Stat of unconfigured backend path should fail")
		}
	})
}

// TestE2EVirtualRoot_RealCollisionWins verifies that if a real backend entry
// exists at <srcDir>/<virtual-name>, it shadows the virtual entry.
func TestE2EVirtualRoot_RealCollisionWins(t *testing.T) {
	backend := connectBackend(t)
	base := uniqueBackendBase("collision")
	ensureBackendDir(t, backend, base+"/projA/dsA", base)
	// Pre-create a real entry at <srcDir>/shared-datasets, where srcDir == base.
	ensureBackendDir(t, backend, base+"/shared-datasets/real-content", "")

	configs := []VirtualDirectoryConfig{{
		Name:        "shared-datasets",
		Paths:       []string{"projA/dsA"},
		BackendRoot: base,
	}}

	withVirtualMount(t, base, configs, func(mountPoint string, _ HdfsAccessor) {
		entries := direntNamesFromFileInfo(listDir(t, filepath.Join(mountPoint, "shared-datasets")))
		if !containsName(entries, "real-content") {
			t.Errorf("Expected real-content (real wins on collision), got: %v", entries)
		}
		if containsName(entries, "projA") {
			t.Errorf("Virtual leaf must not appear when real collision wins, got: %v", entries)
		}
	})
}

// TestE2EVirtualRoot_MutationsOnSyntheticRejected verifies every mutating
// FUSE operation on a *VirtualDirINode returns EPERM and produces no backend
// side effect.
func TestE2EVirtualRoot_MutationsOnSyntheticRejected(t *testing.T) {
	backend := connectBackend(t)
	base := uniqueBackendBase("mut")
	ensureBackendDir(t, backend, base+"/projA/dsA", base)

	configs := []VirtualDirectoryConfig{{
		Name:        "shared-datasets",
		Paths:       []string{"projA/dsA"},
		BackendRoot: base,
	}}

	withVirtualMount(t, "/", configs, func(mountPoint string, _ HdfsAccessor) {
		virtualRoot := filepath.Join(mountPoint, "shared-datasets")
		virtualBranch := filepath.Join(virtualRoot, "projA")

		assertSyscallErr(t, os.Mkdir(filepath.Join(virtualRoot, "new"), 0755), syscall.EPERM)
		assertSyscallErr(t, os.Mkdir(filepath.Join(virtualBranch, "new"), 0755), syscall.EPERM)

		f, err := os.Create(filepath.Join(virtualRoot, "newfile"))
		if f != nil {
			f.Close()
		}
		assertSyscallErr(t, err, syscall.EPERM)

		assertSyscallErr(t, os.Remove(filepath.Join(virtualRoot, "projA")), syscall.EPERM)
		assertSyscallErr(t, os.Chmod(virtualRoot, 0700), syscall.EPERM)

		// Backend has no new entries — only the seeded projA subtree.
		entries, err := backend.ReadDir(base)
		if err != nil {
			t.Fatalf("ReadDir backend: %v", err)
		}
		names := make([]string, len(entries))
		for i, a := range entries {
			names[i] = a.Name
		}
		if len(names) != 1 || names[0] != "projA" {
			t.Errorf("Expected only 'projA' at base after rejected mutations, got: %v", names)
		}
	})
}

// TestE2EVirtualRoot_OperationsInsideLeafSucceed verifies that inside a
// configured leaf, the mount behaves like a normal HopsFS mount.
func TestE2EVirtualRoot_OperationsInsideLeafSucceed(t *testing.T) {
	backend := connectBackend(t)
	base := uniqueBackendBase("leaf")
	ensureBackendDir(t, backend, base+"/projA/dsA", base)

	configs := []VirtualDirectoryConfig{{
		Name:        "shared-datasets",
		Paths:       []string{"projA/dsA"},
		BackendRoot: base,
	}}

	withVirtualMount(t, "/", configs, func(mountPoint string, _ HdfsAccessor) {
		leafPath := filepath.Join(mountPoint, "shared-datasets", "projA", "dsA")

		if err := os.Mkdir(filepath.Join(leafPath, "subdir"), 0755); err != nil {
			t.Fatalf("Mkdir inside leaf failed: %v", err)
		}
		if err := createFile(filepath.Join(leafPath, "data.txt"), "hello"); err != nil {
			t.Fatalf("createFile inside leaf failed: %v", err)
		}

		names := direntNamesFromFileInfo(listDir(t, leafPath))
		if !containsName(names, "subdir") || !containsName(names, "data.txt") {
			t.Errorf("Expected subdir and data.txt inside leaf, got: %v", names)
		}

		backendAttrs, err := backend.ReadDir(base + "/projA/dsA")
		if err != nil {
			t.Fatalf("Backend ReadDir leaf: %v", err)
		}
		backendNames := make([]string, len(backendAttrs))
		for i, a := range backendAttrs {
			backendNames[i] = a.Name
		}
		if !containsName(backendNames, "subdir") || !containsName(backendNames, "data.txt") {
			t.Errorf("Backend should see leaf contents, got: %v", backendNames)
		}
	})
}

// TestE2EVirtualRoot_RenameOfVirtualRootNameRejected verifies the configured
// virtual-root name is reserved; rename of it returns EPERM.
func TestE2EVirtualRoot_RenameOfVirtualRootNameRejected(t *testing.T) {
	backend := connectBackend(t)
	base := uniqueBackendBase("renroot")
	ensureBackendDir(t, backend, base+"/projA/dsA", base)

	configs := []VirtualDirectoryConfig{{
		Name:        "shared-datasets",
		Paths:       []string{"projA/dsA"},
		BackendRoot: base,
	}}

	withVirtualMount(t, "/", configs, func(mountPoint string, _ HdfsAccessor) {
		src := filepath.Join(mountPoint, "shared-datasets")
		dst := filepath.Join(mountPoint, "renamed")
		assertSyscallErr(t, os.Rename(src, dst), syscall.EPERM)
	})
}

// TestE2EVirtualRoot_RenameIntoVirtualRootRejected verifies a rename whose
// destination parent is a synthetic directory returns EPERM (NOT panic).
func TestE2EVirtualRoot_RenameIntoVirtualRootRejected(t *testing.T) {
	backend := connectBackend(t)
	base := uniqueBackendBase("reninto")
	ensureBackendDir(t, backend, base+"/projA/dsA", base)
	ensureBackendDir(t, backend, base+"/realdir", "")

	configs := []VirtualDirectoryConfig{{
		Name:        "shared-datasets",
		Paths:       []string{"projA/dsA"},
		BackendRoot: base,
	}}

	withVirtualMount(t, base, configs, func(mountPoint string, _ HdfsAccessor) {
		src := filepath.Join(mountPoint, "realdir")
		dst := filepath.Join(mountPoint, "shared-datasets", "renamed-here")
		assertSyscallErr(t, os.Rename(src, dst), syscall.EPERM)
	})
}

// TestE2EVirtualRoot_MultiLevelPathStructure verifies a configured leaf with
// an intermediate path segment (projB/some_dir/dsC) produces a synthetic
// intermediate branch (some_dir) visible only because it leads to a leaf.
func TestE2EVirtualRoot_MultiLevelPathStructure(t *testing.T) {
	backend := connectBackend(t)
	base := uniqueBackendBase("multi")
	ensureBackendDir(t, backend, base+"/projA/dsA", base)
	ensureBackendDir(t, backend, base+"/projA/dsB", "")
	ensureBackendDir(t, backend, base+"/projB/some_dir/dsC", "")

	configs := []VirtualDirectoryConfig{{
		Name:        "shared-datasets",
		Paths:       []string{"projA/dsA", "projA/dsB", "projB/some_dir/dsC"},
		BackendRoot: base,
	}}

	withVirtualMount(t, "/", configs, func(mountPoint string, _ HdfsAccessor) {
		virtualRoot := filepath.Join(mountPoint, "shared-datasets")

		top := direntNamesFromFileInfo(listDir(t, virtualRoot))
		if !containsName(top, "projA") || !containsName(top, "projB") {
			t.Errorf("Expected projA and projB at virtual root, got: %v", top)
		}

		projA := direntNamesFromFileInfo(listDir(t, filepath.Join(virtualRoot, "projA")))
		if !containsName(projA, "dsA") || !containsName(projA, "dsB") {
			t.Errorf("Expected dsA and dsB under projA, got: %v", projA)
		}

		projB := direntNamesFromFileInfo(listDir(t, filepath.Join(virtualRoot, "projB")))
		if len(projB) != 1 || !containsName(projB, "some_dir") {
			t.Errorf("Expected only some_dir under projB (synthetic intermediate), got: %v", projB)
		}

		someDir := direntNamesFromFileInfo(listDir(t, filepath.Join(virtualRoot, "projB", "some_dir")))
		if len(someDir) != 1 || !containsName(someDir, "dsC") {
			t.Errorf("Expected only dsC under some_dir, got: %v", someDir)
		}

		info, err := os.Stat(filepath.Join(virtualRoot, "projB", "some_dir", "dsC"))
		if err != nil {
			t.Fatalf("Stat dsC: %v", err)
		}
		if !info.IsDir() {
			t.Error("dsC should be a real directory")
		}
	})
}

// TestE2EVirtualRoot_MultipleRootsBothVisible verifies mixed collisions:
// configure two virtual roots, only one of which collides with a real backend
// entry. ReadDir of the mount root must show the real entry for the collision
// and the synthetic entry for the non-collision, without duplicates.
func TestE2EVirtualRoot_MultipleRootsBothVisible(t *testing.T) {
	backend := connectBackend(t)
	base := uniqueBackendBase("mixed")
	ensureBackendDir(t, backend, base+"/projA/dsA", base)
	ensureBackendDir(t, backend, base+"/projB/dsB", "")
	// Real collision for 'shared-datasets' — but not for 'shared-models'.
	ensureBackendDir(t, backend, base+"/shared-datasets/real-content", "")

	configs := []VirtualDirectoryConfig{
		{
			Name:        "shared-datasets",
			Paths:       []string{"projA/dsA"},
			BackendRoot: base,
		},
		{
			Name:        "shared-models",
			Paths:       []string{"projB/dsB"},
			BackendRoot: base,
		},
	}

	withVirtualMount(t, base, configs, func(mountPoint string, _ HdfsAccessor) {
		top := direntNamesFromFileInfo(listDir(t, mountPoint))

		// Real shared-datasets must appear exactly once (collision precedence).
		dsCount := 0
		for _, n := range top {
			if n == "shared-datasets" {
				dsCount++
			}
		}
		if dsCount != 1 {
			t.Errorf("Expected 'shared-datasets' exactly once at mount root, found %d times in %v", dsCount, top)
		}
		if !containsName(top, "shared-models") {
			t.Errorf("Expected 'shared-models' (no collision) at mount root, got: %v", top)
		}

		// shared-datasets resolves to the REAL directory.
		dsEntries := direntNamesFromFileInfo(listDir(t, filepath.Join(mountPoint, "shared-datasets")))
		if !containsName(dsEntries, "real-content") {
			t.Errorf("shared-datasets should show real backend content, got: %v", dsEntries)
		}

		// shared-models resolves to the synthetic directory.
		modelEntries := direntNamesFromFileInfo(listDir(t, filepath.Join(mountPoint, "shared-models")))
		if !containsName(modelEntries, "projB") {
			t.Errorf("shared-models should show synthetic content (projB), got: %v", modelEntries)
		}
	})
}

// ============================================================================
// Rename matrix — all e2e through os.Rename
// ============================================================================

// TestE2EVirtualRoot_RenameWithinMountRootSanity verifies the renameInt
// refactor does not break ordinary same-parent rename of a real file.
func TestE2EVirtualRoot_RenameWithinMountRootSanity(t *testing.T) {
	backend := connectBackend(t)
	base := uniqueBackendBase("ren-sanity")
	ensureBackendDir(t, backend, base+"/projA/dsA", base)

	configs := []VirtualDirectoryConfig{{
		Name:        "shared-datasets",
		Paths:       []string{"projA/dsA"},
		BackendRoot: base,
	}}

	withVirtualMount(t, base, configs, func(mountPoint string, _ HdfsAccessor) {
		src := filepath.Join(mountPoint, "src.txt")
		dst := filepath.Join(mountPoint, "dst.txt")
		if err := createFile(src, "hello"); err != nil {
			t.Fatalf("createFile: %v", err)
		}
		if err := os.Rename(src, dst); err != nil {
			t.Fatalf("ordinary same-parent rename failed: %v", err)
		}
		if _, err := os.Stat(dst); err != nil {
			t.Errorf("dst should exist after rename: %v", err)
		}
		if _, err := os.Stat(src); !os.IsNotExist(err) {
			t.Errorf("src should be gone after rename, got: %v", err)
		}
	})
}

// TestE2EVirtualRoot_RenameIntoLeafSucceeds verifies renaming a real file
// FROM the mount root INTO a configured leaf (which is a real *DirINode whose
// Parent is *VirtualDirINode) succeeds and resolves to the leaf's backend path.
func TestE2EVirtualRoot_RenameIntoLeafSucceeds(t *testing.T) {
	backend := connectBackend(t)
	base := uniqueBackendBase("ren-into-leaf")
	ensureBackendDir(t, backend, base+"/projA/dsA", base)

	configs := []VirtualDirectoryConfig{{
		Name:        "shared-datasets",
		Paths:       []string{"projA/dsA"},
		BackendRoot: base,
	}}

	withVirtualMount(t, base, configs, func(mountPoint string, _ HdfsAccessor) {
		src := filepath.Join(mountPoint, "src.txt")
		dst := filepath.Join(mountPoint, "shared-datasets", "projA", "dsA", "dst.txt")
		if err := createFile(src, "hello"); err != nil {
			t.Fatalf("createFile: %v", err)
		}
		if err := os.Rename(src, dst); err != nil {
			t.Fatalf("rename mount-root -> leaf failed: %v", err)
		}
		if _, err := os.Stat(dst); err != nil {
			t.Errorf("dst should exist in leaf: %v", err)
		}
		// And verify the backend actually has it at the resolved path.
		if _, err := backend.Stat(base + "/projA/dsA/dst.txt"); err != nil {
			t.Errorf("backend should have file at resolved leaf path: %v", err)
		}
	})
}

// TestE2EVirtualRoot_RenameFromLeafToMountRoot verifies renaming a file FROM
// a configured leaf back to the mount root succeeds.
func TestE2EVirtualRoot_RenameFromLeafToMountRoot(t *testing.T) {
	backend := connectBackend(t)
	base := uniqueBackendBase("ren-from-leaf")
	ensureBackendDir(t, backend, base+"/projA/dsA", base)

	configs := []VirtualDirectoryConfig{{
		Name:        "shared-datasets",
		Paths:       []string{"projA/dsA"},
		BackendRoot: base,
	}}

	withVirtualMount(t, base, configs, func(mountPoint string, _ HdfsAccessor) {
		leafFile := filepath.Join(mountPoint, "shared-datasets", "projA", "dsA", "src.txt")
		if err := createFile(leafFile, "hello"); err != nil {
			t.Fatalf("createFile in leaf: %v", err)
		}
		dst := filepath.Join(mountPoint, "moved.txt")
		if err := os.Rename(leafFile, dst); err != nil {
			t.Fatalf("rename leaf -> mount root failed: %v", err)
		}
		if _, err := os.Stat(dst); err != nil {
			t.Errorf("dst should exist at mount root: %v", err)
		}
	})
}

// TestE2EVirtualRoot_RenameBetweenLeavesSucceeds verifies renaming between
// two different configured leaves succeeds; both parents are real *DirINode
// whose Parents are *VirtualDirINode.
func TestE2EVirtualRoot_RenameBetweenLeavesSucceeds(t *testing.T) {
	backend := connectBackend(t)
	base := uniqueBackendBase("ren-leaf2leaf")
	ensureBackendDir(t, backend, base+"/projA/dsA", base)
	ensureBackendDir(t, backend, base+"/projB/dsB", "")

	configs := []VirtualDirectoryConfig{{
		Name:        "shared-datasets",
		Paths:       []string{"projA/dsA", "projB/dsB"},
		BackendRoot: base,
	}}

	withVirtualMount(t, base, configs, func(mountPoint string, _ HdfsAccessor) {
		src := filepath.Join(mountPoint, "shared-datasets", "projA", "dsA", "x.txt")
		dst := filepath.Join(mountPoint, "shared-datasets", "projB", "dsB", "y.txt")
		if err := createFile(src, "hello"); err != nil {
			t.Fatalf("createFile in leaf A: %v", err)
		}
		if err := os.Rename(src, dst); err != nil {
			t.Fatalf("leaf -> leaf rename failed: %v", err)
		}
		if _, err := backend.Stat(base + "/projB/dsB/y.txt"); err != nil {
			t.Errorf("backend should have moved file at leaf B path: %v", err)
		}
	})
}

// TestE2EVirtualRoot_RenameInsideSyntheticRejected verifies that a rename
// whose SOURCE parent is *VirtualDirINode (mv shared-datasets/projA somewhere)
// returns EPERM via VirtualDirINode.Rename intercepting before renameInt runs.
func TestE2EVirtualRoot_RenameInsideSyntheticRejected(t *testing.T) {
	backend := connectBackend(t)
	base := uniqueBackendBase("ren-from-virt")
	ensureBackendDir(t, backend, base+"/projA/dsA", base)
	ensureBackendDir(t, backend, base+"/projB/dsB", "")

	configs := []VirtualDirectoryConfig{{
		Name:        "shared-datasets",
		Paths:       []string{"projA/dsA", "projB/dsB"},
		BackendRoot: base,
	}}

	withVirtualMount(t, base, configs, func(mountPoint string, _ HdfsAccessor) {
		// projA is a *VirtualDirINode child of shared-datasets. Renaming it should EPERM.
		src := filepath.Join(mountPoint, "shared-datasets", "projA")
		dst := filepath.Join(mountPoint, "shared-datasets", "projC")
		assertSyscallErr(t, os.Rename(src, dst), syscall.EPERM)
	})
}

// TestE2EVirtualRoot_RenameIntoVirtualBranchRejected verifies that renaming
// a real file with destination parent = synthetic branch (not the root)
// returns EPERM. Exercises the dstParentDir.(*DirINode) guard in renameInt
// against a non-root *VirtualDirINode.
func TestE2EVirtualRoot_RenameIntoVirtualBranchRejected(t *testing.T) {
	backend := connectBackend(t)
	base := uniqueBackendBase("ren-into-branch")
	ensureBackendDir(t, backend, base+"/projA/dsA", base)
	ensureBackendDir(t, backend, base+"/realdir", "")

	configs := []VirtualDirectoryConfig{{
		Name:        "shared-datasets",
		Paths:       []string{"projA/dsA"},
		BackendRoot: base,
	}}

	withVirtualMount(t, base, configs, func(mountPoint string, _ HdfsAccessor) {
		// Destination parent is shared-datasets/projA, a synthetic branch (*VirtualDirINode).
		src := filepath.Join(mountPoint, "realdir")
		dst := filepath.Join(mountPoint, "shared-datasets", "projA", "renamed-here")
		assertSyscallErr(t, os.Rename(src, dst), syscall.EPERM)
	})
}

// ============================================================================
// ENOTSUP rejection
// ============================================================================

// TestE2EVirtualRoot_SymlinkOnSyntheticRejected verifies symlinking into a
// synthetic directory returns ENOTSUP (not EPERM, not success).
func TestE2EVirtualRoot_SymlinkOnSyntheticRejected(t *testing.T) {
	backend := connectBackend(t)
	base := uniqueBackendBase("sym")
	ensureBackendDir(t, backend, base+"/projA/dsA", base)

	configs := []VirtualDirectoryConfig{{
		Name:        "shared-datasets",
		Paths:       []string{"projA/dsA"},
		BackendRoot: base,
	}}

	withVirtualMount(t, "/", configs, func(mountPoint string, _ HdfsAccessor) {
		link := filepath.Join(mountPoint, "shared-datasets", "mylink")
		err := os.Symlink("/some/target", link)
		assertSyscallErr(t, err, syscall.ENOTSUP)
	})
}

// TestE2EVirtualRoot_HardLinkOnSyntheticRejected verifies hard linking into
// a synthetic directory returns ENOTSUP.
func TestE2EVirtualRoot_HardLinkOnSyntheticRejected(t *testing.T) {
	backend := connectBackend(t)
	base := uniqueBackendBase("hlink")
	ensureBackendDir(t, backend, base+"/projA/dsA", base)

	configs := []VirtualDirectoryConfig{{
		Name:        "shared-datasets",
		Paths:       []string{"projA/dsA"},
		BackendRoot: base,
	}}

	withVirtualMount(t, base, configs, func(mountPoint string, _ HdfsAccessor) {
		realFile := filepath.Join(mountPoint, "source.txt")
		if err := createFile(realFile, "data"); err != nil {
			t.Fatalf("createFile source: %v", err)
		}
		link := filepath.Join(mountPoint, "shared-datasets", "linked")
		err := os.Link(realFile, link)
		// os.Link may return ENOTSUP or EPERM depending on which side fails first;
		// our *VirtualDirINode.Link returns ENOTSUP.
		assertSyscallErr(t, err, syscall.ENOTSUP)
	})
}

// ============================================================================
// File collision
// ============================================================================

// TestE2EVirtualRoot_CollisionRealFileWins verifies that if a real backend
// FILE (not directory) exists at <srcDir>/<virtual-name>, the file shadows
// the virtual entry. The user sees the file, not the synthetic directory.
func TestE2EVirtualRoot_CollisionRealFileWins(t *testing.T) {
	backend := connectBackend(t)
	base := uniqueBackendBase("file-collision")
	ensureBackendDir(t, backend, base+"/projA/dsA", base)

	// Create a regular file at <srcDir>/shared-datasets.
	writer, err := backend.CreateFile(base+"/shared-datasets", os.FileMode(0644), false)
	if err != nil {
		t.Fatalf("CreateFile collision: %v", err)
	}
	_, _ = writer.Write([]byte("real file content"))
	_ = writer.Close()

	configs := []VirtualDirectoryConfig{{
		Name:        "shared-datasets",
		Paths:       []string{"projA/dsA"},
		BackendRoot: base,
	}}

	withVirtualMount(t, base, configs, func(mountPoint string, _ HdfsAccessor) {
		// shared-datasets should resolve to the REAL FILE, not a synthetic directory.
		info, err := os.Stat(filepath.Join(mountPoint, "shared-datasets"))
		if err != nil {
			t.Fatalf("Stat colliding name failed: %v", err)
		}
		if info.IsDir() {
			t.Errorf("Expected real file collision to win — got a directory instead")
		}
		if info.Size() != int64(len("real file content")) {
			t.Errorf("File size mismatch: got %d", info.Size())
		}
	})
}
