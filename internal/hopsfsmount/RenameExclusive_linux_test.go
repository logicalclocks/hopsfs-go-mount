// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package hopsfsmount

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// renameat2(RENAME_NOREPLACE) through the mount renames onto a missing path and fails with EEXIST
// onto an existing one, leaving both files as they were.
func TestRenameNoReplace(t *testing.T) {
	withMount(t, "/", DelaySyncUntilClose, func(mountPoint string, hdfsAccessor HdfsAccessor) {
		src := filepath.Join(mountPoint, uniqueTestName("noreplace_src"))
		other := filepath.Join(mountPoint, uniqueTestName("noreplace_other"))
		dst := filepath.Join(mountPoint, uniqueTestName("noreplace_dst"))
		defer os.Remove(src)
		defer os.Remove(other)
		defer os.Remove(dst)
		if err := createFile(src, "src"); err != nil {
			t.Fatalf("create %s: %v", src, err)
		}
		if err := createFile(other, "other"); err != nil {
			t.Fatalf("create %s: %v", other, err)
		}

		if err := unix.Renameat2(unix.AT_FDCWD, src, unix.AT_FDCWD, dst, unix.RENAME_NOREPLACE); err != nil {
			t.Fatalf("rename onto a missing path: %v", err)
		}
		err := unix.Renameat2(unix.AT_FDCWD, other, unix.AT_FDCWD, dst, unix.RENAME_NOREPLACE)
		if !errors.Is(err, unix.EEXIST) {
			t.Fatalf("rename onto an existing path. Expecting: EEXIST, Got: %v", err)
		}
		if got, _ := readFile(t, dst); got != "src" {
			t.Fatalf("the destination was replaced. Expecting: src, Got: %s", got)
		}
		if got, _ := readFile(t, other); got != "other" {
			t.Fatalf("the source of the failed rename changed. Expecting: other, Got: %s", got)
		}
	})
}

// lookUpAbsent looks each path up through its mount and fails unless it is absent. The mount keeps
// the name in its negative cache, so the kernel's lookup before a rename or create onto it gets
// ENOENT from the mount, and the operation reaches the namenode, even after another mount has made
// the name.
func lookUpAbsent(t *testing.T, round int, paths ...string) {
	t.Helper()
	for _, p := range paths {
		if _, err := os.Stat(p); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("round %d: %s before the race. Expecting: ENOENT, Got: %v", round, p, err)
		}
	}
}

// Two mounts, as two pods have, rename their own file onto one path with RENAME_NOREPLACE at once.
// Both have the path as absent, so both renames reach the namenode, and exactly one wins.
func TestRenameNoReplaceAcrossMounts(t *testing.T) {
	// Longer than a round, so the negative cache entries from lookUpAbsent last through it.
	origCacheAttrsTimeDuration := CacheAttrsTimeDuration
	CacheAttrsTimeDuration = 30 * time.Second
	defer func() { CacheAttrsTimeDuration = origCacheAttrsTimeDuration }()

	withMount(t, "/", DelaySyncUntilClose, func(mountA string, hdfsAccessor HdfsAccessor) {
		withMount(t, "/", DelaySyncUntilClose, func(mountB string, _ HdfsAccessor) {
			for round := 0; round < 10; round++ {
				srcA := filepath.Join(mountA, uniqueTestName("across_src_a"))
				srcB := filepath.Join(mountB, uniqueTestName("across_src_b"))
				dstName := uniqueTestName("across_dst")
				if err := createFile(srcA, "a"); err != nil {
					t.Fatalf("create %s: %v", srcA, err)
				}
				if err := createFile(srcB, "b"); err != nil {
					t.Fatalf("create %s: %v", srcB, err)
				}
				lookUpAbsent(t, round, filepath.Join(mountA, dstName), filepath.Join(mountB, dstName))

				errs := make([]error, 2)
				var start, done sync.WaitGroup
				start.Add(1)
				for i, mv := range [][2]string{{srcA, filepath.Join(mountA, dstName)}, {srcB, filepath.Join(mountB, dstName)}} {
					done.Add(1)
					go func(i int, from, to string) {
						defer done.Done()
						start.Wait()
						errs[i] = unix.Renameat2(unix.AT_FDCWD, from, unix.AT_FDCWD, to, unix.RENAME_NOREPLACE)
					}(i, mv[0], mv[1])
				}
				start.Done()
				done.Wait()

				winners := 0
				for _, err := range errs {
					if err == nil {
						winners++
					} else if !errors.Is(err, unix.EEXIST) {
						t.Fatalf("round %d: a losing rename failed with %v. Expecting: EEXIST", round, err)
					}
				}
				if winners != 1 {
					t.Fatalf("round %d: %d renames onto %s won", round, winners, dstName)
				}
				// Cleaned up on the backend: when B won, mount A still has the destination as
				// absent from its lookup, so a remove through A may not reach the namenode.
				if err := hdfsAccessor.Remove("/" + dstName); err != nil {
					t.Fatalf("round %d: remove %s: %v", round, dstName, err)
				}
				for _, src := range []string{srcA, srcB} {
					if err := hdfsAccessor.Remove("/" + filepath.Base(src)); err != nil && !errors.Is(err, unix.ENOENT) {
						t.Fatalf("round %d: remove %s: %v", round, src, err)
					}
				}
			}
		})
	})
}

// Two mounts create one path with O_CREAT|O_EXCL at once. Both have the path as absent, so both
// creates reach the namenode: exactly one wins, the other gets EEXIST.
func TestCreateExclusiveAcrossMounts(t *testing.T) {
	origCacheAttrsTimeDuration := CacheAttrsTimeDuration
	CacheAttrsTimeDuration = 30 * time.Second
	defer func() { CacheAttrsTimeDuration = origCacheAttrsTimeDuration }()

	withMount(t, "/", DelaySyncUntilClose, func(mountA string, hdfsAccessor HdfsAccessor) {
		withMount(t, "/", DelaySyncUntilClose, func(mountB string, _ HdfsAccessor) {
			for round := 0; round < 10; round++ {
				name := uniqueTestName("excl")
				lookUpAbsent(t, round, filepath.Join(mountA, name), filepath.Join(mountB, name))
				files := make([]*os.File, 2)
				errs := make([]error, 2)
				var start, done sync.WaitGroup
				start.Add(1)
				for i, mountPoint := range []string{mountA, mountB} {
					done.Add(1)
					go func(i int, path string) {
						defer done.Done()
						start.Wait()
						files[i], errs[i] = os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
					}(i, filepath.Join(mountPoint, name))
				}
				start.Done()
				done.Wait()

				winners := 0
				for i, err := range errs {
					if err == nil {
						winners++
						files[i].Close()
					} else if !errors.Is(err, unix.EEXIST) {
						t.Fatalf("round %d: a losing create failed with %v. Expecting: EEXIST", round, err)
					}
				}
				if winners != 1 {
					t.Fatalf("round %d: %d creates of %s won", round, winners, name)
				}
				// On the backend, for the same reason as in TestRenameNoReplaceAcrossMounts.
				if err := hdfsAccessor.Remove("/" + name); err != nil {
					t.Fatalf("round %d: remove %s: %v", round, name, err)
				}
			}
		})
	})
}

// A rename onto a non-empty directory fails with ENOTEMPTY, which the mount does not retry.
func TestRenameOntoNonEmptyDirectoryIsENOTEMPTY(t *testing.T) {
	withMount(t, "/", DelaySyncUntilClose, func(mountPoint string, hdfsAccessor HdfsAccessor) {
		src := filepath.Join(mountPoint, uniqueTestName("notempty_src"))
		dstName := uniqueTestName("notempty_dst")
		dst := filepath.Join(mountPoint, dstName)
		defer os.RemoveAll(src)
		defer os.RemoveAll(dst)
		mkdir(t, src)
		mkdir(t, dst)
		// The child is created on the backend, not through the mount: neither the kernel nor the
		// mount knows the destination is not empty, so ENOTEMPTY can only come from the namenode.
		w, err := hdfsAccessor.CreateFile("/"+dstName+"/file", 0644, false)
		if err != nil {
			t.Fatalf("create in %s on the backend: %v", dstName, err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("close %s/file on the backend: %v", dstName, err)
		}

		// os.Rename refuses an existing directory destination itself, without the syscall.
		err = unix.Rename(src, dst)
		if !errors.Is(err, unix.ENOTEMPTY) {
			t.Fatalf("rename onto a non-empty directory. Expecting: ENOTEMPTY, Got: %v", err)
		}
	})
}
