// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package hopsfsmount

import (
	"os"

	"github.com/colinmarc/hdfs/v2"
)

// Adds automatic retry capability to HdfsAccessor with respect to RetryPolicy
type FaultTolerantHdfsAccessor struct {
	Impl        HdfsAccessor
	RetryPolicy *RetryPolicy
}

var _ HdfsAccessor = (*FaultTolerantHdfsAccessor)(nil) // ensure FaultTolerantHdfsAccessor implements HdfsAccessor

// Creates an instance of FaultTolerantHdfsAccessor
func NewFaultTolerantHdfsAccessor(impl HdfsAccessor, retryPolicy *RetryPolicy) *FaultTolerantHdfsAccessor {
	return &FaultTolerantHdfsAccessor{
		Impl:        impl,
		RetryPolicy: retryPolicy}
}

// Ensures HDFS accessor is connected to the HDFS name node
func (fta *FaultTolerantHdfsAccessor) EnsureConnected() error {
	op := fta.RetryPolicy.StartOperation()
	for {
		err := fta.Impl.EnsureConnected()
		if IsSuccessOrNonRetriableError(err) || !op.ShouldRetry("Connect: %s", err) {
			return err
		}
	}
}

// Opens HDFS file for reading
func (fta *FaultTolerantHdfsAccessor) OpenRead(path string) (ReadSeekCloser, error) {
	op := fta.RetryPolicy.StartOperation()
	for {
		result, err := fta.Impl.OpenRead(path)
		if err == nil {
			return result, nil
		}
		if IsSuccessOrNonRetriableError(err) || !op.ShouldRetry("[%s] OpenRead: %s", path, err) {
			return nil, err
		} else {
			// Clean up the bad connection, to let underline connection to get automatic refresh
			fta.Impl.Close()
		}
	}
}

// Opens HDFS file for writing
func (fta *FaultTolerantHdfsAccessor) CreateFile(path string, mode os.FileMode, overwrite bool) (HdfsWriter, error) {
	// TODO: implement fault-tolerance. For now re-try-loop is implemented inside FileHandleWriter
	return fta.Impl.CreateFile(path, mode, overwrite)
}

// Opens HDFS file for writing with specific group
func (fta *FaultTolerantHdfsAccessor) CreateFileWithGroup(path string, mode os.FileMode, overwrite bool, groupname string) (HdfsWriter, error) {
	// TODO: implement fault-tolerance. For now re-try-loop is implemented inside FileHandleWriter
	return fta.Impl.CreateFileWithGroup(path, mode, overwrite, groupname)
}

// Enumerates HDFS directory
func (fta *FaultTolerantHdfsAccessor) ReadDir(path string) ([]Attrs, error) {
	op := fta.RetryPolicy.StartOperation()
	for {
		result, err := fta.Impl.ReadDir(path)
		if IsSuccessOrNonRetriableError(err) || !op.ShouldRetry("[%s] ReadDir: %s", path, err) {
			return result, err
		} else {
			// Clean up the bad connection, to let underline connection to get automatic refresh
			fta.Impl.Close()
		}
	}
}

// Retrieves file/directory attributes
func (fta *FaultTolerantHdfsAccessor) Stat(path string) (Attrs, error) {
	op := fta.RetryPolicy.StartOperation()
	for {
		result, err := fta.Impl.Stat(path)
		if IsSuccessOrNonRetriableError(err) || !op.ShouldRetry("[%s] Stat: %s", path, err) {
			return result, err
		} else {
			// Clean up the bad connection, to let underline connection to get automatic refresh
			fta.Impl.Close()
		}
	}
}

// Retrieves HDFS usage
func (fta *FaultTolerantHdfsAccessor) StatFs() (FsInfo, error) {
	op := fta.RetryPolicy.StartOperation()
	for {
		result, err := fta.Impl.StatFs()
		if IsSuccessOrNonRetriableError(err) || !op.ShouldRetry("StatFs: %s", err) {
			return result, err
		} else {
			// Clean up the bad connection, to let underline connection to get automatic refresh
			fta.Impl.Close()
		}
	}
}

// Creates a directory
func (fta *FaultTolerantHdfsAccessor) Mkdir(path string, mode os.FileMode) error {
	op := fta.RetryPolicy.StartOperation()
	for {
		err := fta.Impl.Mkdir(path, mode)
		if IsSuccessOrNonRetriableError(err) || !op.ShouldRetry("[%s] Mkdir %s: %s", path, mode, err) {
			return err
		} else {
			// Clean up the bad connection, to let underline connection to get automatic refresh
			fta.Impl.Close()
		}
	}
}

// Creates a directory with specific group
func (fta *FaultTolerantHdfsAccessor) MkdirWithGroup(path string, mode os.FileMode, groupname string) error {
	op := fta.RetryPolicy.StartOperation()
	for {
		err := fta.Impl.MkdirWithGroup(path, mode, groupname)
		if IsSuccessOrNonRetriableError(err) || !op.ShouldRetry("[%s] MkdirWithGroup %s (group=%s): %s", path, mode, groupname, err) {
			return err
		} else {
			// Clean up the bad connection, to let underline connection to get automatic refresh
			fta.Impl.Close()
		}
	}
}

// Removes a file or directory
func (fta *FaultTolerantHdfsAccessor) Remove(path string) error {
	op := fta.RetryPolicy.StartOperation()
	for {
		err := fta.Impl.Remove(path)
		if IsSuccessOrNonRetriableError(err) || !op.ShouldRetry("[%s] Remove: %s", path, err) {
			return err
		} else {
			// Clean up the bad connection, to let underline connection to get automatic refresh
			fta.Impl.Close()
		}
	}
}

// Renames file or directory
func (fta *FaultTolerantHdfsAccessor) Rename(oldPath string, newPath string) error {
	op := fta.RetryPolicy.StartOperation()
	for {
		err := fta.Impl.Rename(oldPath, newPath)
		if IsSuccessOrNonRetriableError(err) || !op.ShouldRetry("[%s] Rename to %s: %s", oldPath, newPath, err) {
			return err
		} else {
			// Clean up the bad connection, to let underline connection to get automatic refresh
			fta.Impl.Close()
		}
	}
}

// Renames file or directory
func (fta *FaultTolerantHdfsAccessor) Rename2(oldPath string, newPath string, options hdfs.RenameOptions) error {
	op := fta.RetryPolicy.StartOperation()
	for {
		err := fta.Impl.Rename2(oldPath, newPath, options)
		if IsSuccessOrNonRetriableError(err) || !op.ShouldRetry("[%s] Rename to %s: %s", oldPath, newPath, err) {
			return err
		} else {
			// Clean up the bad connection, to let underline connection to get automatic refresh
			fta.Impl.Close()
		}
	}
}

// Chmod file or directory
func (fta *FaultTolerantHdfsAccessor) Chmod(path string, mode os.FileMode) error {
	op := fta.RetryPolicy.StartOperation()
	for {
		err := fta.Impl.Chmod(path, mode)
		if IsSuccessOrNonRetriableError(err) || !op.ShouldRetry("Chmod [%s] to [%d]: %s", path, mode, err) {
			return err
		} else {
			// Clean up the bad connection, to let underline connection to get automatic refresh
			fta.Impl.Close()
		}
	}
}

// Chown file or directory
func (fta *FaultTolerantHdfsAccessor) Chown(path string, user, group string) error {
	op := fta.RetryPolicy.StartOperation()
	for {
		err := fta.Impl.Chown(path, user, group)
		if IsSuccessOrNonRetriableError(err) || !op.ShouldRetry("Chown [%s] to [%s:%s]: %s", path, user, group, err) {
			return err
		} else {
			// Clean up the bad connection, to let underline connection to get automatic refresh
			fta.Impl.Close()
		}
	}
}

// Close underline connection if needed
func (fta *FaultTolerantHdfsAccessor) Close() error {
	return fta.Impl.Close()
}
