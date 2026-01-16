// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package hopsfsmount

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"bazil.org/fuse/fs/fstestutil"
	"github.com/colinmarc/hdfs/v2"
	"golang.org/x/sys/unix"
	"hopsworks.ai/hopsfsmount/internal/hopsfsmount/logger"
	"hopsworks.ai/hopsfsmount/internal/hopsfsmount/ugcache"
)

func TestReadWriteEmptyFile(t *testing.T) {

	withMount(t, "/", DelaySyncUntilClose, func(mountPoint string, hdfsAccessor HdfsAccessor) {
		//create a file, make sure that use and group information is correct
		r := rand.New(rand.NewSource(time.Now().Local().Unix()))
		for i := 0; i < 10; i++ {
			testFile := filepath.Join(mountPoint, fmt.Sprintf("somefile_%d", r.Int()))
			os.Remove(testFile)

			file, err := os.Create(testFile)
			if err != nil {
				t.Fatalf("Unable to create a new file")
			}

			file.WriteString("test")
			err = file.Close()
			if err != nil {
				t.Fatalf("Close failed")
			}
			os.Remove(testFile)
		}
		logger.Debug("Done", nil)
	})
}

func TestSimple(t *testing.T) {

	withMount(t, "/", DelaySyncUntilClose, func(mountPoint string, hdfsAccessor HdfsAccessor) {
		for i := 0; i < 3; i++ {
			testFile := filepath.Join(mountPoint, fmt.Sprintf("somefile_%d", i))
			os.Remove(testFile)
			logger.Info(fmt.Sprintf("New file: %s", testFile), nil)
			if err := createFile(testFile, "some data"); err != nil {
				t.Fatalf("Failed to write %v", err)
			}
			os.Remove(testFile)
		}
	})
}

func TestRename1(t *testing.T) {

	withMount(t, "/", DelaySyncUntilClose, func(mountPoint string, hdfsAccessor HdfsAccessor) {

		// perform this N number of
		//   1. rename file1 -> file2
		//   2. rename file2 -> file1
		//   3. check file stats
		file1 := filepath.Join(mountPoint, "file1")
		file2 := filepath.Join(mountPoint, "file2")
		t.Run("test1", func(t *testing.T) {
			renameTestInt(t, file1, file2)
		})

		file1 = filepath.Join(mountPoint, "file1")
		dir := filepath.Join(mountPoint, "/dir")
		file2 = filepath.Join(mountPoint, "/dir/file2")
		rmDir(t, dir)
		mkdir(t, dir)
		t.Run("test2", func(t *testing.T) {
			renameTestInt(t, file1, file2)
		})

	})

}

func renameTestInt(t *testing.T, file1, file2 string) {

	if err := createFile(file1, "some_data"); err != nil {
		t.Fatalf("Failed %v", err)
	}
	defer os.Remove(file1)

	if err := createFile(file2, "some_data"); err != nil {
		t.Fatalf("Failed %v", err)
	}
	defer os.Remove(file2)

	for i := 0; i < 20; i++ {
		logger.Debug(fmt.Sprintf("_____ %d _____", i), nil)

		if err := os.Rename(file1, file2); err != nil {
			t.Fatalf("Failed %v", err)
		}

		if err := os.Rename(file2, file1); err != nil {
			t.Fatalf("Failed %v", err)
		}

		fi, err := os.Stat(file1)
		if err != nil {
			t.Fatalf("Stat Failed %v", err)
		}
		logger.Debug(fmt.Sprintf("Stat worked. File Size: %d", fi.Size()), nil)

		time.Sleep(time.Microsecond * 100)
	}
}

func TestTruncate(t *testing.T) {

	withMount(t, "/", DelaySyncUntilClose, func(mountPoint string, hdfsAccessor HdfsAccessor) {
		//create a file, make sure that use and group information is correct
		testFile := filepath.Join(mountPoint, "somefile")
		os.Remove(testFile)

		logger.Info(fmt.Sprintf("New file: %s", testFile), nil)
		data1 := "123456790"
		data2 := "abcde"
		if err := createFile(testFile, data1); err != nil {
			t.Fatalf("Failed to write %v", err)
		}
		fi, _ := os.Stat(testFile)
		fileSize := fi.Size()

		if int(fileSize) != len(data1) {
			t.Errorf("Invalid file size. Expecting: %d Got: %d", len(data1), fileSize)
		}

		if err := createFile(testFile, data2); err != nil { // truncates if file already exists
			t.Fatalf("Failed to write %v", err)
		}
		fi, _ = os.Stat(testFile)
		fileSize = fi.Size()

		if int(fileSize) != len(data2) {
			t.Errorf("Invalid file size. Expecting: %d Got: %d", len(data2), fileSize)
		}

		os.Remove(testFile)
	})
}

func TestTruncateGreaterLength(t *testing.T) {

	withMount(t, "/", DelaySyncUntilClose, func(mountPoint string, hdfsAccessor HdfsAccessor) {
		//create a file, make sure that use and group information is correct
		testFile1 := filepath.Join(mountPoint, "somefile1")
		os.Remove(testFile1)
		truncateLen := int64(1024 * 1024)

		file, err := os.Create(testFile1)
		if err != nil {
			t.Fatalf("Unable to create a new file")
		}

		stat, err := file.Stat()
		if err != nil {
			t.Fatalf("Unable to stat test file")
		}

		if stat.Size() != 0 {
			t.Fatalf("Wrong file size. Expecting: 0. Got: %d ", stat.Size())
		}

		err = file.Truncate(truncateLen)
		if err != nil {
			t.Fatalf("Truncate failed")
		}

		err = file.Close()
		if err != nil {
			t.Fatalf("Close failed")
		}

		fileReader, err := os.Open(testFile1)
		if err != nil {
			t.Fatalf("File open failed")
		}

		buffer := make([]byte, truncateLen)
		lenRead, err := fileReader.Read(buffer)
		if err != nil {
			t.Fatalf("File read failed")
		}

		if lenRead != int(truncateLen) {
			t.Fatalf("Expecting %d bytes to read. Got: %d", truncateLen, lenRead)
		}

		err = fileReader.Close()
		if err != nil {
			t.Fatalf("File close failed")
		}

		os.Remove(testFile1)
		logger.Debug("Done", nil)
	})
}

// testing multiple read write clients perfile
func TestMultipleRWCllients(t *testing.T) {

	withMount(t, "/", DelaySyncUntilClose, func(mountPoint string, hdfsAccessor HdfsAccessor) {
		//create a file, make sure that use and group information is correct
		// mountPoint = "/tmp"
		testFile1 := filepath.Join(mountPoint, "somefile")
		testFile2 := filepath.Join(mountPoint, "somefile.bak")
		logger.Info(fmt.Sprintf("New file: %s", testFile1), nil)
		if err := createFile(testFile1, "initial data\nadsf\n"); err != nil {
			t.Fatalf("Failed to write %v", err)
		}

		c1, _ := os.OpenFile(testFile1, os.O_RDWR, 0600)
		c2, _ := os.OpenFile(testFile1, os.O_RDWR, 0600)
		c3, _ := os.OpenFile(testFile1, os.O_RDWR, 0600)

		c1.WriteString("First client\n")
		c1.Close()

		os.Rename(testFile1, testFile2)

		c2.WriteString("Second client\nSecond client\n")
		c3.WriteString("Third client\nThird client\nThird Client\n")
		c2.Close()
		c3.Close()

		c5, err := os.Open(testFile2)

		if err != nil {
			t.Error("The file should have opened successfully")
		} else {
			logger.Info("File opened successfully", nil)
			buffer := make([]byte, 1024)
			c5.Read(buffer)
			//fmt.Printf("%s", buffer)
		}
		c5.Close()

		os.Remove(testFile1)
		os.Remove(testFile2)
	})
}

func TestMountSubDir(t *testing.T) {
	//mount and create some files and dirs
	dirs := 5
	filesPdir := 3
	withMount(t, "/", DelaySyncUntilClose, func(mountPoint string, hdfsAccessor HdfsAccessor) {
		//create some directories and files
		for i := 0; i < dirs; i++ {
			dir := filepath.Join(mountPoint, "dir"+strconv.Itoa(i))
			mkdir(t, dir)
			for j := 0; j < filesPdir; j++ {
				f := filepath.Join(dir, "file"+strconv.Itoa(j))
				if err := createFile(f, "initial data"); err != nil {
					t.Fatalf("Failed to write %v", err)
				}
			}
		}

		content := listDir(t, mountPoint)
		if len(content) < dirs {
			t.Errorf("Failed. Expected >= %d, Got %d", dirs, len(content))
		}
	})

	// remount only one dir
	withMount(t, "/dir1", DelaySyncUntilClose, func(mountPoint string, hdfsAccessor HdfsAccessor) {
		content := listDir(t, mountPoint)
		if len(content) != filesPdir {
			t.Errorf("Failed. Expected == %d, Got %d ", filesPdir, len(content))
			for _, ent := range content {
				logger.Info(fmt.Sprintf("file/dir %s", ent.Name()), nil)
			}
		}
	})

	// remount every thing for cleanup
	withMount(t, "/", DelaySyncUntilClose, func(mountPoint string, hdfsAccessor HdfsAccessor) {
		//delete all the files and dirs created in this test
		for i := 0; i < dirs; i++ {
			dir := filepath.Join(mountPoint, "dir"+strconv.Itoa(i))
			for j := 0; j < filesPdir; j++ {
				f := filepath.Join(dir, "file"+strconv.Itoa(j))
				err := rmFile(f)
				if err != nil {
					t.Fatalf("Deleting file failed %s, Error: %v", f, err)
				}
			}
			err := rmFile(dir)
			if err != nil {
				t.Fatalf("Deleting file failed %s, Error: %v", dir, err)
			}

		}
	})
}

// perform lots of seek operations on large files
func TestSeekLargeFile(t *testing.T) {
	diskSeekTestFile := "/tmp/diskSeekTestLargeFile"
	dfsSeekTestFile := "/dfsSeekTestLargeFile"
	seekTest(t, 10000000 /*numbers in the file*/, diskSeekTestFile, dfsSeekTestFile)
}

// perform lots of seek operations on small files
func TestSeekSmallFile(t *testing.T) {
	diskSeekTestFile := "/tmp/diskSeekTestSmallFile"
	dfsSeekTestFile := "/dfsSeekTestSmallFile"
	seekTest(t, 10000 /*numbers in the file*/, diskSeekTestFile, dfsSeekTestFile)
}

func seekTest(t *testing.T, dataSize int, diskSeekTestFile string, dfsSeekTestFile string) {
	addresses := make([]string, 1)
	addresses[0] = "localhost:8020"

	user, err := ugcache.CurrentUserName()
	if err != nil {
		t.Fatalf("couldn't determine user: %s", err)
	}

	hdfsOptions := hdfs.ClientOptions{
		Addresses: addresses,
		User:      user,
	}

	client, err := hdfs.NewClient(hdfsOptions)
	if err != nil {
		t.Fatalf("Failed %v", err)
	}
	defer client.Close()

	prepare(t, client, dataSize, diskSeekTestFile, dfsSeekTestFile)

	testSeeks(t, client, diskSeekTestFile, dfsSeekTestFile)

	err = client.Remove(dfsSeekTestFile)
	if err != nil {
		t.Fatalf("Failed %v", err)
	}

}

func prepare(t *testing.T, client *hdfs.Client, dataSize int, diskTestFile string, dfsTestFile string) {

	logger.Info("Creating test data ...", nil)
	recreateDFSFile := false
	if _, err := os.Stat(diskTestFile); errors.Is(err, os.ErrNotExist) {
		testFile, err := os.Create(diskTestFile)
		if err != nil {
			t.Fatalf("Failed to create test file %v", err)
		}

		for i := 0; i < dataSize; i++ {
			number := fmt.Sprintf("%d,", i)
			testFile.Write([]byte(number))
		}
		testFile.Close()
		recreateDFSFile = true
	}

	if recreateDFSFile {
		client.Remove(dfsTestFile)
	}

	if _, err := client.Stat(dfsTestFile); errors.Is(err, os.ErrNotExist) {
		dfsWriter, err := client.CreateFile(dfsTestFile, 3, 1024*1024, os.FileMode(0777), true, true)
		if err != nil {
			t.Fatalf("Failed %v", err)
		}

		diskReader, err := os.Open(diskTestFile)
		if err != nil {
			t.Fatalf("Failed %v", err)
		}

		for {
			buffer := make([]byte, 1024*64)
			read, err := diskReader.Read(buffer)
			if read > 0 {
				written, err := dfsWriter.Write(buffer[:read])
				if written != read {
					t.Fatalf("Failed. The amount of read and write data do not match ")
				}
				if err != nil {
					t.Fatalf("Failed %v", err)
				}
			}

			if err != nil {
				break
			}
		}

		diskReader.Close()
		dfsWriter.Close()
	}

}

func testSeeks(t *testing.T, client *hdfs.Client, diskTestFile string, dfsTestFile string) {
	fileInfo, _ := os.Stat(diskTestFile)
	diskReader, _ := os.Open(diskTestFile)
	dfsReader, _ := client.Open(dfsTestFile)
	bufferSize := 4 * 1024
	for i := 0; i < 10000; i++ {

		// seek to random location
		seek := rand.Int63n(fileInfo.Size())

		// seek to random location at the end of the file
		// rng := int64(6 * 1024)
		// random := rand.Int63n(rng)
		// seek := fileInfo.Size() - rng + random

		// to to a location at the end of the block
		// blkSize := int64(1024 * 1024)
		// blk := rand.Int31n(int32(fileInfo.Size()/blkSize)-2) + 1
		// random := rand.Int63n(int64(bufferSize))
		// seek := int64(blk)*blkSize - random

		buffer1 := make([]byte, bufferSize)
		n, err := diskReader.Seek(seek, 0)
		if n != seek {
			t.Fatalf("Disk seek did not skip correct number of bytes. Expected: %d, Got: %d", seek, n)
		}
		if err != nil {
			t.Fatalf("Error in seek %v", err)
		}
		diskReadBytes, diskErr := diskReader.Read(buffer1)

		//fmt.Printf("%d) Seek %d, Bytes read from disk are %d, error: %v. Data: %s\n", i, seek, diskReadBytes, diskErr, string(buffer1)[:30])

		buffer2 := make([]byte, bufferSize)
		n, err = dfsReader.Seek(seek, 0)
		if n != seek {
			t.Fatalf("DFS seek did not skip correct number of bytes. Expected: %d, Got: %d", seek, n)
		}
		if err != nil {
			t.Fatalf("Error in seek %v", err)
		}

		b := buffer2[:]
		var dfsErr error
		var dfsReadBytes int = 0
		for len(b) > 0 {
			m, e := dfsReader.Read(b)

			if m > 0 {
				dfsReadBytes += m
				b = buffer2[dfsReadBytes:]
			}

			if e != nil {
				//fmt.Printf("Error %v\n", e)
				if dfsReadBytes == 0 || e != io.EOF {
					dfsErr = e
				}
				break
			}

		}

		//fmt.Printf("%d) Seek %d, Bytes read from dfs  are %d, error: %v. Data: %s\n\n", i, seek, dfsReadBytes, dfsErr, string(buffer2)[:30])

		if diskReadBytes != dfsReadBytes {
			fmt.Printf("FS: str %s\n", buffer1)
			fmt.Printf("DFS: str %s\n", buffer2)
			t.Fatalf("Size mismatch. Disk read size: %d, DFS read size:  %d", diskReadBytes, dfsReadBytes)
		}

		if diskErr != dfsErr {
			t.Fatalf("Error mismatch. Disk error: %v, DFS error:  %v", diskErr, dfsErr)
		}

		for index := 0; index < bufferSize; index++ {
			if buffer1[index] != buffer2[index] {
				fmt.Printf("FS: str %s\n", buffer1)
				fmt.Printf("DFS: str %s\n", buffer2)
				t.Fatalf("Bytes at index %d  do not match. Expecting %d, Got %d\n", index, buffer1[index], buffer2[index])
			}
		}
	}
}

func TestMultipleMountPoints(t *testing.T) {

	//clean up old runs
	withMount(t, "/", DelaySyncUntilClose, func(mountPoint string, hdfsAccessor HdfsAccessor) {
		testFile := filepath.Join(mountPoint, "somefile")
		rmFile(testFile)
	})

	var wg sync.WaitGroup
	wg.Add(2)

	times := 5

	// read, delete, write
	work := func(id int, testFile string) error {

		for i := 0; i < times; i++ {
			logger.Info(fmt.Sprintf("_____ Thread: %d. Try %d _____", id, i), nil)

			var data string
			var err error
			data, err = readFile(t, testFile)
			if err != nil {
				logger.Info(fmt.Sprintf("_____ Thread: %d.  Error while reading", id), logger.Fields{Error: err, ID: id, Path: testFile})
			}

			if data == "" {
				data = fmt.Sprintf("Test data. failed to read after try: %d", i)
			}

			err = rmFile(testFile)
			if err != nil {
				logger.Info(fmt.Sprintf("_____ Thread: %d. Deleting file failed ", id), logger.Fields{Error: err, ID: id, Path: testFile})
			}

			err = createFile(testFile, data)
			if err != nil {
				logger.Info(fmt.Sprintf("_____ Thread: %d. Error while saving file", id), logger.Fields{Error: err, ID: id, Path: testFile})
			}
		}

		logger.Info(fmt.Sprintf("_____ Thread: %d. Done  _____", id), nil)
		return nil
	}

	go withMount(t, "/", false, func(mountPoint string, hdfsAccessor HdfsAccessor) {
		testFile := filepath.Join(mountPoint, "somefile")
		work(0, testFile)
		wg.Done()
	})

	go withMount(t, "/", false, func(mountPoint string, hdfsAccessor HdfsAccessor) {
		testFile := filepath.Join(mountPoint, "somefile")
		work(1, testFile)
		wg.Done()
	})

	wg.Wait()

	// the file must exist
	withMount(t, "/", DelaySyncUntilClose, func(mountPoint string, hdfsAccessor HdfsAccessor) {
		testFile := filepath.Join(mountPoint, "somefile")
		logger.Info("Checking and Cleaing up", logger.Fields{Path: testFile})

		data, err := readFile(t, testFile)
		if err != nil {
			logger.Info("Error while reading", logger.Fields{Error: err, Path: testFile})
			t.Fatalf("Error while reading, %v", err)
		}
		logger.Info(fmt.Sprintf("Checking and Cleaing up. Data Read: %s", data),
			logger.Fields{Path: testFile})

		if data == "" {
			// unexpected. fail
			logger.Info("Error while reading. Expected to read some data", logger.Fields{Error: err, Path: testFile})
			t.Fatalf("Error while reading, %v", err)
		}

		// clean up
		err = rmFile(testFile)
		if err != nil {
			logger.Info("Error while deleting", logger.Fields{Error: err, Path: testFile})
			t.Fatalf("Error while deleting, %v", err)
		}

	})
}

func withMount(t testing.TB, srcDir string, delaySyncUntilClose bool, fn func(mntPath string, hdfsAccessor HdfsAccessor)) {
	t.Helper()

	// Enable cache for this test
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

	// Wrapping with FaultTolerantHdfsAccessor
	retryPolicy := NewDefaultRetryPolicy(WallClock{})
	retryPolicy.MaxAttempts = 1 // for quick failure
	logger.InitLogger("ERROR", false, "")
	hdfsAccessor, _ := NewHdfsAccessor("localhost:8020", WallClock{}, TLSConfig{TLS: false, RootCABundle: RootCABundle, ClientCertificate: ClientCertificate, ClientKey: ClientKey})
	err := hdfsAccessor.EnsureConnected()
	if err != nil {
		t.Fatalf(fmt.Sprintf("Error/NewHdfsAccessor: %v ", err), nil)
	}

	ftHdfsAccessor := NewFaultTolerantHdfsAccessor(hdfsAccessor, retryPolicy)

	// Creating the virtual file system
	fileSystem, err := NewFileSystem([]HdfsAccessor{ftHdfsAccessor}, srcDir, []string{"*"}, false, delaySyncUntilClose, retryPolicy, WallClock{})
	if err != nil {
		t.Fatalf(fmt.Sprintf("Error/NewFileSystem: %v ", err), nil)
	}

	mountOptions := GetMountOptions(false)
	mnt, err := fstestutil.MountedT(t, fileSystem, nil, mountOptions...)
	if err != nil {
		t.Fatal(fmt.Sprintf("Unable to mount fs: Error %v", err), nil)

	}
	defer mnt.Close()
	logger.Info(fmt.Sprintf("Connected to HopsFS. Mount point is %s", mnt.Dir), nil)

	//disable polling
	disablePolling(mnt.Dir)

	fn(mnt.Dir, hdfsAccessor)
}

func mkdir(t testing.TB, dir string) {
	t.Helper()
	err := os.Mkdir(dir, 0700)
	if err != nil {
		t.Errorf("Faile to create directory %s. Error: %v", dir, err)
	}

}

func createFile(filePath string, data string) error {
	out, err := os.Create(filePath)
	if err != nil {
		return err
	}
	out.WriteString(data)
	out.Close()
	return nil
}

func readFile(t testing.TB, filePath string) (string, error) {
	t.Helper()
	data, err := os.ReadFile(filePath)
	if err != nil {
		return "", err
	}
	return string(data[:]), nil
}

func listDir(t testing.TB, dir string) []fs.FileInfo {
	t.Helper()
	content, err := ioutil.ReadDir(dir)
	if err != nil {
		t.Errorf("Faile to list dir %s. Error: %v", dir, err)
	}
	return content
}

func rmFile(path string) error {
	return os.Remove(path)
}

func rmDir(t testing.TB, dir string) error {
	t.Helper()
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()

	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}

	for _, name := range names {
		err = os.RemoveAll(filepath.Join(dir, name))
		if err != nil {
			return err
		}
	}

	err = os.Remove(dir)
	if err != nil {
		return err
	}

	return nil
}

// https://github.com/bazil/fuse/issues/264
// https://github.com/hanwen/go-fuse/commit/4f10e248ebabd3cdf9c0aa3ae58fd15235f82a79#comments
// Only needed in unit tests
// Open a file and poll it once
// In unit test we send a poll request as soon as the
// mount point is started. The poll request will
// retun syscall.ENOSYS telling the kernel that the
// file system does not support polling and there is no
// need to send furter polling request.
// See HOPSFS-5 for more details
func disablePolling(rootDir string) {

	r := rand.New(rand.NewSource(int64(time.Now().Local().Nanosecond())))
	filePath := fmt.Sprintf("%s/__file_to_diable_polling__%d__", rootDir, r.Int())
	file, err := os.Create(filePath)
	if err != nil {
		logger.Error(fmt.Sprintf("Error opening file: %v", err), nil)
		return
	}
	defer os.Remove(filePath)
	defer file.Close()

	// Create an epoll instance
	fd := int(file.Fd())
	epollFd, err := unix.EpollCreate1(0)
	if err != nil {
		logger.Error(fmt.Sprintf("Error creating epoll instance: %v", err), nil)
		return
	}
	defer unix.Close(epollFd)

	// Add the file descriptor to the epoll instance
	event := unix.EpollEvent{
		Events: unix.EPOLLIN,
		Fd:     int32(fd),
	}
	if err := unix.EpollCtl(epollFd, unix.EPOLL_CTL_ADD, fd, &event); err != nil {
		logger.Error(fmt.Sprintf("Error adding file descriptor to epoll: %v", err), nil)
		return
	}

	logger.Info(fmt.Sprintf("Polling file %s...\n", filePath), nil)

	events := make([]unix.EpollEvent, 1)
	n, err := unix.EpollWait(epollFd, events, 1000)
	if err != nil {
		logger.Error(fmt.Sprintf("Error waiting for events: %v", err), nil)
		return
	}

	if n > 0 {
		// File is ready for reading
		logger.Debug("File is ready for polling.", nil)
	}

}

// TestConcurrentWritesWithFlush tests multiple goroutines writing to the same file
// with fsync calls between writes. This tests:
// 1. dataMutex properly serializes concurrent writes
// 2. File-level isDirty flag prevents redundant flushes
// 3. No data corruption with concurrent handles
func TestConcurrentWritesWithFlush1(t *testing.T) {
	testConcurrentWritesWithFlushInt(t, false)
}
func TestConcurrentWritesWithFlush2(t *testing.T) {
	testConcurrentWritesWithFlushInt(t, true)
}
func testConcurrentWritesWithFlushInt(t *testing.T, delaySyncUntilClose bool) {
	withMount(t, "/", delaySyncUntilClose, func(mountPoint string, hdfsAccessor HdfsAccessor) {
		// Create test directory
		testDir := filepath.Join(mountPoint, "test-concurrent-writes")
		os.RemoveAll(testDir) // Clean up from previous runs
		mkdir(t, testDir)
		defer os.RemoveAll(testDir)

		testFile := filepath.Join(testDir, "concurrent_test.txt")

		// Test parameters
		numThreads := 3
		bytesPerThread := 128 * 1024 * 1024 // 128MB per thread
		chunkSize := 64 * 1024              // 64KB chunks
		syncEveryMB := 30                   // Sync after every 10MB
		useSync := true                     // true = Sync(), false = no sync
		syncEveryBytes := syncEveryMB * 1024 * 1024
		totalFileSize := int64(numThreads * bytesPerThread)

		// Create file and truncate to total size
		f, err := os.Create(testFile)
		if err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}

		// Truncate file to allocate space for all threads
		err = f.Truncate(totalFileSize)
		if err != nil {
			t.Fatalf("Failed to truncate file: %v", err)
		}
		f.Close()

		syncDescription := fmt.Sprintf("Sync every %d MB", syncEveryMB)
		if !useSync {
			syncDescription = "No periodic sync (flush on close only)"
		}
		logger.Info(fmt.Sprintf("Test config: %d threads, %d MB per thread, %d KB chunks, %s",
			numThreads, bytesPerThread/(1024*1024), chunkSize/1024, syncDescription), nil)
		logger.Info(fmt.Sprintf("Total file size: %d bytes (%.1f MB)",
			totalFileSize, float64(totalFileSize)/1024/1024), nil)

		var wg sync.WaitGroup
		wg.Add(numThreads)

		startBarrier := make(chan struct{})
		errors := make(chan error, numThreads)
		threadDurations := make(chan time.Duration, numThreads)

		// Worker function - each thread writes to its own region
		worker := func(threadID int) {
			defer wg.Done()

			// Calculate this thread's region in the file
			threadOffset := int64(threadID * bytesPerThread)

			// Open file for writing (each thread gets its own handle)
			file, err := os.OpenFile(testFile, os.O_WRONLY, 0644)
			if err != nil {
				errors <- fmt.Errorf("Thread %d: failed to open file: %v", threadID, err)
				return
			}
			defer file.Close()

			// Wait for all threads to be ready
			<-startBarrier

			threadStartTime := time.Now()

			// Prepare chunk filled with thread ID
			chunk := make([]byte, chunkSize)
			for i := 0; i < chunkSize; i++ {
				chunk[i] = byte(threadID + '0') // Convert thread ID to ASCII digit
			}

			// Seek to thread's region
			_, err = file.Seek(threadOffset, 0)
			if err != nil {
				errors <- fmt.Errorf("Thread %d: seek failed: %v", threadID, err)
				return
			}

			// Write data in chunks with periodic syncs
			bytesWritten := 0
			bytesSinceLastSync := 0
			syncCount := 0
			numChunks := bytesPerThread / chunkSize

			for chunkNum := 0; chunkNum < numChunks; chunkNum++ {
				n, err := file.Write(chunk)
				if err != nil {
					errors <- fmt.Errorf("Thread %d: write failed at chunk %d: %v", threadID, chunkNum, err)
					return
				}
				if n != chunkSize {
					errors <- fmt.Errorf("Thread %d: incomplete write at chunk %d: %d/%d bytes", threadID, chunkNum, n, chunkSize)
					return
				}

				bytesWritten += n
				bytesSinceLastSync += n

				// Sync after every syncEveryBytes (only if useSync is true)
				if useSync && bytesSinceLastSync >= syncEveryBytes {
					startOp := time.Now()
					err = file.Sync()
					opTime := time.Since(startOp)
					if err != nil {
						errors <- fmt.Errorf("Thread %d: Sync failed after %d MB: %v", threadID, bytesWritten/(1024*1024), err)
						return
					}
					syncCount++
					logger.Info(fmt.Sprintf("Thread %d: Sync after %d MB (#%d) in %v",
						threadID, bytesWritten/(1024*1024), syncCount, opTime), nil)
					bytesSinceLastSync = 0
				}
			}

			// Final sync if useSync is true and there's unsynced data
			if useSync && bytesSinceLastSync > 0 {
				startOp := time.Now()
				err = file.Sync()
				opTime := time.Since(startOp)
				if err != nil {
					errors <- fmt.Errorf("Thread %d: final Sync failed: %v", threadID, err)
					return
				}
				syncCount++
				logger.Info(fmt.Sprintf("Thread %d: Final Sync (#%d) in %v", threadID, syncCount, opTime), nil)
			}

			threadDuration := time.Since(threadStartTime)
			threadDurations <- threadDuration

			operationType := "syncs"
			if !useSync {
				operationType = "no syncs (flush on close)"
			}
			logger.Info(fmt.Sprintf("Thread %d: Completed %d MB in %v (%.2f MB/s, %d %s)",
				threadID, bytesWritten/(1024*1024), threadDuration,
				float64(bytesWritten)/1024/1024/threadDuration.Seconds(), syncCount, operationType), nil)
		}

		// Start all worker threads
		for i := 0; i < numThreads; i++ {
			go worker(i)
		}

		// Signal all threads to start and measure total time
		logger.Info("Starting concurrent writes...", nil)
		testStartTime := time.Now()
		close(startBarrier)

		// Wait for all threads to complete
		wg.Wait()
		totalWallTime := time.Since(testStartTime)
		close(errors)
		close(threadDurations)

		// Collect per-thread durations
		var threadTimes []time.Duration
		for duration := range threadDurations {
			threadTimes = append(threadTimes, duration)
		}

		// Log timing summary
		logger.Info("=== Timing Summary ===", nil)
		logger.Info(fmt.Sprintf("Total wall time: %v (%.2f MB/s)",
			totalWallTime, float64(totalFileSize)/1024/1024/totalWallTime.Seconds()), nil)
		for i, duration := range threadTimes {
			logger.Info(fmt.Sprintf("Thread %d time: %v", i, duration), nil)
		}

		// Check for errors
		for err := range errors {
			t.Error(err)
		}
		if t.Failed() {
			return
		}

		// Verify file size
		fileInfo, err := os.Stat(testFile)
		if err != nil {
			t.Fatalf("Failed to stat file: %v", err)
		}
		if fileInfo.Size() != totalFileSize {
			t.Errorf("File size mismatch! Expected %d bytes, got %d bytes",
				totalFileSize, fileInfo.Size())
			return
		}

		logger.Info("=== Verifying file contents ===", nil)

		// Read back and verify each thread's region
		file, err := os.Open(testFile)
		if err != nil {
			t.Fatalf("Failed to open file for verification: %v", err)
		}
		defer file.Close()

		// Read and verify each thread's region
		verifyBuffer := make([]byte, chunkSize)
		for threadID := 0; threadID < numThreads; threadID++ {
			expectedByte := byte(threadID + '0')
			threadOffset := int64(threadID * bytesPerThread)

			// Seek to thread's region
			_, err := file.Seek(threadOffset, 0)
			if err != nil {
				t.Fatalf("Thread %d region: seek failed: %v", threadID, err)
			}

			// Verify all bytes in this thread's region
			bytesVerified := 0
			errorCount := 0
			numChunks := bytesPerThread / chunkSize

			for chunkNum := 0; chunkNum < numChunks; chunkNum++ {
				n, err := file.Read(verifyBuffer)
				if err != nil {
					t.Fatalf("Thread %d region: read failed at chunk %d: %v", threadID, chunkNum, err)
				}
				if n != chunkSize {
					t.Errorf("Thread %d region: incomplete read at chunk %d: %d/%d bytes", threadID, chunkNum, n, chunkSize)
					break
				}

				// Verify all bytes match thread ID
				for i := 0; i < n; i++ {
					if verifyBuffer[i] != expectedByte {
						errorCount++
						if errorCount <= 5 { // Only log first few errors
							t.Errorf("Thread %d region: byte %d (global offset %d) has wrong value: expected '%c', got '%c'",
								threadID, bytesVerified+i, threadOffset+int64(bytesVerified+i), expectedByte, verifyBuffer[i])
						}
					}
				}

				bytesVerified += n
			}

			if errorCount == 0 {
				logger.Info(fmt.Sprintf("Thread %d region: verified %d MB correctly",
					threadID, bytesVerified/(1024*1024)), nil)
			} else {
				t.Errorf("Thread %d region: found %d corrupted bytes out of %d", threadID, errorCount, bytesVerified)
			}
		}

		if !t.Failed() {
			logger.Info("=== Test completed successfully ===", nil)
		}
	})
}
