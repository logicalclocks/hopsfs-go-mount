// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

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
	"testing"
	"time"

	"bazil.org/fuse/fs/fstestutil"
	"github.com/colinmarc/hdfs/v2"
	"logicalclocks.com/hopsfs-mount/ugcache"
)

func TestReadWriteEmptyFile(t *testing.T) {

	withMount(t, "/", func(mountPoint string, hdfsAccessor HdfsAccessor) {
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
		logdebug("Done", nil)
	})
}

func TestSimple(t *testing.T) {

	withMount(t, "/", func(mountPoint string, hdfsAccessor HdfsAccessor) {
		for i := 0; i < 3; i++ {
			testFile := filepath.Join(mountPoint, fmt.Sprintf("somefile_%d", i))
			os.Remove(testFile)
			loginfo(fmt.Sprintf("New file: %s", testFile), nil)
			createFile(t, testFile, "some data")
			os.Remove(testFile)
		}
	})
}

func TestTruncate(t *testing.T) {

	withMount(t, "/", func(mountPoint string, hdfsAccessor HdfsAccessor) {
		//create a file, make sure that use and group information is correct
		testFile := filepath.Join(mountPoint, "somefile")
		os.Remove(testFile)

		loginfo(fmt.Sprintf("New file: %s", testFile), nil)
		data1 := "123456790"
		data2 := "abcde"
		createFile(t, testFile, data1)
		fi, _ := os.Stat(testFile)
		fileSize := fi.Size()

		if int(fileSize) != len(data1) {
			t.Errorf("Invalid file size. Expecting: %d Got: %d", len(data1), fileSize)
		}

		createFile(t, testFile, data2) // truncates if file already exists
		fi, _ = os.Stat(testFile)
		fileSize = fi.Size()

		if int(fileSize) != len(data2) {
			t.Errorf("Invalid file size. Expecting: %d Got: %d", len(data2), fileSize)
		}

		os.Remove(testFile)
	})
}

func TestTruncateGreaterLength(t *testing.T) {

	withMount(t, "/", func(mountPoint string, hdfsAccessor HdfsAccessor) {
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
		logdebug("Done", nil)
	})
}

// testing multiple read write clients perfile
func TestMultipleRWCllients(t *testing.T) {

	withMount(t, "/", func(mountPoint string, hdfsAccessor HdfsAccessor) {
		//create a file, make sure that use and group information is correct
		// mountPoint = "/tmp"
		testFile1 := filepath.Join(mountPoint, "somefile")
		testFile2 := filepath.Join(mountPoint, "somefile.bak")
		loginfo(fmt.Sprintf("New file: %s", testFile1), nil)
		createFile(t, testFile1, "initial data\nadsf\n")

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
			loginfo("File opened successfully", nil)
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
	withMount(t, "/", func(mountPoint string, hdfsAccessor HdfsAccessor) {
		//create some directories and files
		for i := 0; i < dirs; i++ {
			dir := filepath.Join(mountPoint, "dir"+strconv.Itoa(i))
			mkdir(t, dir)
			for j := 0; j < filesPdir; j++ {
				f := filepath.Join(dir, "file"+strconv.Itoa(j))
				createFile(t, f, "initial data")
			}
		}

		content := listDir(t, mountPoint)
		if len(content) < dirs {
			t.Errorf("Failed. Expected >= %d, Got %d", dirs, len(content))
		}
	})

	// remount only one dir
	withMount(t, "/dir1", func(mountPoint string, hdfsAccessor HdfsAccessor) {
		content := listDir(t, mountPoint)
		if len(content) != filesPdir {
			t.Errorf("Failed. Expected == %d, Got %d ", filesPdir, len(content))
			for _, ent := range content {
				loginfo(fmt.Sprintf("file/dir %s", ent.Name()), nil)
			}
		}
	})

	// remount every thing for cleanup
	withMount(t, "/", func(mountPoint string, hdfsAccessor HdfsAccessor) {
		//delete all the files and dirs created in this test
		for i := 0; i < dirs; i++ {
			dir := filepath.Join(mountPoint, "dir"+strconv.Itoa(i))
			for j := 0; j < filesPdir; j++ {
				f := filepath.Join(dir, "file"+strconv.Itoa(j))
				rmFile(t, f)
			}
			rmFile(t, dir)
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
	fmt.Printf("Connected to DFS\n")

	prepare(t, client, dataSize, diskSeekTestFile, dfsSeekTestFile)

	testSeeks(t, client, diskSeekTestFile, dfsSeekTestFile)

}

func prepare(t *testing.T, client *hdfs.Client, dataSize int, diskTestFile string, dfsTestFile string) {

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
		dfsWriter, err := client.Create(dfsTestFile)
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
	for i := 0; i < 100000; i++ {

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

func withMount(t testing.TB, srcDir string, fn func(mntPath string, hdfsAccessor HdfsAccessor)) {
	t.Helper()

	// Wrapping with FaultTolerantHdfsAccessor
	retryPolicy := NewDefaultRetryPolicy(WallClock{})
	retryPolicy.MaxAttempts = 1 // for quick failure
	initLogger("info", false, "")
	hdfsAccessor, _ := NewHdfsAccessor("localhost:8020", WallClock{}, TLSConfig{TLS: false, RootCABundle: rootCABundle, ClientCertificate: clientCertificate, ClientKey: clientKey})
	err := hdfsAccessor.EnsureConnected()
	if err != nil {
		t.Fatalf(fmt.Sprintf("Error/NewHdfsAccessor: %v ", err), nil)
	}

	ftHdfsAccessor := NewFaultTolerantHdfsAccessor(hdfsAccessor, retryPolicy)

	// Creating the virtual file system
	fileSystem, err := NewFileSystem([]HdfsAccessor{ftHdfsAccessor}, srcDir, []string{"*"}, false, retryPolicy, WallClock{})
	if err != nil {
		t.Fatalf(fmt.Sprintf("Error/NewFileSystem: %v ", err), nil)
	}

	mountOptions := getMountOptions(false)
	mnt, err := fstestutil.MountedT(t, fileSystem, nil, mountOptions...)
	if err != nil {
		t.Fatal(fmt.Sprintf("Unable to mount fs: Error %v", err), nil)

	}
	defer mnt.Close()
	loginfo(fmt.Sprintf("Connected to HopsFS. Mount point is %s", mnt.Dir), nil)
	fn(mnt.Dir, hdfsAccessor)
}

func mkdir(t testing.TB, dir string) {
	t.Helper()
	err := os.Mkdir(dir, 0700)
	if err != nil {
		t.Errorf("Faile to create directory %s. Error: %v", dir, err)
	}

}

func createFile(t testing.TB, filePath string, data string) {
	t.Helper()
	out, err := os.Create(filePath)
	if err != nil {
		t.Errorf("Faile to create test file %s. Error: %v", filePath, err)
	}
	out.WriteString(data)
	out.Close()
}

func listDir(t testing.TB, dir string) []fs.FileInfo {
	t.Helper()
	content, err := ioutil.ReadDir(dir)
	if err != nil {
		t.Errorf("Faile to list dir %s. Error: %v", dir, err)
	}
	return content
}

func rmFile(t testing.TB, path string) {
	t.Helper()
	err := os.Remove(path)
	if err != nil {
		t.Errorf("Faile to remove  %s. Error: %v", path, err)
	}
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
