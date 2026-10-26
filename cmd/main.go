// Copyright (c) Microsoft. All rights reserved.
// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"bazil.org/fuse/fs"
	_ "bazil.org/fuse/fs/fstestutil"
	"hopsworks.ai/hopsfsmount/internal/hopsfsmount"
	"hopsworks.ai/hopsfsmount/internal/hopsfsmount/logger"
)

func main() {
	retryPolicy := hopsfsmount.NewDefaultRetryPolicy(hopsfsmount.WallClock{})
	hopsfsmount.ParseArgsAndInitLogger(retryPolicy)

	// Initialize connection username once at startup
	if err := hopsfsmount.InitConnectionUser(); err != nil {
		logger.Fatal(fmt.Sprintf("Failed to initialize connection user: %v", err), nil)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	hopsRpcAddress := flag.Arg(0)
	mountPoint := flag.Arg(1)
	createStagingDir()

	allowedPrefixes := strings.Split(hopsfsmount.AllowedPrefixesString, ",")

	tlsConfig := hopsfsmount.TLSConfig{
		TLS:               hopsfsmount.Tls,
		RootCABundle:      hopsfsmount.RootCABundle,
		ClientCertificate: hopsfsmount.ClientCertificate,
		ClientKey:         hopsfsmount.ClientKey,
	}

	ftHdfsAccessors := make([]hopsfsmount.HdfsAccessor, hopsfsmount.Connectors)

	for i := 0; i < hopsfsmount.Connectors; i++ {
		hdfsAccessor, err := hopsfsmount.NewHdfsAccessor(hopsRpcAddress, hopsfsmount.WallClock{}, tlsConfig)
		if err != nil {
			logger.Fatal(fmt.Sprintf("Error/NewHopsFSAccessor: %v ", err), nil)
		}
		ftHdfsAccessors[i] = hopsfsmount.NewFaultTolerantHdfsAccessor(hdfsAccessor, retryPolicy)
	}
	logger.Info(fmt.Sprintf("Create %d file system clients", len(ftHdfsAccessors)), nil)

	if strings.Compare(hopsfsmount.MntSrcDir, "/") != 0 {
		err := checkSrcMountPath(ftHdfsAccessors[0])
		if err != nil {
			logger.Fatal(fmt.Sprintf("Unable to mount the file system as source mount directory is not accessible. Error: %v ", err), nil)
		}
	}

	// Wrapping with FaultTolerantHdfsAccessor

	if !hopsfsmount.LazyMount && ftHdfsAccessors[0].EnsureConnected() != nil {
		logger.Fatal("Can't establish connection to HopsFS, mounting will NOT be performend (this can be suppressed with -lazy", nil)
	}

	// Creating the virtual file system
	virtualDirectories, err := hopsfsmount.ParseVirtualDirectoriesSpec(hopsfsmount.VirtualDirectoriesSpec)
	if err != nil {
		logger.Fatal(fmt.Sprintf("Error parsing virtual directory config: %v", err), nil)
	}
	var virtualDirectoryOption hopsfsmount.FileSystemOption
	if len(virtualDirectories) > 0 {
		virtualDirectoryOption = hopsfsmount.WithVirtualDirectories(virtualDirectories)
	}

	fileSystem, err := hopsfsmount.NewFileSystem(ftHdfsAccessors, hopsfsmount.MntSrcDir, allowedPrefixes, hopsfsmount.ReadOnly, hopsfsmount.DelaySyncUntilClose, retryPolicy, hopsfsmount.WallClock{},
		virtualDirectoryOption)
	if err != nil {
		logger.Fatal(fmt.Sprintf("Error/NewFileSystem: %v ", err), nil)
	}

	mountOptions := hopsfsmount.GetMountOptions(hopsfsmount.ReadOnly)
	c, err := fileSystem.Mount(mountPoint, mountOptions...)
	if err != nil {
		logger.Fatal(fmt.Sprintf("Failed to mount FS. Error: %v", err), nil)
	}
	logger.Info(fmt.Sprintf("Mounted successfully. HopsFS src dir: %s ", hopsfsmount.MntSrcDir), nil)

	// Increase the maximum number of file descriptor from 1K to 1M in Linux
	rLimit := syscall.Rlimit{
		Cur: 1024 * 1024,
		Max: 1024 * 1024}
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit); err != nil {
		logger.Error(fmt.Sprintf("Failed to increase file descriptor limit: %v", err), logger.Fields{})
	}
	hopsfsmount.InitStagingFileCache()

	if hopsfsmount.ControlSocket != "" {
		l, err := hopsfsmount.ServeControlSocket(fileSystem, hopsfsmount.ControlSocket)
		if err != nil {
			logger.Fatal(fmt.Sprintf("Failed to listen on control socket %s: %v", hopsfsmount.ControlSocket, err), nil)
		}
		defer l.Close()
	}

	defer func() {
		if hopsfsmount.StagingCache != nil {
			hopsfsmount.StagingCache.Shutdown()
		}
		fileSystem.Unmount(mountPoint)
		logger.Info("Closing...", nil)
		c.Close()
		logger.Info("Closed...", nil)
	}()

	go func() {
		for x := range sigs {
			//Handling INT/TERM signals - trying to gracefully unmount and exit
			logger.Info(fmt.Sprintf("Received signal: %s", x.String()), nil)
			var unresolved []hopsfsmount.Unresolved
			if fileSystem.WB != nil {
				// Writes fenced, open files queued, uploads drained within stopDrainTimeout;
				// what did not land is logged at ERROR with its path.
				unresolved, _ = fileSystem.WB.Stop(hopsfsmount.StopDrainTimeout)
			}
			if hopsfsmount.StagingCache != nil {
				hopsfsmount.StagingCache.Shutdown()
			}
			fileSystem.Unmount(mountPoint) // this will cause Serve() call below to exit
			// Also reseting retry policy properties to stop useless retries
			retryPolicy.MaxAttempts = 0
			retryPolicy.MaxDelay = 0
			if fileSystem.WB != nil {
				// A lazy unmount keeps serving while a process holds a file open, so Serve would
				// not return; with the writes fenced and the uploads drained there is nothing left
				// to serve. Conn.Close would wait on the blocked request reader: the exit closes
				// /dev/fuse and the kernel ends the connection. The exit code says whether every
				// upload landed.
				if len(unresolved) > 0 {
					os.Exit(1)
				}
				os.Exit(0)
			}
		}
	}()
	err = fs.Serve(c, fileSystem)
	if err != nil {
		logger.Fatal(fmt.Sprintf("Failed to serve FS. Error: %v", err), nil)
	}
}

func createStagingDir() {
	if err := os.MkdirAll(hopsfsmount.StagingDir, 0700); err != nil {
		logger.Error(fmt.Sprintf("Failed to create stageDir: %s. Error: %v", hopsfsmount.StagingDir, err), logger.Fields{})
	}
}

func checkSrcMountPath(hdfsAccessor hopsfsmount.HdfsAccessor) error {
	_, err := hdfsAccessor.Stat(hopsfsmount.MntSrcDir)
	if err != nil {
		return err
	}
	return nil
}
