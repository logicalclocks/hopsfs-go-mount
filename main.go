// Copyright (c) Microsoft. All rights reserved.
// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	_ "bazil.org/fuse/fs/fstestutil"
)

var stagingDir string = "/tmp"
var mntSrcDir string = "/"
var logFile string = ""
var logLevel string = "info"
var rootCABundle string = "/srv/hops/super_crypto/hdfs/hops_root_ca.pem"
var clientCertificate string = "/srv/hops/super_crypto/hdfs/hdfs_certificate_bundle.pem"
var clientKey string = "/srv/hops/super_crypto/hdfs/hdfs_priv.pem"
var lazyMount bool = false
var allowedPrefixesString string = "*"
var readOnly bool = false
var tls bool = false
var connectors int
var version bool = false
var forceOverrideUsername string = ""
var useGroupFromHopsFsDatasetPath bool = false
var allowOther bool = false
var hopfsProjectDatasetGroupRegex = regexp.MustCompile(`/*Projects/(?P<projectName>\w+)/(?P<datasetName>\w+)/\/*`)

func main() {

	retryPolicy := NewDefaultRetryPolicy(WallClock{})
	parseArgsAndInitLogger(retryPolicy)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	hopsRpcAddress := flag.Arg(0)
	mountPoint := flag.Arg(1)
	createStagingDir()

	allowedPrefixes := strings.Split(allowedPrefixesString, ",")

	tlsConfig := TLSConfig{
		TLS:               tls,
		RootCABundle:      rootCABundle,
		ClientCertificate: clientCertificate,
		ClientKey:         clientKey,
	}

	ftHdfsAccessors := make([]HdfsAccessor, connectors)

	for i := 0; i < connectors; i++ {
		hdfsAccessor, err := NewHdfsAccessor(hopsRpcAddress, WallClock{}, tlsConfig)
		if err != nil {
			logfatal(fmt.Sprintf("Error/NewHopsFSAccessor: %v ", err), nil)
		}
		ftHdfsAccessors[i] = NewFaultTolerantHdfsAccessor(hdfsAccessor, retryPolicy)
	}
	loginfo(fmt.Sprintf("Create %d file system clients", len(ftHdfsAccessors)), nil)

	if strings.Compare(mntSrcDir, "/") != 0 {
		err := checkSrcMountPath(ftHdfsAccessors[0])
		if err != nil {
			logfatal(fmt.Sprintf("Unable to mount the file system as source mount directory is not accessible. Error: %v ", err), nil)
		}
	}

	// Wrapping with FaultTolerantHdfsAccessor

	if !lazyMount && ftHdfsAccessors[0].EnsureConnected() != nil {
		logfatal("Can't establish connection to HopsFS, mounting will NOT be performend (this can be suppressed with -lazy", nil)
	}

	// Creating the virtual file system
	fileSystem, err := NewFileSystem(ftHdfsAccessors, mntSrcDir, allowedPrefixes, readOnly, retryPolicy, WallClock{})
	if err != nil {
		logfatal(fmt.Sprintf("Error/NewFileSystem: %v ", err), nil)
	}

	mountOptions := getMountOptions(readOnly)
	c, err := fileSystem.Mount(mountPoint, mountOptions...)
	if err != nil {
		logfatal(fmt.Sprintf("Failed to mount FS. Error: %v", err), nil)
	}
	loginfo(fmt.Sprintf("Mounted successfully. HopsFS src dir: %s ", mntSrcDir), nil)

	// Increase the maximum number of file descriptor from 1K to 1M in Linux
	rLimit := syscall.Rlimit{
		Cur: 1024 * 1024,
		Max: 1024 * 1024}
	err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		logerror(fmt.Sprintf("Failed to update the maximum number of file descriptors from 1K to 1M, %v", err), Fields{})
	}

	defer func() {
		fileSystem.Unmount(mountPoint)
		loginfo("Closing...", nil)
		c.Close()
		loginfo("Closed...", nil)
	}()

	go func() {
		for x := range sigs {
			//Handling INT/TERM signals - trying to gracefully unmount and exit
			//TODO: before doing that we need to finish deferred flushes
			loginfo(fmt.Sprintf("Received signal: %s", x.String()), nil)
			fileSystem.Unmount(mountPoint) // this will cause Serve() call below to exit
			// Also reseting retry policy properties to stop useless retries
			retryPolicy.MaxAttempts = 0
			retryPolicy.MaxDelay = 0
		}
	}()
	err = fs.Serve(c, fileSystem)
	if err != nil {
		logfatal(fmt.Sprintf("Failed to serve FS. Error: %v", err), nil)
	}

	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		logfatal(fmt.Sprintf("Mount process had errors: %v", err), nil)
	}
}

var Usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s [Options] Namenode:Port MountPoint\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  \nOptions:\n")
	flag.PrintDefaults()
}

func parseArgsAndInitLogger(retryPolicy *RetryPolicy) {
	lazyMount = *flag.Bool("lazy", false, "Allows to mount HopsFS filesystem before HopsFS is available")
	flag.DurationVar(&retryPolicy.TimeLimit, "retryTimeLimit", 5*time.Minute, "time limit for all retry attempts for failed operations")
	flag.IntVar(&retryPolicy.MaxAttempts, "retryMaxAttempts", 10, "Maxumum retry attempts for failed operations")
	flag.DurationVar(&retryPolicy.MinDelay, "retryMinDelay", 1*time.Second, "minimum delay between retries (note, first retry always happens immediatelly)")
	flag.DurationVar(&retryPolicy.MaxDelay, "retryMaxDelay", 60*time.Second, "maximum delay between retries")
	allowedPrefixesString = *flag.String("allowedPrefixes", "*", "Comma-separated list of allowed path prefixes on the remote file system, if specified the mount point will expose access to those prefixes only")
	readOnly = *flag.Bool("readOnly", false, "Enables mount with readonly")
	flag.StringVar(&logLevel, "logLevel", "info", "logs to be printed. error, warn, info, debug, trace")
	flag.StringVar(&stagingDir, "stageDir", "/tmp", "stage directory for writing files")
	tls = *flag.Bool("tls", false, "Enables tls connections")
	flag.StringVar(&rootCABundle, "rootCABundle", "/srv/hops/super_crypto/hdfs/hops_root_ca.pem", "Root CA bundle location ")
	flag.StringVar(&clientCertificate, "clientCertificate", "/srv/hops/super_crypto/hdfs/hdfs_certificate_bundle.pem", "Client certificate location")
	flag.StringVar(&clientKey, "clientKey", "/srv/hops/super_crypto/hdfs/hdfs_priv.pem", "Client key location")
	flag.StringVar(&mntSrcDir, "srcDir", "/", "HopsFS src directory")
	flag.StringVar(&logFile, "logFile", "", "Log file path. By default the log is written to console")
	flag.IntVar(&connectors, "numConnections", 1, "Number of connections with the namenode")
	flag.StringVar(&forceOverrideUsername, "hopsFSUserName", "", " username")
	useGroupFromHopsFsDatasetPath = *flag.Bool("getGroupFromHopsFSDatasetPath", false, "Get the group from hopsfs dataset path. This will work if a hopsworks project is mounted")
	allowOther = *flag.Bool("allowOther", true, "Allow other users to use the filesystem")
	version = *flag.Bool("version", false, "Print version")

	flag.Usage = Usage
	flag.Parse()

	if version {
		fmt.Printf("Version: %s\n", VERSION)
		fmt.Printf("Git commit: %s\n", GITCOMMIT)
		fmt.Printf("Date: %s\n", BUILDTIME)
		fmt.Printf("Host: %s\n", HOSTNAME)
		os.Exit(0)
	}

	if flag.NArg() != 2 {
		Usage()
		os.Exit(2)
	}

	if err := checkLogFileCreation(); err != nil {
		log.Fatalf("Error creating log file. Error: %v", err)
	}
	initLogger(logLevel, false, logFile)

	loginfo(fmt.Sprintf("Staging dir is:%s, Using TLS: %v, RetryAttempts: %d,  LogFile: %s", stagingDir, tls, retryPolicy.MaxAttempts, logFile), nil)
	loginfo(fmt.Sprintf("hopsfs-mount: current head GITCommit: %s Built time: %s Built by: %s ", GITCOMMIT, BUILDTIME, HOSTNAME), nil)
}

// check that we can create / open the log file
func checkLogFileCreation() error {
	if logFile != "" {
		if _, err := os.Stat(logFile); err == nil {
			// file exists. check if it is writeable
			if f, err := os.OpenFile(logFile, os.O_RDWR|os.O_APPEND, 0600); err != nil {
				return err
			} else {
				f.Close()
			}
		} else if os.IsNotExist(err) {
			// check if we can create the log file
			if f, err := os.OpenFile(logFile, os.O_RDWR|os.O_CREATE, 0600); err != nil {
				return err
			} else {
				f.Close()
			}
		} else {
			// Schrodinger: file may or may not exist. See err for details.
			return err
		}
	}
	return nil
}

func getMountOptions(ro bool) []fuse.MountOption {
	mountOptions := []fuse.MountOption{fuse.FSName("hopsfs"),
		fuse.Subtype("hopsfs"),
		fuse.WritebackCache(),
		// write to kernel cache, improves performance for small writes
		fuse.MaxReadahead(1024 * 64), //TODO: make configurable
		fuse.DefaultPermissions(),
	}

	if allowOther {
		mountOptions = append(mountOptions, fuse.AllowOther())
	}

	if ro {
		mountOptions = append(mountOptions, fuse.ReadOnly())
	}
	return mountOptions
}

func createStagingDir() {
	if err := os.MkdirAll(stagingDir, 0700); err != nil {
		logerror(fmt.Sprintf("Failed to create stageDir: %s. Error: %v", stagingDir, err), Fields{})
	}
}

func checkSrcMountPath(hdfsAccessor HdfsAccessor) error {
	_, err := hdfsAccessor.Stat(mntSrcDir)
	if err != nil {
		return err
	}
	return nil
}
