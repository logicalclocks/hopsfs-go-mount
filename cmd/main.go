// Copyright (c) Microsoft. All rights reserved.
// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package main

import (
	"encoding/json"
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
	virtualDirectories, err := buildVirtualDirectories()
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
			//TODO: before doing that we need to finish deferred flushes
			logger.Info(fmt.Sprintf("Received signal: %s", x.String()), nil)
			if hopsfsmount.StagingCache != nil {
				hopsfsmount.StagingCache.Shutdown()
			}
			fileSystem.Unmount(mountPoint) // this will cause Serve() call below to exit
			// Also reseting retry policy properties to stop useless retries
			retryPolicy.MaxAttempts = 0
			retryPolicy.MaxDelay = 0
		}
	}()
	err = fs.Serve(c, fileSystem)
	if err != nil {
		logger.Fatal(fmt.Sprintf("Failed to serve FS. Error: %v", err), nil)
	}
}

func splitCSV(raw string) []string {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	return result
}

func buildVirtualDirectories() ([]hopsfsmount.VirtualDirectoryConfig, error) {
	return parseVirtualDirectoriesSpec(hopsfsmount.VirtualDirectoriesSpec)
}

func parseVirtualDirectoriesSpec(raw string) ([]hopsfsmount.VirtualDirectoryConfig, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}

	if strings.HasPrefix(raw, "[") || strings.HasPrefix(raw, "{") {
		return parseVirtualDirectoriesJSON(raw)
	}

	configs := make([]hopsfsmount.VirtualDirectoryConfig, 0)
	for _, entry := range strings.Split(raw, ";") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		config, err := parseVirtualDirectorySpecEntry(entry)
		if err != nil {
			return nil, err
		}
		configs = append(configs, config)
	}
	return configs, nil
}

func parseVirtualDirectorySpecEntry(entry string) (hopsfsmount.VirtualDirectoryConfig, error) {
	parts := strings.SplitN(entry, ":", 2)
	if len(parts) != 2 {
		return hopsfsmount.VirtualDirectoryConfig{}, fmt.Errorf("invalid virtual directory spec %q: expected <name>:<backend-dirs>", entry)
	}
	name := strings.TrimSpace(parts[0])
	if name == "" {
		return hopsfsmount.VirtualDirectoryConfig{}, fmt.Errorf("invalid virtual directory spec %q: virtual directory name is required", entry)
	}
	rawDirs := splitCSV(parts[1])
	if len(rawDirs) == 0 {
		return hopsfsmount.VirtualDirectoryConfig{}, fmt.Errorf("invalid virtual directory spec %q: at least one backend directory is required", entry)
	}

	hasAbsolute := false
	hasRelative := false
	normalizedPaths := make([]string, 0, len(rawDirs))
	for _, rawDir := range rawDirs {
		rawDir = strings.TrimSpace(rawDir)
		if rawDir == "" {
			continue
		}
		if strings.HasPrefix(rawDir, "/") {
			hasAbsolute = true
		} else {
			hasRelative = true
		}
		normalized, err := normalizeVirtualDirectorySpecPath(rawDir)
		if err != nil {
			return hopsfsmount.VirtualDirectoryConfig{}, err
		}
		if normalized == "" {
			continue
		}
		normalizedPaths = append(normalizedPaths, normalized)
	}
	if len(normalizedPaths) == 0 {
		return hopsfsmount.VirtualDirectoryConfig{}, fmt.Errorf("invalid virtual directory spec %q: at least one backend directory is required", entry)
	}
	if hasAbsolute && hasRelative {
		return hopsfsmount.VirtualDirectoryConfig{}, fmt.Errorf("invalid virtual directory spec %q: do not mix absolute and relative backend directories", entry)
	}

	backendRoot := "/Projects"
	if hasAbsolute {
		backendRoot = "/"
	}
	return hopsfsmount.VirtualDirectoryConfig{
		Name:        name,
		Paths:       normalizedPaths,
		BackendRoot: backendRoot,
	}, nil
}

func normalizeVirtualDirectorySpecPath(rawPath string) (string, error) {
	rawPath = strings.TrimSpace(rawPath)
	if rawPath == "" {
		return "", nil
	}

	trimmed := strings.Trim(rawPath, "/")
	if trimmed == "" {
		return "", nil
	}

	parts := strings.Split(trimmed, "/")
	normalizedParts := make([]string, 0, len(parts))
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return "", fmt.Errorf("invalid virtual directory path %q: path elements must not be empty, . or ..", rawPath)
		}
		normalizedParts = append(normalizedParts, part)
	}

	return strings.Join(normalizedParts, "/"), nil
}

func parseVirtualDirectoriesJSON(raw string) ([]hopsfsmount.VirtualDirectoryConfig, error) {
	var configs []hopsfsmount.VirtualDirectoryConfig
	if err := json.Unmarshal([]byte(raw), &configs); err != nil {
		var payload struct {
			VirtualDirectories []hopsfsmount.VirtualDirectoryConfig `json:"virtualDirectories"`
		}
		if err2 := json.Unmarshal([]byte(raw), &payload); err2 != nil {
			var single hopsfsmount.VirtualDirectoryConfig
			if err3 := json.Unmarshal([]byte(raw), &single); err3 != nil {
				return nil, err
			}
			if single.Name == "" {
				return nil, fmt.Errorf("virtual directory config must include a non-empty name")
			}
			return []hopsfsmount.VirtualDirectoryConfig{single}, nil
		}
		return payload.VirtualDirectories, nil
	}
	return configs, nil
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
