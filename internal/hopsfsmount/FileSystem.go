// Copyright (c) Microsoft. All rights reserved.
// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package hopsfsmount

import (
	"fmt"
	"path"
	"runtime"
	"sort"
	"strconv"
	"strings"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"hopsworks.ai/hopsfsmount/internal/hopsfsmount/logger"

	"golang.org/x/net/context"

	"io"
	"os"
	"os/exec"
	"os/user"
	"sync"
)

type FileSystem struct {
	HdfsAccessors       []HdfsAccessor // Interface to access HDFS
	hdfsAccessorsIndex  int
	SrcDir              string   // Src directory that will mounted
	AllowedPrefixes     []string // List of allowed path prefixes (only those prefixes are exposed via mountpoint)
	VirtualDirectories  []VirtualDirectoryConfig
	ReadOnly            bool // Indicates whether mount filesystem with readonly
	DelaySyncUntilClose bool // If true, ignore sync/flush operations until file close
	Mounted             bool // True if filesystem is mounted
	RetryPolicy         *RetryPolicy
	Clock               Clock  // interface to get wall clock time
	FsInfo              FsInfo // Usage of HDFS, including capacity, remaining, used sizes.

	closeOnUnmount            []io.Closer // list of opened files (zip archives) to be closed on unmount
	closeOnUnmountLock        sync.Mutex  // mutex to protet closeOnUnmount
	virtualRootCollisions     map[string]bool
	virtualRootCollisionsLock sync.RWMutex
}

type VirtualDirectoryConfig struct {
	Name        string   `json:"name"`
	Paths       []string `json:"paths"`
	BackendRoot string   `json:"backendRoot"`
}

// Verify that *FileSystem implements necesary FUSE interfaces
var _ fs.FS = (*FileSystem)(nil)
var _ fs.FSStatfser = (*FileSystem)(nil)

type FileSystemOption func(*FileSystem)

func WithVirtualDirectories(configs []VirtualDirectoryConfig) FileSystemOption {
	return func(filesystem *FileSystem) {
		filesystem.VirtualDirectories = append([]VirtualDirectoryConfig(nil), configs...)
	}
}

func WithVirtualDirectory(name string, paths []string, backendRoot string) FileSystemOption {
	return WithVirtualDirectories([]VirtualDirectoryConfig{{
		Name:        strings.TrimSpace(name),
		Paths:       append([]string(nil), paths...),
		BackendRoot: strings.TrimSpace(backendRoot),
	}})
}

// Creates an instance of mountable file system
func NewFileSystem(hdfsAccessors []HdfsAccessor, srcDir string, allowedPrefixes []string, readOnly bool, delaySyncUntilClose bool, retryPolicy *RetryPolicy, clock Clock, opts ...FileSystemOption) (*FileSystem, error) {
	filesystem := &FileSystem{
		HdfsAccessors:       hdfsAccessors,
		Mounted:             false,
		AllowedPrefixes:     allowedPrefixes,
		ReadOnly:            readOnly,
		DelaySyncUntilClose: delaySyncUntilClose,
		RetryPolicy:         retryPolicy,
		Clock:               clock,
		SrcDir:              srcDir}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(filesystem)
	}
	if err := filesystem.normalizeVirtualDirectoryConfig(); err != nil {
		return nil, err
	}
	return filesystem, nil
}

// Mounts the filesystem
func (filesystem *FileSystem) Mount(mountPoint string, conf ...fuse.MountOption) (*fuse.Conn, error) {
	var conn *fuse.Conn
	var err error
	conn, err = fuse.Mount(
		mountPoint,
		conf...,
	)
	if err != nil {
		return nil, err
	}
	filesystem.Mounted = true
	return conn, nil
}

// Unmounts the filesysten (invokes fusermount tool)
func (filesystem *FileSystem) Unmount(mountPoint string) {
	if !filesystem.Mounted {
		return
	}
	filesystem.Mounted = false
	logger.Info("Unmounting...", nil)
	cmd := exec.Command("fusermount3", "-zu", mountPoint)
	err := cmd.Run()

	// Closing all the files
	filesystem.closeOnUnmountLock.Lock()
	defer filesystem.closeOnUnmountLock.Unlock()
	for _, f := range filesystem.closeOnUnmount {
		f.Close()
	}

	if err != nil {
		logger.Fatal(fmt.Sprintf("Unable to unmount FS. Error: %v", err), nil)
	}
}

// Returns root directory of the filesystem
func (filesystem *FileSystem) Root() (fs.Node, error) {
	//get UID and GID for the current user
	cu, err := user.Current()
	if err != nil {
		logger.Fatal(fmt.Sprintf("Faile to get current user information. Error: %v", err), nil)
	}
	uid64, _ := strconv.ParseUint(cu.Uid, 10, 32)
	gid64, _ := strconv.ParseUint(cu.Gid, 10, 32)

	return newDirINode(filesystem, nil, Attrs{
		Inode: 1,
		Uid:   uint32(uid64),
		Gid:   uint32(gid64),
		Mode:  0755 | os.ModeDir,
		Mtime: filesystem.Clock.Now(),
		Ctime: filesystem.Clock.Now(),
	}), nil
}

// Returns if given absoute path allowed by any of the prefixes
func (filesystem *FileSystem) IsPathAllowed(candidate string) bool {
	if candidate == "/" {
		return true
	}
	for _, prefix := range filesystem.AllowedPrefixes {
		if prefix == "*" {
			return true
		}
		p := "/" + prefix
		if p == candidate || strings.HasPrefix(candidate, p+"/") {
			return true
		}
	}

	for _, virtualDirectory := range filesystem.VirtualDirectories {
		if virtualDirectory.isPathAllowed(candidate) {
			return true
		}
	}
	return false
}

func normalizeVirtualDirectoryName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", nil
	}
	if strings.Contains(name, "/") {
		return "", fmt.Errorf("invalid virtual directory name %q: must be a single path element", name)
	}
	if name == "." || name == ".." {
		return "", fmt.Errorf("invalid virtual directory name %q: must not be . or ..", name)
	}
	return name, nil
}

func normalizeVirtualDirectoryPaths(paths []string) ([]string, error) {
	result := make([]string, 0, len(paths))
	seen := make(map[string]struct{})
	for _, p := range paths {
		normalized, err := normalizeVirtualDirectoryPath(p)
		if err != nil {
			return nil, err
		}
		if normalized == "" {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		result = append(result, normalized)
	}
	sort.Strings(result)
	return result, nil
}

func normalizeVirtualDirectoryPath(rawPath string) (string, error) {
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

func normalizeVirtualDirectoryBackendRoot(rawPath string) (string, error) {
	rawPath = strings.TrimSpace(rawPath)
	if rawPath == "" {
		return "/Projects", nil
	}
	if rawPath == "/" {
		return "/", nil
	}
	if !strings.HasPrefix(rawPath, "/") {
		return "", fmt.Errorf("invalid virtual directory backend root %q: must be an absolute path", rawPath)
	}

	normalized, err := normalizeVirtualDirectoryPath(rawPath)
	if err != nil {
		return "", fmt.Errorf("invalid virtual directory backend root %q: %w", rawPath, err)
	}
	return "/" + normalized, nil
}

func (filesystem *FileSystem) HasVirtualDirectory() bool {
	return len(filesystem.VirtualDirectories) > 0
}

func (filesystem *FileSystem) HasVirtualDirectories() bool {
	return filesystem.HasVirtualDirectory()
}

func (filesystem *FileSystem) setVirtualRootCollisions(collisions map[string]bool) {
	filesystem.virtualRootCollisionsLock.Lock()
	defer filesystem.virtualRootCollisionsLock.Unlock()

	if len(collisions) == 0 {
		filesystem.virtualRootCollisions = nil
		return
	}

	filesystem.virtualRootCollisions = make(map[string]bool, len(collisions))
	for name, collided := range collisions {
		if collided {
			filesystem.virtualRootCollisions[name] = true
		}
	}
}

func (filesystem *FileSystem) virtualRootCollision(name string) bool {
	filesystem.virtualRootCollisionsLock.RLock()
	defer filesystem.virtualRootCollisionsLock.RUnlock()

	if filesystem.virtualRootCollisions == nil {
		return false
	}
	return filesystem.virtualRootCollisions[name]
}

func (filesystem *FileSystem) firstVirtualDirectoryConfig() (VirtualDirectoryConfig, bool) {
	if len(filesystem.VirtualDirectories) == 0 {
		return VirtualDirectoryConfig{}, false
	}
	return filesystem.VirtualDirectories[0], true
}

func (filesystem *FileSystem) virtualDirectoryConfigByName(name string) (VirtualDirectoryConfig, bool) {
	for _, virtualDirectory := range filesystem.VirtualDirectories {
		if virtualDirectory.Name == name {
			return virtualDirectory, true
		}
	}
	return VirtualDirectoryConfig{}, false
}

func (filesystem *FileSystem) normalizeVirtualDirectoryConfig() error {
	normalized, err := normalizeVirtualDirectoryConfigs(filesystem.VirtualDirectories)
	if err != nil {
		return err
	}
	filesystem.VirtualDirectories = normalized
	return nil
}

func normalizeVirtualDirectoryConfigs(configs []VirtualDirectoryConfig) ([]VirtualDirectoryConfig, error) {
	normalized := make([]VirtualDirectoryConfig, 0, len(configs))
	seenNames := make(map[string]struct{})
	for _, config := range configs {
		name, err := normalizeVirtualDirectoryName(config.Name)
		if err != nil {
			return nil, err
		}
		paths, err := normalizeVirtualDirectoryPaths(config.Paths)
		if err != nil {
			return nil, err
		}
		if name == "" || len(paths) == 0 {
			continue
		}
		if _, ok := seenNames[name]; ok {
			return nil, fmt.Errorf("duplicate virtual directory name %q", name)
		}
		backendRoot, err := normalizeVirtualDirectoryBackendRoot(config.BackendRoot)
		if err != nil {
			return nil, err
		}
		normalized = append(normalized, VirtualDirectoryConfig{
			Name:        name,
			Paths:       paths,
			BackendRoot: backendRoot,
		})
		seenNames[name] = struct{}{}
	}
	sort.Slice(normalized, func(i, j int) bool {
		return normalized[i].Name < normalized[j].Name
	})
	return normalized, nil
}

func (filesystem *FileSystem) VirtualDirectoryPath(relPath string) string {
	if virtualDirectory, ok := filesystem.firstVirtualDirectoryConfig(); ok {
		return virtualDirectory.Path(relPath)
	}
	relPath = strings.Trim(relPath, "/")
	if relPath == "" {
		return "/"
	}
	return path.Join("/", relPath)
}

func (filesystem *FileSystem) VirtualDirectoryRootPath() string {
	if virtualDirectory, ok := filesystem.firstVirtualDirectoryConfig(); ok {
		return virtualDirectory.RootPath()
	}
	return ""
}

func (filesystem *FileSystem) virtualDirectoryRelPathExists(relPath string) bool {
	if virtualDirectory, ok := filesystem.firstVirtualDirectoryConfig(); ok {
		return virtualDirectory.relPathExists(relPath)
	}
	return false
}

func (filesystem *FileSystem) virtualDirectoryLeafExists(relPath string) bool {
	if virtualDirectory, ok := filesystem.firstVirtualDirectoryConfig(); ok {
		return virtualDirectory.leafExists(relPath)
	}
	return false
}

func (filesystem *FileSystem) virtualDirectoryPathRelativeToBackendRoot(candidate string) (string, bool) {
	if virtualDirectory, ok := filesystem.firstVirtualDirectoryConfig(); ok {
		return virtualDirectory.pathRelativeToBackendRoot(candidate)
	}
	return "", false
}

func (filesystem *FileSystem) virtualDirectoryMutationAllowed(candidate string) bool {
	if virtualDirectory, ok := filesystem.firstVirtualDirectoryConfig(); ok {
		return virtualDirectory.mutationAllowed(candidate)
	}
	return false
}

func (filesystem *FileSystem) virtualDirectoryChildNames(relPath string) []string {
	if virtualDirectory, ok := filesystem.firstVirtualDirectoryConfig(); ok {
		return virtualDirectory.childNames(relPath)
	}
	return nil
}

func (config VirtualDirectoryConfig) RootPath() string {
	if config.Name == "" {
		return ""
	}
	return path.Join("/", config.Name)
}

func (config VirtualDirectoryConfig) Path(relPath string) string {
	relPath = strings.Trim(relPath, "/")
	if relPath == "" {
		return path.Clean(config.BackendRoot)
	}
	return path.Join(config.BackendRoot, relPath)
}

func (config VirtualDirectoryConfig) relPathExists(relPath string) bool {
	relPath = strings.Trim(relPath, "/")
	if relPath == "" {
		return false
	}
	for _, candidate := range config.Paths {
		if candidate == relPath || strings.HasPrefix(candidate, relPath+"/") {
			return true
		}
	}
	return false
}

func (config VirtualDirectoryConfig) leafExists(relPath string) bool {
	relPath = strings.Trim(relPath, "/")
	for _, candidate := range config.Paths {
		if candidate == relPath {
			return true
		}
	}
	return false
}

func (config VirtualDirectoryConfig) pathRelativeToBackendRoot(candidate string) (string, bool) {
	candidate = strings.TrimSpace(candidate)
	if candidate == "" {
		return "", false
	}

	candidate = path.Clean(candidate)
	backendRoot := path.Clean(config.BackendRoot)

	if backendRoot == "/" {
		rel := strings.TrimPrefix(candidate, "/")
		if rel == "" {
			return "", false
		}
		return rel, true
	}

	if candidate == backendRoot {
		return "", false
	}

	prefix := backendRoot + "/"
	if !strings.HasPrefix(candidate, prefix) {
		return "", false
	}

	rel := strings.TrimPrefix(candidate, prefix)
	if rel == "" {
		return "", false
	}
	return rel, true
}

func (config VirtualDirectoryConfig) mutationAllowed(candidate string) bool {
	relPath, ok := config.pathRelativeToBackendRoot(candidate)
	if !ok {
		return false
	}

	for _, virtualPath := range config.Paths {
		if relPath == virtualPath || strings.HasPrefix(relPath, virtualPath+"/") {
			return true
		}
	}
	return false
}

func (config VirtualDirectoryConfig) isPathAllowed(candidate string) bool {
	if candidate == config.RootPath() {
		return true
	}
	relPath, ok := config.pathRelativeToBackendRoot(candidate)
	if !ok {
		return false
	}

	for _, virtualPath := range config.Paths {
		if relPath == virtualPath || strings.HasPrefix(relPath, virtualPath+"/") {
			return true
		}
	}
	return false
}

func (config VirtualDirectoryConfig) childNames(relPath string) []string {
	relPath = strings.Trim(relPath, "/")
	children := make(map[string]struct{})
	prefix := relPath
	if prefix != "" {
		prefix += "/"
	}
	for _, candidate := range config.Paths {
		if relPath == "" {
			parts := strings.Split(candidate, "/")
			if len(parts) > 0 && parts[0] != "" {
				children[parts[0]] = struct{}{}
			}
			continue
		}
		if !strings.HasPrefix(candidate, prefix) {
			continue
		}
		remainder := strings.TrimPrefix(candidate, prefix)
		if remainder == "" {
			continue
		}
		parts := strings.Split(remainder, "/")
		if len(parts) > 0 && parts[0] != "" {
			children[parts[0]] = struct{}{}
		}
	}

	names := make([]string, 0, len(children))
	for name := range children {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Register a file to be closed on Unmount()
func (filesystem *FileSystem) CloseOnUnmount(file io.Closer) {
	filesystem.closeOnUnmountLock.Lock()
	defer filesystem.closeOnUnmountLock.Unlock()
	filesystem.closeOnUnmount = append(filesystem.closeOnUnmount, file)
}

// Statfs is called to obtain file system metadata.
// It should write that data to resp.
func (filesystem *FileSystem) Statfs(ctx context.Context, req *fuse.StatfsRequest, resp *fuse.StatfsResponse) error {
	fsInfo, err := filesystem.getDFSConnector().StatFs()
	if err != nil {
		logger.Warn("Stat DFS failed", logger.Fields{Operation: StatFS, Error: err})
		return err
	}
	resp.Bsize = 1024
	resp.Bfree = fsInfo.remaining / uint64(resp.Bsize)
	resp.Bavail = resp.Bfree
	resp.Blocks = fsInfo.capacity / uint64(resp.Bsize)
	return nil
}

func (filesystem *FileSystem) getDFSConnector() HdfsAccessor {
	n := len(filesystem.HdfsAccessors)
	for {
		start := filesystem.hdfsAccessorsIndex + 1
		for i := 0; i < n; i++ {
			index := (start + i) % n
			if filesystem.HdfsAccessors[index].IsAvailable() {
				filesystem.hdfsAccessorsIndex = index
				return filesystem.HdfsAccessors[index]
			}
		}
		// All connections busy — yield and retry.
		// Normal operations finish in milliseconds, so this resolves quickly.
		runtime.Gosched()
	}
}
