// Copyright (c) Microsoft. All rights reserved.
// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package hopsfsmount

import (
	"fmt"
	"path"
	"sort"
	"strings"
)

type VirtualDirectoryConfig struct {
	Name        string   `json:"name"`
	Paths       []string `json:"paths"`
	BackendRoot string   `json:"backendRoot"`
}

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

// mutationAllowed allows mutations only inside descendants of a configured leaf.
// The leaf path itself is structural metadata and should not be renamed or replaced.
func (config VirtualDirectoryConfig) mutationAllowed(candidate string) bool {
	relPath, ok := config.pathRelativeToBackendRoot(candidate)
	if !ok {
		return false
	}

	for _, virtualPath := range config.Paths {
		if strings.HasPrefix(relPath, virtualPath+"/") {
			return true
		}
	}
	return false
}

// isPathAllowed keeps the configured leaf itself visible for read/list operations
// while also allowing descendants under that leaf.
func (config VirtualDirectoryConfig) isPathAllowed(candidate string) bool {
	if candidate == config.RootPath() {
		return true
	}
	relPath, ok := config.pathRelativeToBackendRoot(candidate)
	if !ok {
		return false
	}

	for _, virtualPath := range config.Paths {
		if strings.HasPrefix(relPath+"/", virtualPath+"/") {
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
