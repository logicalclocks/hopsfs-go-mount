// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package hopsfsmount

import (
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strings"
)

// VirtualDirectoryConfig describes one configured virtual directory: a synthetic
// name surfaced at the mount root, plus the backend paths exposed inside it.
//
// Paths is interpreted as an *allowlist*. Only names that appear in Paths verbatim
// (leaves) or are a prefix of an entry in Paths (synthetic branches) are visible
// through the virtual tree. Backend entries that exist but are not listed are
// invisible — synthetic lookup of unlisted names returns ENOENT regardless of
// what the backend contains. This is what stops the virtual layer from becoming
// a passthrough of the backend subtree underneath BackendRoot.
//
// Paths are relative to BackendRoot. The full backend path of an entry is
// path.Join(BackendRoot, entry). BackendRoot must be absolute and is required
// (no implicit default).
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
		return "", fmt.Errorf("invalid virtual directory backend root: must be specified and absolute")
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

// relPathExists reports whether relPath is part of the configured tree — either
// a leaf itself or a strict prefix of a leaf (i.e. a synthetic branch). Lookup
// uses this to short-circuit to ENOENT for names that match nothing in Paths.
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

// leafExists reports whether relPath is exactly a configured leaf (a path in Paths
// verbatim). Leaves are the synthetic/real boundary: Lookup of a leaf returns a
// real backend node rather than a synthetic one. Inside a leaf, the mount behaves
// like a normal HopsFS mount.
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

// ParseVirtualDirectoriesSpec parses the raw --virtualDirectories flag value into a
// slice of VirtualDirectoryConfig. The raw input may be either:
//   - the compact spec: <name>:<backend-root>:<paths>[;<name>:...]
//   - a JSON array of VirtualDirectoryConfig
//   - a JSON object with a "virtualDirectories" array, or a single-object form
func ParseVirtualDirectoriesSpec(raw string) ([]VirtualDirectoryConfig, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}

	if strings.HasPrefix(raw, "[") || strings.HasPrefix(raw, "{") {
		return parseVirtualDirectoriesJSON(raw)
	}

	configs := make([]VirtualDirectoryConfig, 0)
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

func parseVirtualDirectorySpecEntry(entry string) (VirtualDirectoryConfig, error) {
	parts := strings.SplitN(entry, ":", 3)
	if len(parts) != 3 {
		return VirtualDirectoryConfig{}, fmt.Errorf("invalid virtual directory spec %q: expected <name>:<backend-root>:<backend-dirs>", entry)
	}
	name := strings.TrimSpace(parts[0])
	if name == "" {
		return VirtualDirectoryConfig{}, fmt.Errorf("invalid virtual directory spec %q: virtual directory name is required", entry)
	}
	backendRoot, err := normalizeVirtualDirectoryBackendRoot(parts[1])
	if err != nil {
		return VirtualDirectoryConfig{}, fmt.Errorf("invalid virtual directory spec %q: %w", entry, err)
	}
	rawDirs := splitCSV(parts[2])
	if len(rawDirs) == 0 {
		return VirtualDirectoryConfig{}, fmt.Errorf("invalid virtual directory spec %q: at least one backend directory is required", entry)
	}

	normalizedPaths := make([]string, 0, len(rawDirs))
	for _, rawDir := range rawDirs {
		rawDir = strings.TrimSpace(rawDir)
		if rawDir == "" {
			continue
		}
		normalized, err := normalizeVirtualDirectoryPath(rawDir)
		if err != nil {
			return VirtualDirectoryConfig{}, err
		}
		if normalized == "" {
			continue
		}
		normalizedPaths = append(normalizedPaths, normalized)
	}
	if len(normalizedPaths) == 0 {
		return VirtualDirectoryConfig{}, fmt.Errorf("invalid virtual directory spec %q: at least one backend directory is required", entry)
	}
	return VirtualDirectoryConfig{
		Name:        name,
		Paths:       normalizedPaths,
		BackendRoot: backendRoot,
	}, nil
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

func parseVirtualDirectoriesJSON(raw string) ([]VirtualDirectoryConfig, error) {
	var configs []VirtualDirectoryConfig
	if err := json.Unmarshal([]byte(raw), &configs); err == nil {
		if len(configs) == 0 {
			return nil, fmt.Errorf("virtual directory JSON array must not be empty")
		}
		if err := validateVirtualDirectoryJSONConfigs(configs); err != nil {
			return nil, err
		}
		return configs, nil
	}

	var payload map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &payload); err == nil {
		if rawVirtualDirectories, ok := payload["virtualDirectories"]; ok {
			if err := json.Unmarshal(rawVirtualDirectories, &configs); err != nil {
				return nil, fmt.Errorf("invalid virtualDirectories field: %w", err)
			}
			if len(configs) == 0 {
				return nil, fmt.Errorf("virtualDirectories must contain at least one entry")
			}
			if err := validateVirtualDirectoryJSONConfigs(configs); err != nil {
				return nil, err
			}
			return configs, nil
		}

		var single VirtualDirectoryConfig
		if err := json.Unmarshal([]byte(raw), &single); err != nil {
			return nil, err
		}
		if strings.TrimSpace(single.Name) == "" {
			return nil, fmt.Errorf("virtual directory config must include a non-empty name")
		}
		if len(single.Paths) == 0 {
			return nil, fmt.Errorf("virtual directory config %q must include at least one backend directory", single.Name)
		}
		if err := validateVirtualDirectoryJSONConfig(single); err != nil {
			return nil, err
		}
		return []VirtualDirectoryConfig{single}, nil
	}

	return nil, fmt.Errorf("invalid virtual directory JSON config")
}

func validateVirtualDirectoryJSONConfigs(configs []VirtualDirectoryConfig) error {
	for _, config := range configs {
		if err := validateVirtualDirectoryJSONConfig(config); err != nil {
			return err
		}
	}
	return nil
}

func validateVirtualDirectoryJSONConfig(config VirtualDirectoryConfig) error {
	if strings.TrimSpace(config.Name) == "" {
		return fmt.Errorf("virtual directory config must include a non-empty name")
	}
	if len(config.Paths) == 0 {
		return fmt.Errorf("virtual directory config %q must include at least one backend directory", config.Name)
	}
	if _, err := normalizeVirtualDirectoryBackendRoot(config.BackendRoot); err != nil {
		return fmt.Errorf("invalid virtual directory config %q: %w", config.Name, err)
	}
	return nil
}

// childNames returns the next-segment names under relPath in the configured
// allowlist. This is the source of truth for synthetic ReadDirAll — the backend
// is not consulted. As a result, the synthetic listing shows only configured
// paths, not whatever else happens to exist under the same backend prefix.
//
// Example with Paths=["projA/dsA", "projA/dsB", "projB/dsC"]:
//
//	childNames("")          → ["projA", "projB"]   // top-level segments, deduped
//	childNames("projA")     → ["dsA", "dsB"]       // under projA branch
//	childNames("projA/dsA") → []                   // leaf has no synthetic children
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
