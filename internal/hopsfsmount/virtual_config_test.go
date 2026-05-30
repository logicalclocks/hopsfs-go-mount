// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package hopsfsmount

import (
	"reflect"
	"testing"
)

func TestParseVirtualDirectoriesJSONFallsBackToSingleObject(t *testing.T) {
	configs, err := parseVirtualDirectoriesJSON(`{"name":"shared","paths":["a"],"backendRoot":"/Projects"}`)
	if err != nil {
		t.Fatalf("expected single object fallback to succeed, got error: %v", err)
	}
	expected := []VirtualDirectoryConfig{{Name: "shared", Paths: []string{"a"}, BackendRoot: "/Projects"}}
	if !reflect.DeepEqual(configs, expected) {
		t.Fatalf("unexpected configs: got %#v want %#v", configs, expected)
	}
}

func TestParseVirtualDirectoriesJSONParsesWrapperObject(t *testing.T) {
	configs, err := parseVirtualDirectoriesJSON(`{"virtualDirectories":[{"name":"shared","paths":["a"],"backendRoot":"/Projects"}]}`)
	if err != nil {
		t.Fatalf("expected wrapper object to succeed, got error: %v", err)
	}
	expected := []VirtualDirectoryConfig{{Name: "shared", Paths: []string{"a"}, BackendRoot: "/Projects"}}
	if !reflect.DeepEqual(configs, expected) {
		t.Fatalf("unexpected configs: got %#v want %#v", configs, expected)
	}
}

func TestParseVirtualDirectoriesJSONRejectsMissingBackendRoot(t *testing.T) {
	_, err := parseVirtualDirectoriesJSON(`{"name":"shared","paths":["a"]}`)
	if err == nil {
		t.Fatal("expected missing backendRoot to fail")
	}
}

func TestParseVirtualDirectoriesJSONRejectsEmptyWrapperObject(t *testing.T) {
	_, err := parseVirtualDirectoriesJSON(`{}`)
	if err == nil {
		t.Fatal("expected empty wrapper object to fail")
	}
}

func TestParseVirtualDirectoriesJSONRejectsEmptyVirtualDirectoriesField(t *testing.T) {
	_, err := parseVirtualDirectoriesJSON(`{"virtualDirectories":[]}`)
	if err == nil {
		t.Fatal("expected empty virtualDirectories field to fail")
	}
}

func TestParseVirtualDirectoriesJSONRejectsEmptyJSONArray(t *testing.T) {
	_, err := parseVirtualDirectoriesJSON(`[]`)
	if err == nil {
		t.Fatal("expected empty JSON array to fail")
	}
}

func TestParseVirtualDirectorySpecEntryParsesExplicitBackendRoot(t *testing.T) {
	config, err := parseVirtualDirectorySpecEntry("shared:/Projects:a,b")
	if err != nil {
		t.Fatalf("expected explicit backend root to parse, got error: %v", err)
	}

	expected := VirtualDirectoryConfig{
		Name:        "shared",
		Paths:       []string{"a", "b"},
		BackendRoot: "/Projects",
	}
	if !reflect.DeepEqual(config, expected) {
		t.Fatalf("unexpected config: got %#v want %#v", config, expected)
	}
}

func TestParseVirtualDirectorySpecEntryRejectsMissingBackendRoot(t *testing.T) {
	_, err := parseVirtualDirectorySpecEntry("shared:a,b")
	if err == nil {
		t.Fatal("expected missing backend root to fail")
	}
}

// ============================================================================
// Predicate tests — path-component guards
// ============================================================================

// TestPathRelativeToBackendRoot — exercises the prefix-as-path-component guard.
// This is the function whose correctness prevents `/foo` from accidentally
// matching `/foobar`.
func TestPathRelativeToBackendRoot(t *testing.T) {
	cases := []struct {
		name        string
		backendRoot string
		candidate   string
		wantRel     string
		wantOk      bool
	}{
		{"root-self", "/", "/", "", false},
		{"root-with-child", "/", "/foo", "foo", true},
		{"root-with-deep-child", "/", "/foo/bar", "foo/bar", true},
		{"backendroot-self", "/Projects", "/Projects", "", false},
		{"backendroot-with-child", "/Projects", "/Projects/foo", "foo", true},
		{"backendroot-with-deep-child", "/Projects", "/Projects/foo/bar", "foo/bar", true},
		// THE KEY CASE: /Projects must NOT match /ProjectsX (substring-prefix bug guard).
		{"prefix-guard-suffix-name", "/Projects", "/ProjectsX/foo", "", false},
		{"unrelated-path", "/Projects", "/other/foo", "", false},
		{"empty-candidate", "/Projects", "", "", false},
		{"trailing-slash", "/Projects", "/Projects/", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			config := VirtualDirectoryConfig{BackendRoot: tc.backendRoot}
			rel, ok := config.pathRelativeToBackendRoot(tc.candidate)
			if rel != tc.wantRel || ok != tc.wantOk {
				t.Errorf("pathRelativeToBackendRoot(%q, %q) = (%q, %v), want (%q, %v)",
					tc.backendRoot, tc.candidate, rel, ok, tc.wantRel, tc.wantOk)
			}
		})
	}
}

// TestMutationAllowed — descendants-of-leaf only. Leaf itself and synthetic
// branches must be rejected (structural).
func TestMutationAllowed(t *testing.T) {
	config := VirtualDirectoryConfig{
		Name:        "shared",
		Paths:       []string{"projA/dsA", "projB/some_dir/dsC"},
		BackendRoot: "/Projects",
	}
	cases := []struct {
		name      string
		candidate string
		want      bool
	}{
		{"file-inside-leaf", "/Projects/projA/dsA/file", true},
		{"deep-inside-leaf", "/Projects/projA/dsA/sub/sub2/file", true},
		{"file-inside-deep-leaf", "/Projects/projB/some_dir/dsC/file", true},
		{"leaf-itself", "/Projects/projA/dsA", false},
		{"deep-leaf-itself", "/Projects/projB/some_dir/dsC", false},
		{"branch", "/Projects/projA", false},
		{"intermediate-branch", "/Projects/projB/some_dir", false},
		{"backend-root", "/Projects", false},
		{"outside-backend-root", "/other/file", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := config.mutationAllowed(tc.candidate); got != tc.want {
				t.Errorf("mutationAllowed(%q) = %v, want %v", tc.candidate, got, tc.want)
			}
		})
	}
}

// TestIsPathAllowed — visible for read/list: mount-side virtual root, the
// configured leaf, and any descendant of the leaf. NOT branches.
func TestIsPathAllowed(t *testing.T) {
	config := VirtualDirectoryConfig{
		Name:        "shared",
		Paths:       []string{"projA/dsA"},
		BackendRoot: "/Projects",
	}
	cases := []struct {
		name      string
		candidate string
		want      bool
	}{
		{"mount-side-root", "/shared", true},
		{"configured-leaf", "/Projects/projA/dsA", true},
		{"descendant-of-leaf", "/Projects/projA/dsA/file", true},
		{"deep-descendant-of-leaf", "/Projects/projA/dsA/sub/file", true},
		{"branch-not-leaf", "/Projects/projA", false},
		{"backend-root", "/Projects", false},
		{"outside-backend-root", "/other/file", false},
		{"empty", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := config.isPathAllowed(tc.candidate); got != tc.want {
				t.Errorf("isPathAllowed(%q) = %v, want %v", tc.candidate, got, tc.want)
			}
		})
	}
}

// TestRelPathExists — true for leaves and any prefix of a leaf.
func TestRelPathExists(t *testing.T) {
	config := VirtualDirectoryConfig{
		Paths: []string{"projA/dsA", "projB/some_dir/dsC"},
	}
	cases := []struct {
		name    string
		relPath string
		want    bool
	}{
		{"empty", "", false},
		{"top-branch-projA", "projA", true},
		{"top-branch-projB", "projB", true},
		{"intermediate-branch", "projB/some_dir", true},
		{"leaf-projA-dsA", "projA/dsA", true},
		{"deep-leaf", "projB/some_dir/dsC", true},
		{"nonexistent", "projC", false},
		{"partial-segment-substring", "projA/ds", false}, // not a path component
		{"trailing-slash-leaf", "projA/dsA/", true},
		{"past-the-leaf", "projA/dsA/extra", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := config.relPathExists(tc.relPath); got != tc.want {
				t.Errorf("relPathExists(%q) = %v, want %v", tc.relPath, got, tc.want)
			}
		})
	}
}

// TestLeafExists — only true for exact leaves; branches and unrelated names false.
func TestLeafExists(t *testing.T) {
	config := VirtualDirectoryConfig{
		Paths: []string{"projA/dsA", "projB/some_dir/dsC"},
	}
	cases := []struct {
		name    string
		relPath string
		want    bool
	}{
		{"empty", "", false},
		{"branch", "projA", false},
		{"intermediate-branch", "projB/some_dir", false},
		{"leaf-projA-dsA", "projA/dsA", true},
		{"deep-leaf", "projB/some_dir/dsC", true},
		{"trailing-slash-leaf", "projA/dsA/", true},
		{"nonexistent", "projC", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := config.leafExists(tc.relPath); got != tc.want {
				t.Errorf("leafExists(%q) = %v, want %v", tc.relPath, got, tc.want)
			}
		})
	}
}

// TestChildNames_MultiLevel configures paths with intermediate synthetic
// branches and verifies childNames returns the right next-segments at every
// level.
func TestChildNames_MultiLevel(t *testing.T) {
	config := VirtualDirectoryConfig{
		Paths: []string{"projA/dsA", "projA/dsB", "projB/some_dir/dsC"},
	}
	cases := []struct {
		name    string
		relPath string
		want    []string
	}{
		{"top-level", "", []string{"projA", "projB"}},
		{"projA-children", "projA", []string{"dsA", "dsB"}},
		{"projB-children-intermediate", "projB", []string{"some_dir"}},
		{"intermediate-branch-children", "projB/some_dir", []string{"dsC"}},
		{"leaf-has-no-children", "projA/dsA", []string{}},
		{"deep-leaf-has-no-children", "projB/some_dir/dsC", []string{}},
		{"nonexistent", "projC", []string{}},
		{"partial-but-wrong", "projB/wrong_dir", []string{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := config.childNames(tc.relPath)
			if !reflect.DeepEqual(got, tc.want) {
				// Treat nil and empty slice as equivalent for the "empty result" cases.
				if len(got) == 0 && len(tc.want) == 0 {
					return
				}
				t.Errorf("childNames(%q) = %v, want %v", tc.relPath, got, tc.want)
			}
		})
	}
}

// TestChildNames_Dedupes — two paths that share a top-level segment produce
// only one entry at the top level.
func TestChildNames_Dedupes(t *testing.T) {
	config := VirtualDirectoryConfig{
		Paths: []string{"projA/dsA", "projA/dsB", "projA/dsC"},
	}
	got := config.childNames("")
	if !reflect.DeepEqual(got, []string{"projA"}) {
		t.Errorf("childNames(\"\") = %v, want [projA] (deduped)", got)
	}
}

// ============================================================================
// Normalizer tests
// ============================================================================

func TestNormalizeVirtualDirectoryName(t *testing.T) {
	cases := []struct {
		input   string
		want    string
		wantErr bool
	}{
		{"", "", false},    // empty → silent skip
		{"   ", "", false}, // whitespace → silent skip
		{"shared", "shared", false},
		{"  shared  ", "shared", false}, // trimmed
		{"a/b", "", true},               // slash forbidden
		{".", "", true},                 // dot forbidden
		{"..", "", true},                // dotdot forbidden
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			got, err := normalizeVirtualDirectoryName(tc.input)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestNormalizeVirtualDirectoryPath(t *testing.T) {
	cases := []struct {
		input   string
		want    string
		wantErr bool
	}{
		{"", "", false},
		{"   ", "", false},
		{"foo", "foo", false},
		{"/foo", "foo", false},
		{"foo/", "foo", false},
		{"/foo/bar/", "foo/bar", false},
		{"foo/bar/baz", "foo/bar/baz", false},
		{"foo//bar", "", true}, // empty component
		{"foo/./bar", "", true},
		{"foo/../bar", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			got, err := normalizeVirtualDirectoryPath(tc.input)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestNormalizeVirtualDirectoryBackendRoot(t *testing.T) {
	cases := []struct {
		input   string
		want    string
		wantErr bool
	}{
		{"", "", true},    // empty → error
		{"foo", "", true}, // relative → error
		{"/", "/", false}, // root is allowed
		{"/Projects", "/Projects", false},
		{"/Projects/", "/Projects", false},
		{"  /Projects  ", "/Projects", false},
		{"/Projects/foo", "/Projects/foo", false},
		{"/foo/./bar", "", true}, // dot forbidden
		{"/foo/..", "", true},    // dotdot forbidden
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			got, err := normalizeVirtualDirectoryBackendRoot(tc.input)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestNormalizeVirtualDirectoryConfigs(t *testing.T) {
	t.Run("empty-input", func(t *testing.T) {
		got, err := normalizeVirtualDirectoryConfigs(nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got) != 0 {
			t.Errorf("expected empty output, got: %v", got)
		}
	})

	t.Run("single-valid-config", func(t *testing.T) {
		input := []VirtualDirectoryConfig{{
			Name:        "shared",
			Paths:       []string{"projA/dsA"},
			BackendRoot: "/Projects",
		}}
		got, err := normalizeVirtualDirectoryConfigs(input)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got) != 1 || got[0].Name != "shared" || got[0].BackendRoot != "/Projects" {
			t.Errorf("unexpected output: %v", got)
		}
	})

	t.Run("duplicate-names-rejected", func(t *testing.T) {
		input := []VirtualDirectoryConfig{
			{Name: "shared", Paths: []string{"a"}, BackendRoot: "/Projects"},
			{Name: "shared", Paths: []string{"b"}, BackendRoot: "/Projects"},
		}
		_, err := normalizeVirtualDirectoryConfigs(input)
		if err == nil {
			t.Fatal("expected duplicate-name error")
		}
	})

	t.Run("empty-name-silently-skipped", func(t *testing.T) {
		input := []VirtualDirectoryConfig{
			{Name: "", Paths: []string{"a"}, BackendRoot: "/Projects"},
			{Name: "good", Paths: []string{"a"}, BackendRoot: "/Projects"},
		}
		got, err := normalizeVirtualDirectoryConfigs(input)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got) != 1 || got[0].Name != "good" {
			t.Errorf("expected only 'good' config, got: %v", got)
		}
	})

	t.Run("empty-paths-silently-skipped", func(t *testing.T) {
		input := []VirtualDirectoryConfig{
			{Name: "shared", Paths: []string{}, BackendRoot: "/Projects"},
		}
		got, err := normalizeVirtualDirectoryConfigs(input)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got) != 0 {
			t.Errorf("expected empty output (no paths), got: %v", got)
		}
	})

	t.Run("sorted-by-name", func(t *testing.T) {
		input := []VirtualDirectoryConfig{
			{Name: "zeta", Paths: []string{"a"}, BackendRoot: "/Projects"},
			{Name: "alpha", Paths: []string{"a"}, BackendRoot: "/Projects"},
			{Name: "beta", Paths: []string{"a"}, BackendRoot: "/Projects"},
		}
		got, err := normalizeVirtualDirectoryConfigs(input)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got) != 3 || got[0].Name != "alpha" || got[1].Name != "beta" || got[2].Name != "zeta" {
			t.Errorf("expected alpha,beta,zeta in order, got: %v", got)
		}
	})

	t.Run("paths-sorted-and-deduped", func(t *testing.T) {
		input := []VirtualDirectoryConfig{{
			Name:        "shared",
			Paths:       []string{"zeta/x", "alpha/x", "zeta/x", "beta/x"},
			BackendRoot: "/Projects",
		}}
		got, err := normalizeVirtualDirectoryConfigs(input)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		wantPaths := []string{"alpha/x", "beta/x", "zeta/x"}
		if !reflect.DeepEqual(got[0].Paths, wantPaths) {
			t.Errorf("expected paths %v, got %v", wantPaths, got[0].Paths)
		}
	})

	t.Run("invalid-backend-root-error", func(t *testing.T) {
		input := []VirtualDirectoryConfig{{
			Name:        "shared",
			Paths:       []string{"a"},
			BackendRoot: "relative", // not absolute
		}}
		_, err := normalizeVirtualDirectoryConfigs(input)
		if err == nil {
			t.Fatal("expected invalid backend root error")
		}
	})
}
