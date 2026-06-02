// Copyright (c) Microsoft. All rights reserved.
// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package main

import (
	"reflect"
	"testing"

	"hopsworks.ai/hopsfsmount/internal/hopsfsmount"
)

func TestParseVirtualDirectoriesJSONFallsBackToSingleObject(t *testing.T) {
	configs, err := parseVirtualDirectoriesJSON(`{"name":"shared","paths":["a"],"backendRoot":"/Projects"}`)
	if err != nil {
		t.Fatalf("expected single object fallback to succeed, got error: %v", err)
	}
	expected := []hopsfsmount.VirtualDirectoryConfig{{Name: "shared", Paths: []string{"a"}, BackendRoot: "/Projects"}}
	if !reflect.DeepEqual(configs, expected) {
		t.Fatalf("unexpected configs: got %#v want %#v", configs, expected)
	}
}

func TestParseVirtualDirectoriesJSONParsesWrapperObject(t *testing.T) {
	configs, err := parseVirtualDirectoriesJSON(`{"virtualDirectories":[{"name":"shared","paths":["a"],"backendRoot":"/Projects"}]}`)
	if err != nil {
		t.Fatalf("expected wrapper object to succeed, got error: %v", err)
	}
	expected := []hopsfsmount.VirtualDirectoryConfig{{Name: "shared", Paths: []string{"a"}, BackendRoot: "/Projects"}}
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

	expected := hopsfsmount.VirtualDirectoryConfig{
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
