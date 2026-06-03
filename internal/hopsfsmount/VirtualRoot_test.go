package hopsfsmount

import (
	"os"
	"syscall"
	"testing"

	"bazil.org/fuse"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestVirtualRootMergesConfiguredPaths(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	hdfsAccessor.EXPECT().IsAvailable().Return(true).AnyTimes()
	hdfsAccessor.EXPECT().Stat("/Projects/current-project").Return(Attrs{
		Name: "current-project",
		Mode: os.ModeDir | 0770,
		Uid:  111,
		Gid:  222,
	}, nil).AnyTimes()
	hdfsAccessor.EXPECT().Stat("/Projects/another-project").Return(Attrs{
		Name: "another-project",
		Mode: os.ModeDir | 0770,
		Uid:  333,
		Gid:  444,
	}, nil).AnyTimes()
	hdfsAccessor.EXPECT().Stat("/Projects/other-project").Return(Attrs{
		Name: "other-project",
		Mode: os.ModeDir | 0770,
		Uid:  555,
		Gid:  666,
	}, nil).AnyTimes()

	fs, _ := NewFileSystem(
		[]HdfsAccessor{hdfsAccessor},
		"/Projects/current-project",
		[]string{"*"},
		false,
		DelaySyncUntilClose,
		NewDefaultRetryPolicy(mockClock),
		mockClock,
		WithVirtualDirectory("virtual-share", []string{"other-project/shared-a", "other-project/shared-b", "another-project/shared-c"}, "/Projects"),
	)
	root, _ := fs.Root()

	hdfsAccessor.EXPECT().ReadDir("/Projects/current-project").Return([]Attrs{
		{Name: "dataset-a", Mode: os.ModeDir},
		{Name: "dataset-b", Mode: os.ModeDir},
	}, nil)
	hdfsAccessor.EXPECT().Stat("/Projects/current-project/virtual-share").Return(Attrs{}, syscall.ENOENT)

	dirents, err := root.(*DirINode).ReadDirAll(nil)
	assert.Nil(t, err)
	assert.Equal(t, []string{"dataset-a", "dataset-b", "virtual-share"}, direntNames(dirents))

	sharedRoot, err := root.(*DirINode).Lookup(nil, "virtual-share")
	assert.Nil(t, err)
	assert.Equal(t, uint32(111), sharedRoot.(*VirtualDirINode).Attrs.Uid)
	assert.Equal(t, uint32(222), sharedRoot.(*VirtualDirINode).Attrs.Gid)

	sharedDirents, err := sharedRoot.(*VirtualDirINode).ReadDirAll(nil)
	assert.Nil(t, err)
	assert.Equal(t, []string{"another-project", "other-project"}, direntNames(sharedDirents))

	sharedProject, err := sharedRoot.(*VirtualDirINode).Lookup(nil, "other-project")
	assert.Nil(t, err)
	assert.Equal(t, uint32(555), sharedProject.(*VirtualDirINode).Attrs.Uid)
	assert.Equal(t, uint32(666), sharedProject.(*VirtualDirINode).Attrs.Gid)

	hdfsAccessor.EXPECT().Stat("/Projects/other-project/shared-a").Return(Attrs{Name: "shared-a", Mode: os.ModeDir}, nil).AnyTimes()
	sharedDataset, err := sharedProject.(*VirtualDirINode).Lookup(nil, "shared-a")
	assert.Nil(t, err)
	assert.NotNil(t, sharedDataset)

	projectDirents, err := sharedProject.(*VirtualDirINode).ReadDirAll(nil)
	assert.Nil(t, err)
	assert.Equal(t, []string{"shared-a", "shared-b"}, direntNames(projectDirents))

	_, err = sharedProject.(*VirtualDirINode).Lookup(nil, "not-shared")
	assert.Equal(t, syscall.ENOENT, err)
}

func TestMultipleVirtualRootsAreMergedAtRoot(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	hdfsAccessor.EXPECT().IsAvailable().Return(true).AnyTimes()
	hdfsAccessor.EXPECT().Stat("/Projects/current-project").Return(Attrs{
		Name: "current-project",
		Mode: os.ModeDir | 0770,
		Uid:  111,
		Gid:  222,
	}, nil).AnyTimes()
	hdfsAccessor.EXPECT().Stat("/Projects/current-project/shared-data").Return(Attrs{}, syscall.ENOENT).AnyTimes()
	hdfsAccessor.EXPECT().Stat("/Projects/current-project/shared-datasets").Return(Attrs{}, syscall.ENOENT).AnyTimes()
	hdfsAccessor.EXPECT().Stat("/shared-data/logs").Return(Attrs{
		Name: "logs",
		Mode: os.ModeDir | 0770,
		Uid:  333,
		Gid:  444,
	}, nil).AnyTimes()
	hdfsAccessor.EXPECT().Stat("/shared-data/logs/app-a").Return(Attrs{
		Name: "app-a",
		Mode: os.ModeDir | 0770,
		Uid:  555,
		Gid:  666,
	}, nil).AnyTimes()

	fs, _ := NewFileSystem(
		[]HdfsAccessor{hdfsAccessor},
		"/Projects/current-project",
		[]string{"*"},
		false,
		DelaySyncUntilClose,
		NewDefaultRetryPolicy(mockClock),
		mockClock,
		WithVirtualDirectories([]VirtualDirectoryConfig{
			{
				Name:        "shared-data",
				Paths:       []string{"logs/app-a"},
				BackendRoot: "/shared-data",
			},
			{
				Name:        "shared-datasets",
				Paths:       []string{"other-project/shared-a"},
				BackendRoot: "/Projects",
			},
		}),
	)
	root, _ := fs.Root()

	hdfsAccessor.EXPECT().ReadDir("/Projects/current-project").Return([]Attrs{
		{Name: "dataset-a", Mode: os.ModeDir},
		{Name: "dataset-b", Mode: os.ModeDir},
	}, nil)

	dirents, err := root.(*DirINode).ReadDirAll(nil)
	assert.Nil(t, err)
	assert.Equal(t, []string{"dataset-a", "dataset-b", "shared-data", "shared-datasets"}, direntNames(dirents))

	sharedData, err := root.(*DirINode).Lookup(nil, "shared-data")
	assert.Nil(t, err)
	sharedDataDirents, err := sharedData.(*VirtualDirINode).ReadDirAll(nil)
	assert.Nil(t, err)
	assert.Equal(t, []string{"logs"}, direntNames(sharedDataDirents))

	logs, err := sharedData.(*VirtualDirINode).Lookup(nil, "logs")
	assert.Nil(t, err)
	logsDirents, err := logs.(*VirtualDirINode).ReadDirAll(nil)
	assert.Nil(t, err)
	assert.Equal(t, []string{"app-a"}, direntNames(logsDirents))

	app, err := logs.(*VirtualDirINode).Lookup(nil, "app-a")
	assert.Nil(t, err)
	assert.NotNil(t, app)
}

func TestVirtualRootIsDisabledWithoutConfiguration(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	hdfsAccessor.EXPECT().IsAvailable().Return(true).AnyTimes()

	fs, _ := NewFileSystem(
		[]HdfsAccessor{hdfsAccessor},
		"/Projects/current-project",
		[]string{"*"},
		false,
		DelaySyncUntilClose,
		NewDefaultRetryPolicy(mockClock),
		mockClock,
	)
	root, _ := fs.Root()

	hdfsAccessor.EXPECT().ReadDir("/Projects/current-project").Return([]Attrs{
		{Name: "dataset-a", Mode: os.ModeDir},
	}, nil)

	dirents, err := root.(*DirINode).ReadDirAll(nil)
	assert.Nil(t, err)
	assert.Equal(t, []string{"dataset-a"}, direntNames(dirents))
}

func TestVirtualDirectoryConfigValidation(t *testing.T) {
	mockClock := &MockClock{}

	_, err := NewFileSystem(
		nil,
		"/Projects/current-project",
		[]string{"*"},
		false,
		DelaySyncUntilClose,
		NewDefaultRetryPolicy(mockClock),
		mockClock,
		WithVirtualDirectory("shared/datasets", []string{"other-project/shared-a"}, "/Projects"),
	)
	assert.Error(t, err)

	_, err = NewFileSystem(
		nil,
		"/Projects/current-project",
		[]string{"*"},
		false,
		DelaySyncUntilClose,
		NewDefaultRetryPolicy(mockClock),
		mockClock,
		WithVirtualDirectory("shared-datasets", []string{"../secret"}, "/Projects"),
	)
	assert.Error(t, err)

	_, err = NewFileSystem(
		nil,
		"/Projects/current-project",
		[]string{"*"},
		false,
		DelaySyncUntilClose,
		NewDefaultRetryPolicy(mockClock),
		mockClock,
		WithVirtualDirectory("shared-datasets", []string{"other-project/shared-a"}, ""),
	)
	assert.Error(t, err)

	_, err = NewFileSystem(
		nil,
		"/Projects/current-project",
		[]string{"*"},
		false,
		DelaySyncUntilClose,
		NewDefaultRetryPolicy(mockClock),
		mockClock,
		WithVirtualDirectory("shared-datasets", []string{"other-project/shared-a"}, "Projects"),
	)
	assert.Error(t, err)
}

func TestVirtualDirectoryLeafTargetsAreVisibleButNotMutable(t *testing.T) {
	cfg := VirtualDirectoryConfig{
		Name:        "shared-datasets",
		Paths:       []string{"projA/dataset-1", "projB/dataset-2"},
		BackendRoot: "/Projects",
	}

	assert.True(t, cfg.isPathAllowed("/Projects/projA/dataset-1"))
	assert.True(t, cfg.isPathAllowed("/Projects/projA/dataset-1/file1"))
}

func TestVirtualRootCollisionPrefersBackendEntry(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	hdfsAccessor.EXPECT().IsAvailable().Return(true).AnyTimes()

	fs, _ := NewFileSystem(
		[]HdfsAccessor{hdfsAccessor},
		"/Projects/current-project",
		[]string{"*"},
		false,
		DelaySyncUntilClose,
		NewDefaultRetryPolicy(mockClock),
		mockClock,
		WithVirtualDirectory("shared-datasets", []string{"other-project/shared-a"}, "/Projects"),
	)
	root, _ := fs.Root()

	hdfsAccessor.EXPECT().Stat("/Projects/current-project/shared-datasets").Return(Attrs{}, syscall.ENOENT)
	hdfsAccessor.EXPECT().Stat("/Projects/current-project").Return(Attrs{
		Name:    "current-project",
		Mode:    os.ModeDir | 0770,
		Uid:     111,
		Gid:     222,
		Expires: mockClock.Now().Add(CacheAttrsTimeDuration),
	}, nil)
	synthetic, err := root.(*DirINode).Lookup(nil, "shared-datasets")
	assert.Nil(t, err)
	assert.IsType(t, &VirtualDirINode{}, synthetic)

	hdfsAccessor.EXPECT().ReadDir("/Projects/current-project").Return([]Attrs{
		{
			Name:    "dataset-a",
			Mode:    os.ModeDir,
			Expires: mockClock.Now().Add(CacheAttrsTimeDuration),
		},
		{
			Name:    "shared-datasets",
			Mode:    os.ModeDir | 0770,
			Uid:     333,
			Gid:     444,
			Expires: mockClock.Now().Add(CacheAttrsTimeDuration),
		},
	}, nil)

	dirents, err := root.(*DirINode).ReadDirAll(nil)
	assert.Nil(t, err)
	assert.Equal(t, []string{"dataset-a", "shared-datasets"}, direntNames(dirents))

	node, err := root.(*DirINode).Lookup(nil, "shared-datasets")
	assert.Nil(t, err)
	collision := node.(*DirINode)
	assert.Equal(t, uint32(333), collision.Attrs.Uid)
	assert.Equal(t, uint32(444), collision.Attrs.Gid)
}

func TestVirtualDirectoryMetadataIsCached(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	hdfsAccessor.EXPECT().IsAvailable().Return(true).AnyTimes()

	fs, _ := NewFileSystem(
		[]HdfsAccessor{hdfsAccessor},
		"/Projects/current-project",
		[]string{"*"},
		false,
		DelaySyncUntilClose,
		NewDefaultRetryPolicy(mockClock),
		mockClock,
		WithVirtualDirectory("shared-datasets", []string{"other-project/shared-a"}, "/Projects"),
	)
	root, _ := fs.Root()

	hdfsAccessor.EXPECT().Stat("/Projects/current-project/shared-datasets").Return(Attrs{}, syscall.ENOENT)
	hdfsAccessor.EXPECT().Stat("/Projects/current-project").Return(Attrs{
		Name:    "current-project",
		Mode:    os.ModeDir | 0770,
		Uid:     111,
		Gid:     222,
		Expires: mockClock.Now().Add(CacheAttrsTimeDuration),
	}, nil)
	first, err := root.(*DirINode).Lookup(nil, "shared-datasets")
	assert.Nil(t, err)
	second, err := root.(*DirINode).Lookup(nil, "shared-datasets")
	assert.Nil(t, err)
	assert.Equal(t, first, second)

	hdfsAccessor.EXPECT().Stat("/Projects/other-project").Return(Attrs{
		Name:    "other-project",
		Mode:    os.ModeDir | 0770,
		Uid:     333,
		Gid:     444,
		Expires: mockClock.Now().Add(CacheAttrsTimeDuration),
	}, nil)
	sharedProject, err := first.(*VirtualDirINode).Lookup(nil, "other-project")
	assert.Nil(t, err)
	dirents, err := first.(*VirtualDirINode).ReadDirAll(nil)
	assert.Nil(t, err)
	assert.Equal(t, []string{"other-project"}, direntNames(dirents))
	direntsAgain, err := first.(*VirtualDirINode).ReadDirAll(nil)
	assert.Nil(t, err)
	assert.Equal(t, []string{"other-project"}, direntNames(direntsAgain))
	sharedProjectAgain, err := first.(*VirtualDirINode).Lookup(nil, "other-project")
	assert.Nil(t, err)
	assert.Equal(t, sharedProject, sharedProjectAgain)
}

func TestVirtualDirectoryMutationsOutsideConfiguredLeavesAreRejected(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	hdfsAccessor.EXPECT().IsAvailable().Return(true).AnyTimes()

	fs, _ := NewFileSystem(
		[]HdfsAccessor{hdfsAccessor},
		"/Projects/current-project",
		[]string{"*"},
		false,
		DelaySyncUntilClose,
		NewDefaultRetryPolicy(mockClock),
		mockClock,
		WithVirtualDirectory("shared-datasets", []string{"other-project/shared-a"}, "/Projects"),
	)
	root, _ := fs.Root()

	hdfsAccessor.EXPECT().Stat("/Projects/current-project/shared-datasets").Return(Attrs{}, syscall.ENOENT)
	hdfsAccessor.EXPECT().Stat("/Projects/current-project").Return(Attrs{
		Name:    "current-project",
		Mode:    os.ModeDir | 0770,
		Uid:     111,
		Gid:     222,
		Expires: mockClock.Now().Add(CacheAttrsTimeDuration),
	}, nil)
	sharedRoot, err := root.(*DirINode).Lookup(nil, "shared-datasets")
	assert.Nil(t, err)

	_, err = sharedRoot.(*VirtualDirINode).Mkdir(nil, &fuse.MkdirRequest{
		Name: "other-project",
		Mode: os.ModeDir | 0755,
	})
	assert.Equal(t, syscall.EPERM, err)
}

func TestVirtualDirectoryRenameWithinConfiguredLeafPath(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	hdfsAccessor.EXPECT().IsAvailable().Return(true).AnyTimes()

	fs, _ := NewFileSystem(
		[]HdfsAccessor{hdfsAccessor},
		"/Projects/current-project",
		[]string{"*"},
		false,
		DelaySyncUntilClose,
		NewDefaultRetryPolicy(mockClock),
		mockClock,
		WithVirtualDirectory("shared-datasets", []string{"other-project/shared-a"}, "/Projects"),
	)
	root, _ := fs.Root()

	hdfsAccessor.EXPECT().Stat("/Projects/current-project/shared-datasets").Return(Attrs{}, syscall.ENOENT)
	hdfsAccessor.EXPECT().Stat("/Projects/current-project").Return(Attrs{
		Name:    "current-project",
		Mode:    os.ModeDir | 0770,
		Uid:     111,
		Gid:     222,
		Expires: mockClock.Now().Add(CacheAttrsTimeDuration),
	}, nil)
	sharedRoot, err := root.(*DirINode).Lookup(nil, "shared-datasets")
	assert.Nil(t, err)

	hdfsAccessor.EXPECT().Stat("/Projects/other-project").Return(Attrs{
		Name:    "other-project",
		Mode:    os.ModeDir | 0770,
		Uid:     333,
		Gid:     444,
		Expires: mockClock.Now().Add(CacheAttrsTimeDuration),
	}, nil)
	sharedProject, err := sharedRoot.(*VirtualDirINode).Lookup(nil, "other-project")
	assert.Nil(t, err)

	hdfsAccessor.EXPECT().Stat("/Projects/other-project/shared-a").Return(Attrs{
		Name:    "shared-a",
		Mode:    os.ModeDir | 0770,
		Uid:     555,
		Gid:     666,
		Expires: mockClock.Now().Add(CacheAttrsTimeDuration),
	}, nil)
	sharedLeaf, err := sharedProject.(*VirtualDirINode).Lookup(nil, "shared-a")
	assert.Nil(t, err)

	// Cache file1 before the rename. In production the kernel issues a Lookup before
	// dispatching a rename, so the inode is normally present in the children cache
	// when renameInt runs. The cached inode is what renameInt re-parents under the
	// new name in the destination directory.
	hdfsAccessor.EXPECT().Stat("/Projects/other-project/shared-a/file1").Return(Attrs{
		Name:    "file1",
		Mode:    0644,
		Uid:     777,
		Gid:     888,
		Expires: mockClock.Now().Add(CacheAttrsTimeDuration),
	}, nil)
	_, err = sharedLeaf.(*DirINode).Lookup(nil, "file1")
	assert.Nil(t, err)

	hdfsAccessor.EXPECT().Rename2("/Projects/other-project/shared-a/file1", "/Projects/other-project/shared-a/file2", gomock.Any()).
		Return(nil)

	err = sharedLeaf.(*DirINode).Rename2(nil, &fuse.Rename2Request{
		OldName: "file1",
		NewName: "file2",
	}, sharedLeaf)
	assert.Nil(t, err)

	// After rename, file2 is in the cache via adoption — no backend Stat needed.
	renamed, err := sharedLeaf.(*DirINode).Lookup(nil, "file2")
	assert.Nil(t, err)
	assert.NotNil(t, renamed)
}

func TestVirtualRootReadDirDoesNotAbortOnMissingBranchMetadata(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	hdfsAccessor.EXPECT().IsAvailable().Return(true).AnyTimes()

	fs, _ := NewFileSystem(
		[]HdfsAccessor{hdfsAccessor},
		"/Projects/current-project",
		[]string{"*"},
		false,
		DelaySyncUntilClose,
		NewDefaultRetryPolicy(mockClock),
		mockClock,
		WithVirtualDirectories([]VirtualDirectoryConfig{
			{
				Name:        "shared-datasets",
				Paths:       []string{"good-project/shared-a", "bad-project/shared-b"},
				BackendRoot: "/Projects",
			},
		}),
	)
	root, _ := fs.Root()

	hdfsAccessor.EXPECT().Stat("/Projects/current-project/shared-datasets").Return(Attrs{}, syscall.ENOENT)
	hdfsAccessor.EXPECT().Stat("/Projects/current-project").Return(Attrs{
		Name:    "current-project",
		Mode:    os.ModeDir | 0770,
		Uid:     111,
		Gid:     222,
		Expires: mockClock.Now().Add(CacheAttrsTimeDuration),
	}, nil)
	sharedRoot, err := root.(*DirINode).Lookup(nil, "shared-datasets")
	assert.Nil(t, err)

	hdfsAccessor.EXPECT().Stat("/Projects/good-project").Return(Attrs{
		Name:    "good-project",
		Mode:    os.ModeDir | 0770,
		Uid:     333,
		Gid:     444,
		Expires: mockClock.Now().Add(CacheAttrsTimeDuration),
	}, nil)
	hdfsAccessor.EXPECT().Stat("/Projects/bad-project").Return(Attrs{}, syscall.EACCES)
	dirents, err := sharedRoot.(*VirtualDirINode).ReadDirAll(nil)
	assert.Nil(t, err)
	assert.Equal(t, []string{"bad-project", "good-project"}, direntNames(dirents))
}

func direntNames(dirents []fuse.Dirent) []string {
	names := make([]string, 0, len(dirents))
	for _, dirent := range dirents {
		names = append(names, dirent.Name)
	}
	return names
}
