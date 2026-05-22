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

	dirents, err := root.(*DirINode).ReadDirAll(nil)
	assert.Nil(t, err)
	assert.Equal(t, []string{"dataset-a", "dataset-b", "virtual-share"}, direntNames(dirents))

	sharedRoot, err := root.(*DirINode).Lookup(nil, "virtual-share")
	assert.Nil(t, err)
	assert.Equal(t, uint32(111), sharedRoot.(*DirINode).Attrs.Uid)
	assert.Equal(t, uint32(222), sharedRoot.(*DirINode).Attrs.Gid)

	sharedDirents, err := sharedRoot.(*DirINode).ReadDirAll(nil)
	assert.Nil(t, err)
	assert.Equal(t, []string{"another-project", "other-project"}, direntNames(sharedDirents))

	sharedProject, err := sharedRoot.(*DirINode).Lookup(nil, "other-project")
	assert.Nil(t, err)
	assert.Equal(t, uint32(555), sharedProject.(*DirINode).Attrs.Uid)
	assert.Equal(t, uint32(666), sharedProject.(*DirINode).Attrs.Gid)

	hdfsAccessor.EXPECT().Stat("/Projects/other-project/shared-a").Return(Attrs{Name: "shared-a", Mode: os.ModeDir}, nil).AnyTimes()
	sharedDataset, err := sharedProject.(*DirINode).Lookup(nil, "shared-a")
	assert.Nil(t, err)
	assert.NotNil(t, sharedDataset)

	projectDirents, err := sharedProject.(*DirINode).ReadDirAll(nil)
	assert.Nil(t, err)
	assert.Equal(t, []string{"shared-a", "shared-b"}, direntNames(projectDirents))

	_, err = sharedProject.(*DirINode).Lookup(nil, "not-shared")
	assert.Equal(t, syscall.ENOENT, err)
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

func direntNames(dirents []fuse.Dirent) []string {
	names := make([]string, 0, len(dirents))
	for _, dirent := range dirents {
		names = append(names, dirent.Name)
	}
	return names
}
