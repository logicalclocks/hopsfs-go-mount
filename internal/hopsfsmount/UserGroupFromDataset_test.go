package hopsfsmount

import (
	"fmt"
	"testing"
)

func TestGetGroupFromDatasetPath(t *testing.T) {
	withMount(t, "/", DelaySyncUntilClose, func(mountPoint string, hdfsAccessor HdfsAccessor) {
		expectedPath := "tets_project__Jupyter"
		cases := []struct {
			Name          string
			Path          string
			IsCorrectPath bool
		}{
			{
				"correct_path 1",
				"/Projects/tets_project/Jupyter/",
				true,
			},
			{
				"correct_path 2",
				"/Projects/tets_project/Jupyter/README.md",
				true,
			},
			{
				"correct_path 3",
				"/Projects/tets_project/Jupyter/test/README.md",
				true,
			},
			{
				"correct_path 4",
				"/hdfs/hadoop/Projects/tets_project/Jupyter/test/README.md",
				true,
			},
			{
				"wrong path 1",
				"/hdfs/tets_project/Jupyter/test/README.md",
				false,
			},
			{
				"wrong path 2",
				"hdfs/hadoop/Projects/tets_project/README.md",
				false,
			},
		}

		for _, tc := range cases {
			t.Run(fmt.Sprintf("%s path %v", tc.Name, tc.Path), func(t *testing.T) {
				groupFromPath := getGroupNameFromPath(tc.Path)
				if tc.IsCorrectPath {
					if groupFromPath == "" {
						t.Errorf("Error failed to find group name from path: %v: ", tc.Path)
					} else if groupFromPath != expectedPath {
						t.Errorf("Returned wrong group from path: %v: got %v", tc.Path, groupFromPath)
					}
				} else {
					if groupFromPath != "" {
						t.Errorf("Expected empty group from wrong path: %v: but got %v", tc.Path, groupFromPath)
					}
				}
			})
		}
	})
}
