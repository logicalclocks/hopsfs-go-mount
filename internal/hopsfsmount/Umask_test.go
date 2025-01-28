package hopsfsmount

import (
	"bazil.org/fuse"
	"crypto/rand"
	"errors"
	"fmt"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sys/unix"
	"math/big"
	"os"
	"testing"
)

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func TestUmaskValidation(t *testing.T) {
	cases := []struct {
		TestUmask     string
		ExpectedUmask os.FileMode
		ExpectedError error
	}{
		{
			"000", os.FileMode(0000), nil,
		},
		{"777", os.FileMode(0777), nil},
		{
			"0000", os.FileMode(0000), nil,
		},
		{"0777", os.FileMode(0777), nil},
		{"00000", os.FileMode(0), errors.New("umask must be exactly 3 or 4 digits")},
		{"7777", os.FileMode(0), errors.New("umask must be within the range 0000 to 0777")},
		{"abc", os.FileMode(0), errors.New("umask must contain only digits")},
		{"a777", os.FileMode(0), errors.New("umask must contain only digits")},
		{"1777", os.FileMode(0), errors.New("umask must be within the range 0000 to 0777")},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("Test umask %s ", tc.TestUmask), func(t *testing.T) {
			actualUmask, actualError := ValidateUmask(tc.TestUmask)
			if actualUmask != tc.ExpectedUmask {
				t.Errorf("Wrong umask for test: %s, expected: %v, got: %v",
					tc.TestUmask, tc.ExpectedUmask, actualUmask)
			}
			if actualError == nil && tc.ExpectedError != nil {
				t.Errorf("Expected error for test: %s, got none", tc.TestUmask)
			} else if actualError != nil && tc.ExpectedError == nil {
				t.Errorf("Expected no error for test: %s, got: %v", tc.TestUmask, actualError)
			} else if actualError != nil && tc.ExpectedError != nil && actualError.Error() != tc.ExpectedError.Error() {
				t.Errorf("Wrong error for test: %s, expected: %v, got: %v", tc.TestUmask, tc.ExpectedError, actualError)
			}
		})
	}
}

func TestUmaskBasedPermissions(t *testing.T) {
	cases := []struct {
		TestUmask              string
		ExpectedUmask          os.FileMode
		DefaultPermissions     os.FileMode
		ExpectedFilePermission os.FileMode
		Dir                    bool
	}{
		{
			"0007", os.FileMode(0007), os.FileMode(0744), os.FileMode(0760), true,
		},
		{
			"0077", os.FileMode(0077), os.FileMode(0744), os.FileMode(0700), false,
		},
		{
			"0022", os.FileMode(0022), os.FileMode(0775), os.FileMode(0755), false,
		},
	}

	currentUmask := unix.Umask(0)
	defer ResetUmask(currentUmask)

	for _, tc := range cases {
		t.Run(fmt.Sprintf("Test umask %s ", tc.TestUmask), func(t *testing.T) {
			setUmask, err := ValidateUmask(tc.TestUmask)
			assert.Nil(t, err)
			assert.Equal(t, tc.ExpectedUmask, setUmask)
			Umask = setUmask
			UserUmask = tc.TestUmask
			mockCtrl := gomock.NewController(t)
			mockClock := &MockClock{}
			hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
			dir, err := GenerateTestDir()
			assert.Nil(t, err)
			dir = "/" + dir
			hdfsAccessor.EXPECT().Chown(dir, gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			// We should expect the file or dir to be created with the expected permissions in hopsfs
			expected := ComputePermissions(tc.ExpectedFilePermission)
			if tc.Dir {
				expected = ComputePermissions(tc.ExpectedFilePermission) | os.ModeDir
			}
			hdfsAccessor.EXPECT().Mkdir(dir, expected).Return(nil).AnyTimes()
			fs, _ := NewFileSystem([]HdfsAccessor{hdfsAccessor}, "/", []string{"*"}, false, NewDefaultRetryPolicy(mockClock), mockClock)
			root, _ := fs.Root()
			defaultPermissions := tc.DefaultPermissions
			if tc.Dir {
				defaultPermissions = tc.DefaultPermissions | os.ModeDir
			}
			_, err = root.(*DirINode).Mkdir(nil, &fuse.MkdirRequest{Name: dir, Mode: defaultPermissions})
			// If no errors then the files or dirs are being created with the expected permissions otherwise the our mock would fail
			assert.Nil(t, err)
		})
	}

}

func ResetUmask(umask int) {
	unix.Umask(umask)
	UserUmask = ""
}

func GenerateTestDir() (string, error) {
	length := 16
	result := make([]byte, length)
	for i := 0; i < length; i++ {
		index, err := rand.Int(rand.Reader, big.NewInt(int64(len(letters))))
		if err != nil {
			return "", err
		}
		result[i] = letters[index.Int64()]
	}
	return string(result), nil
}
