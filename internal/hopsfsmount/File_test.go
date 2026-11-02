// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package hopsfsmount

import (
	"os"
	"syscall"
	"testing"
	"time"

	"bazil.org/fuse"
	"github.com/golang/mock/gomock"
	"github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
)

// A file the mount knows that the backend no longer has is the one ENOENT on
// a known file. The sidecar logs at warning level by default, so anything
// quieter would leave no trace of it.
func TestAttrWarnsWhenAKnownFileIsGoneFromTheBackend(t *testing.T) {
	hooks := logrus.StandardLogger().ReplaceHooks(make(logrus.LevelHooks))
	t.Cleanup(func() { logrus.StandardLogger().ReplaceHooks(hooks) })
	// The suite's integration tests leave the logger at ERROR, below which a
	// warning never reaches a hook.
	level := logrus.GetLevel()
	logrus.SetLevel(logrus.WarnLevel)
	t.Cleanup(func() { logrus.SetLevel(level) })
	logs := logtest.NewGlobal()

	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	hdfsAccessor.EXPECT().IsAvailable().Return(true).AnyTimes()
	fs, _ := NewFileSystem([]HdfsAccessor{hdfsAccessor}, "/", []string{"*"}, false, DelaySyncUntilClose, NewDefaultRetryPolicy(mockClock), mockClock)
	root, _ := fs.Root()
	file := root.(*DirINode).addOrUpdateChildInodeAttrs("unit_test", "gone", Attrs{Name: "gone", Mode: os.FileMode(0644), Expires: mockClock.Now()}).(*FileINode)
	mockClock.NotifyTimeElapsed(time.Second)

	hdfsAccessor.EXPECT().Stat("/gone").Return(Attrs{}, syscall.ENOENT)
	var attr fuse.Attr
	assert.Equal(t, syscall.ENOENT, file.Attr(nil, &attr))

	var warnings []string
	for _, entry := range logs.AllEntries() {
		if entry.Level == logrus.WarnLevel {
			warnings = append(warnings, entry.Message)
		}
	}
	assert.Equal(t, []string{"Known file not found on backend"}, warnings)
}
