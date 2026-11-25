// Copyright (c) Microsoft. All rights reserved.
// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package hopsfsmount

import (
	"errors"
	"io"
	"os"
	"syscall"
	"testing"
	"time"

	"bazil.org/fuse"
	"github.com/colinmarc/hdfs/v2"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

// Testing retry logic for EnsureConnected()
func TestEnsureConnectedWithRetries(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	hdfsAccessor.EXPECT().TryReserve().Return(true).AnyTimes()
	ftHdfsAccessor := NewFaultTolerantHdfsAccessor(hdfsAccessor, atMost2Attempts())
	hdfsAccessor.EXPECT().EnsureConnected().Return(errors.New("Injected failure"))
	hdfsAccessor.EXPECT().EnsureConnected().Return(nil)
	err := ftHdfsAccessor.EnsureConnected()
	assert.Nil(t, err)
}

// Testing retry logic for Stat()
func TestStatWithRetries(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	hdfsAccessor.EXPECT().TryReserve().Return(true).AnyTimes()
	ftHdfsAccessor := NewFaultTolerantHdfsAccessor(hdfsAccessor, atMost2Attempts())
	hdfsAccessor.EXPECT().Stat("/test/file").Return(Attrs{}, errors.New("Injected failure"))
	hdfsAccessor.EXPECT().Stat("/test/file").Return(Attrs{Name: "file"}, nil)
	hdfsAccessor.EXPECT().Close().Return(nil)
	attrs, err := ftHdfsAccessor.Stat("/test/file")
	assert.Nil(t, err)
	assert.Equal(t, "file", attrs.Name)
}

// Testing retry logic for Mkdir()
func TestMkdirWithRetries(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	hdfsAccessor.EXPECT().TryReserve().Return(true).AnyTimes()
	ftHdfsAccessor := NewFaultTolerantHdfsAccessor(hdfsAccessor, atMost2Attempts())
	hdfsAccessor.EXPECT().Mkdir("/test/dir", os.FileMode(0757)).Return(errors.New("Injected failure"))
	hdfsAccessor.EXPECT().Mkdir("/test/dir", os.FileMode(0757)).Return(nil)
	hdfsAccessor.EXPECT().Close().Return(nil)
	err := ftHdfsAccessor.Mkdir("/test/dir", os.FileMode(0757))
	assert.Nil(t, err)
}

// Testing retry logic for ReadDir()
func TestReadDirWithRetries(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	hdfsAccessor.EXPECT().TryReserve().Return(true).AnyTimes()
	ftHdfsAccessor := NewFaultTolerantHdfsAccessor(hdfsAccessor, atMost2Attempts())
	var result []Attrs
	var err error
	hdfsAccessor.EXPECT().ReadDir("/test/dir").Return(nil, errors.New("Injected failure"))
	hdfsAccessor.EXPECT().ReadDir("/test/dir").Return(make([]Attrs, 10), nil)
	hdfsAccessor.EXPECT().Close().Return(nil)
	result, err = ftHdfsAccessor.ReadDir("/test/dir")
	assert.Nil(t, err)
	assert.Equal(t, 10, len(result))
}

// A subtree lock is the namenode's answer over a healthy connection: the retry
// keeps the client, and with it the readers sharing its connection. The mock
// refuses any call not expected, so a Close here fails the test.
func TestStatRetriesASubtreeLockWithoutReconnecting(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	hdfsAccessor.EXPECT().TryReserve().Return(true).AnyTimes()
	ftHdfsAccessor := NewFaultTolerantHdfsAccessor(hdfsAccessor, atMost2Attempts())
	hdfsAccessor.EXPECT().Stat("/test/file").Return(Attrs{}, syscall.EBUSY)
	hdfsAccessor.EXPECT().Stat("/test/file").Return(Attrs{Name: "file"}, nil)
	attrs, err := ftHdfsAccessor.Stat("/test/file")
	assert.Nil(t, err)
	assert.Equal(t, "file", attrs.Name)
}

// Once the policy's attempts are spent the lock is reported as it is.
func TestStatReturnsEBUSYWhenTheSubtreeStaysLocked(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	hdfsAccessor.EXPECT().TryReserve().Return(true).AnyTimes()
	ftHdfsAccessor := NewFaultTolerantHdfsAccessor(hdfsAccessor, atMost2Attempts())
	hdfsAccessor.EXPECT().Stat("/test/file").Return(Attrs{}, syscall.EBUSY).Times(2)
	_, err := ftHdfsAccessor.Stat("/test/file")
	assert.Equal(t, syscall.EBUSY, err)
}

// generates a test retry policy which allows 2 attempst
func atMost2Attempts() *RetryPolicy {
	clock := &MockClock{}
	rp := NewDefaultRetryPolicy(clock)
	rp.MaxAttempts = 2
	rp.TimeLimit = time.Hour
	return rp
}

// connectionLost is the error the accessor returns for a namenode call that
// failed on its connection: the client's ConnectionError, translated.
func connectionLost(sent bool) error {
	return unwrapAndTranslateError(&os.PathError{Op: "stat", Path: "/test", Err: &hdfs.ConnectionError{Method: "getFileInfo", Sent: sent, Err: io.EOF}})
}

// An idle namenode connection closed by the far side reads end of stream. The
// client reports it as a ConnectionError, which the mount must retry on a new
// client and never pass on as a benign end of file (EIO to the user).
func TestMetadataEOFIsAConnectionFailure(t *testing.T) {
	for _, sent := range []bool{false, true} {
		err := connectionLost(sent)
		assert.False(t, IsSuccessOrNonRetriableError(err), "sent=%v", sent)
		assert.True(t, needsReconnect(err), "sent=%v", sent)
		assert.NotEqual(t, io.EOF, err)
		assert.Equal(t, fuse.EIO, fuse.ToErrno(err))
	}

	mockCtrl := gomock.NewController(t)
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	hdfsAccessor.EXPECT().TryReserve().Return(true).AnyTimes()
	ftHdfsAccessor := NewFaultTolerantHdfsAccessor(hdfsAccessor, atMost2Attempts())
	hdfsAccessor.EXPECT().Stat("/test/file").Return(Attrs{}, connectionLost(true))
	hdfsAccessor.EXPECT().Close().Return(nil)
	hdfsAccessor.EXPECT().Stat("/test/file").Return(Attrs{Name: "file"}, nil)
	attrs, err := ftHdfsAccessor.Stat("/test/file")
	assert.Nil(t, err)
	assert.Equal(t, "file", attrs.Name)
}

// A change whose request was sent and got no answer may have run, so it is
// not sent again. The mock refuses a second call.
func TestChangesAreNotRetriedAfterTheRequestWasSent(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	hdfsAccessor.EXPECT().TryReserve().Return(true).AnyTimes()
	ftHdfsAccessor := NewFaultTolerantHdfsAccessor(hdfsAccessor, atMost2Attempts())

	hdfsAccessor.EXPECT().Mkdir("/d", os.FileMode(0755)).Return(connectionLost(true))
	assert.Error(t, ftHdfsAccessor.Mkdir("/d", os.FileMode(0755)))
	hdfsAccessor.EXPECT().Remove("/d").Return(connectionLost(true))
	assert.Error(t, ftHdfsAccessor.Remove("/d"))
	hdfsAccessor.EXPECT().Rename("/a", "/b").Return(connectionLost(true))
	assert.Error(t, ftHdfsAccessor.Rename("/a", "/b"))
	hdfsAccessor.EXPECT().Rename2("/a", "/b", hdfs.RenameOptions(hdfs.RENAME_OPTION_NONE)).Return(connectionLost(true))
	assert.Error(t, ftHdfsAccessor.Rename2("/a", "/b", hdfs.RenameOptions(hdfs.RENAME_OPTION_NONE)))
	hdfsAccessor.EXPECT().CreateFile("/f", os.FileMode(0644), false).Return(nil, connectionLost(true))
	_, err := ftHdfsAccessor.CreateFile("/f", os.FileMode(0644), false)
	assert.Error(t, err)
}

// A change whose request never went out is safe to send on a new client.
func TestChangesAreRetriedWhenTheRequestWasNotSent(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	hdfsAccessor.EXPECT().TryReserve().Return(true).AnyTimes()
	ftHdfsAccessor := NewFaultTolerantHdfsAccessor(hdfsAccessor, atMost2Attempts())

	gomock.InOrder(
		hdfsAccessor.EXPECT().Mkdir("/d", os.FileMode(0755)).Return(connectionLost(false)),
		hdfsAccessor.EXPECT().Close().Return(nil),
		hdfsAccessor.EXPECT().Mkdir("/d", os.FileMode(0755)).Return(nil),
	)
	assert.Nil(t, ftHdfsAccessor.Mkdir("/d", os.FileMode(0755)))

	gomock.InOrder(
		hdfsAccessor.EXPECT().CreateFile("/f", os.FileMode(0644), false).Return(nil, connectionLost(false)),
		hdfsAccessor.EXPECT().Close().Return(nil),
		hdfsAccessor.EXPECT().CreateFile("/f", os.FileMode(0644), false).Return(nil, nil),
	)
	_, err := ftHdfsAccessor.CreateFile("/f", os.FileMode(0644), false)
	assert.Nil(t, err)
}
