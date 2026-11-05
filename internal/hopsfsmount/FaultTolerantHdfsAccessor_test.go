// Copyright (c) Microsoft. All rights reserved.
// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package hopsfsmount

import (
	"errors"
	"os"
	"syscall"
	"testing"
	"time"

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
