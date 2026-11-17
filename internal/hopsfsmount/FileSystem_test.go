// Copyright (c) Microsoft. All rights reserved.
// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package hopsfsmount

import (
	"sync/atomic"
	"testing"
	"time"

	"bazil.org/fuse"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestIsPathAllowedForStarPrefix(t *testing.T) {
	fs, _ := NewFileSystem(nil, "/", []string{"*"}, false, DelaySyncUntilClose, NewDefaultRetryPolicy(WallClock{}), WallClock{})
	assert.True(t, fs.IsPathAllowed("/"))
	assert.True(t, fs.IsPathAllowed("/foo"))
	assert.True(t, fs.IsPathAllowed("/foo/bar"))
}

func TestIsPathAllowedForMiscPrefixes(t *testing.T) {
	fs, _ := NewFileSystem(nil, "/", []string{"foo", "bar", "baz/qux"}, false, DelaySyncUntilClose, NewDefaultRetryPolicy(WallClock{}), WallClock{})
	assert.True(t, fs.IsPathAllowed("/"))
	assert.True(t, fs.IsPathAllowed("/foo"))
	assert.True(t, fs.IsPathAllowed("/bar"))
	assert.True(t, fs.IsPathAllowed("/foo/x"))
	assert.True(t, fs.IsPathAllowed("/baz/qux"))
	assert.True(t, fs.IsPathAllowed("/baz/qux/y"))
	assert.False(t, fs.IsPathAllowed("qux"))
	assert.False(t, fs.IsPathAllowed("w/z"))
}

func TestStatfs(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	hdfsAccessor.EXPECT().TryReserve().Return(true).AnyTimes()
	fs, _ := NewFileSystem([]HdfsAccessor{hdfsAccessor}, "/", []string{"*"}, false, DelaySyncUntilClose, NewDefaultRetryPolicy(mockClock), mockClock)

	hdfsAccessor.EXPECT().StatFs().Return(FsInfo{capacity: uint64(10240), remaining: uint64(1024)}, nil)
	fsInfo := &fuse.StatfsResponse{}
	err := fs.Statfs(nil, &fuse.StatfsRequest{}, fsInfo)
	assert.Nil(t, err)
	assert.Equal(t, uint64(10), fsInfo.Blocks)
	assert.Equal(t, uint64(1), fsInfo.Bfree)
}

// A connector busy for the whole of a call, as one waiting out a subtree
// operation is, must not hold up a request on another path: while every
// connector is busy the file system adds one, up to the extra it was given,
// and only at that limit does a request wait for a connector to free.
func TestGetDFSConnectorAddsAConnectorWhileAllAreBusy(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	busy := NewMockHdfsAccessor(mockCtrl)
	busy.EXPECT().TryReserve().Return(false).AnyTimes()
	var freed atomic.Bool
	freesLater := NewMockHdfsAccessor(mockCtrl)
	freesLater.EXPECT().TryReserve().DoAndReturn(func() bool { return freed.Load() }).AnyTimes()
	added := NewMockHdfsAccessor(mockCtrl)
	added.EXPECT().TryReserve().Return(false).AnyTimes()

	made := 0
	fs, err := NewFileSystem([]HdfsAccessor{busy, freesLater}, "/", []string{"*"}, false, DelaySyncUntilClose, NewDefaultRetryPolicy(&MockClock{}), &MockClock{},
		WithExtraConnectors(1, func() (HdfsAccessor, error) { made++; return added, nil }))
	assert.Nil(t, err)

	assert.Same(t, added, fs.getDFSConnector())
	assert.Equal(t, 1, made)
	assert.Equal(t, 3, len(fs.HdfsAccessors))

	// At the limit: the request waits until a connector frees.
	got := make(chan HdfsAccessor, 1)
	go func() { got <- fs.getDFSConnector() }()
	select {
	case c := <-got:
		t.Fatalf("got a connector while all were busy and the limit was reached: %v", c)
	case <-time.After(100 * time.Millisecond):
	}
	freed.Store(true)
	select {
	case c := <-got:
		assert.Same(t, freesLater, c)
	case <-time.After(5 * time.Second):
		t.Fatal("the request did not get the freed connector")
	}
	assert.Equal(t, 1, made)
}

// Requests arriving together while every connector is busy add one connector
// each, never past the extra allowed: three connections with twenty extra end
// at twenty-three, and the requests past that wait for a connector to free.
func TestGetDFSConnectorStopsAtConnectionsPlusExtra(t *testing.T) {
	const connections, extra, requests = 3, 20, 50
	mockCtrl := gomock.NewController(t)
	var freed atomic.Bool
	newConnector := func() HdfsAccessor {
		connector := NewMockHdfsAccessor(mockCtrl)
		connector.EXPECT().TryReserve().DoAndReturn(func() bool { return freed.Load() }).AnyTimes()
		return connector
	}
	var initial []HdfsAccessor
	for i := 0; i < connections; i++ {
		initial = append(initial, newConnector())
	}
	var made atomic.Int32
	fs, err := NewFileSystem(initial, "/", []string{"*"}, false, DelaySyncUntilClose, NewDefaultRetryPolicy(&MockClock{}), &MockClock{},
		WithExtraConnectors(extra, func() (HdfsAccessor, error) { made.Add(1); return newConnector(), nil }))
	assert.Nil(t, err)

	got := make(chan HdfsAccessor, requests)
	for i := 0; i < requests; i++ {
		go func() { got <- fs.getDFSConnector() }()
	}
	// While none is free, a request can only get a connector it added.
	added := map[HdfsAccessor]bool{}
	for i := 0; i < extra; i++ {
		select {
		case c := <-got:
			added[c] = true
		case <-time.After(5 * time.Second):
			t.Fatalf("%d of %d connectors added", i, extra)
		}
	}
	select {
	case c := <-got:
		t.Fatalf("got a connector past the limit while all were busy: %v", c)
	case <-time.After(100 * time.Millisecond):
	}
	assert.Equal(t, extra, len(added))
	assert.Equal(t, int32(extra), made.Load())

	freed.Store(true)
	for i := extra; i < requests; i++ {
		select {
		case <-got:
		case <-time.After(5 * time.Second):
			t.Fatalf("%d requests still waiting after the connectors freed", requests-i)
		}
	}
	assert.Equal(t, int32(extra), made.Load())
	assert.Equal(t, connections+extra, len(fs.HdfsAccessors))
}

// A request waiting for a connector to free sleeps between scans of the pool
// rather than spinning on its lock: the connectors can stay in a call for
// minutes, under a subtree operation.
func TestGetDFSConnectorWaitsForAConnectorWithoutSpinning(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	var freed atomic.Bool
	var scans atomic.Int32
	busy := NewMockHdfsAccessor(mockCtrl)
	busy.EXPECT().TryReserve().DoAndReturn(func() bool { scans.Add(1); return freed.Load() }).AnyTimes()
	fs, err := NewFileSystem([]HdfsAccessor{busy}, "/", []string{"*"}, false, DelaySyncUntilClose, NewDefaultRetryPolicy(&MockClock{}), &MockClock{})
	assert.Nil(t, err)

	got := make(chan HdfsAccessor, 1)
	go func() { got <- fs.getDFSConnector() }()
	time.Sleep(200 * time.Millisecond)
	// Scans a millisecond apart, backing off to ten, make about 23 in 200 ms.
	assert.Less(t, scans.Load(), int32(50), "the waiting request scanned the pool without sleeping")

	freed.Store(true)
	select {
	case c := <-got:
		assert.Same(t, busy, c)
	case <-time.After(time.Second):
		t.Fatal("the request did not get the freed connector")
	}
}

// A free connector is used before one is added, while there is still room to
// add one.
func TestGetDFSConnectorUsesAFreeConnectorBeforeAddingOne(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	free := NewMockHdfsAccessor(mockCtrl)
	free.EXPECT().TryReserve().Return(true).AnyTimes()
	busy := NewMockHdfsAccessor(mockCtrl)
	busy.EXPECT().TryReserve().Return(false).AnyTimes()
	fs, err := NewFileSystem([]HdfsAccessor{free, busy}, "/", []string{"*"}, false, DelaySyncUntilClose, NewDefaultRetryPolicy(&MockClock{}), &MockClock{},
		WithExtraConnectors(20, func() (HdfsAccessor, error) { t.Error("added a connector while one was free"); return busy, nil }))
	assert.Nil(t, err)

	// Each request comes to the busy connector first in the round robin.
	assert.Same(t, free, fs.getDFSConnector())
	assert.Same(t, free, fs.getDFSConnector())
	assert.Equal(t, 2, len(fs.HdfsAccessors))
}

// A connector is reserved for the call of the request it is handed to, so a
// second request arriving before that call starts gets another connector.
// The call ends the reservation; one no call takes up lapses.
func TestGetDFSConnectorReservesTheConnectorItHandsOut(t *testing.T) {
	clock := &MockClock{now: time.Now()}
	newConnector := func() (HdfsAccessor, error) { return NewHdfsAccessor("localhost:8020", clock, TLSConfig{}) }
	first, _ := newConnector()
	fs, err := NewFileSystem([]HdfsAccessor{first}, "/", []string{"*"}, false, DelaySyncUntilClose, NewDefaultRetryPolicy(clock), clock,
		WithExtraConnectors(1, newConnector))
	assert.Nil(t, err)

	a := fs.getDFSConnector()
	b := fs.getDFSConnector()
	assert.NotSame(t, a, b)
	assert.Equal(t, 2, len(fs.HdfsAccessors))

	a.(*HdfsAccessorImpl).lockHadoopClient()
	a.(*HdfsAccessorImpl).unlockHadoopClient()
	assert.Same(t, a, fs.getDFSConnector())

	clock.NotifyTimeElapsed(reservationTimeout)
	c := fs.getDFSConnector()
	assert.True(t, c == a || c == b)
	assert.Equal(t, 2, len(fs.HdfsAccessors))
}

// interleavingClock runs hook on its next Now, which TryReserve reads between
// its check and its reservation.
type interleavingClock struct {
	MockClock
	hook func()
}

func (c *interleavingClock) Now() time.Time {
	if hook := c.hook; hook != nil {
		c.hook = nil
		hook()
	}
	return c.MockClock.Now()
}

// A call that starts while a request is reserving the connector keeps it:
// the request is refused rather than handed a connector already in a call.
func TestTryReserveRefusesAConnectorWhoseCallStartsMeanwhile(t *testing.T) {
	clock := &interleavingClock{MockClock: MockClock{now: time.Now()}}
	accessor, _ := NewHdfsAccessor("localhost:8020", clock, TLSConfig{})
	connector := accessor.(*HdfsAccessorImpl)
	assert.True(t, connector.TryReserve())

	locked, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	clock.hook = func() {
		// The request holding the reservation starts its call.
		go func() {
			connector.lockHadoopClient()
			close(locked)
			<-release
			connector.unlockHadoopClient()
			close(done)
		}()
		select {
		case <-locked:
		case <-time.After(100 * time.Millisecond):
		}
	}
	assert.False(t, connector.TryReserve())
	<-locked
	assert.False(t, connector.TryReserve())
	close(release)
	<-done
	assert.True(t, connector.TryReserve())
}

// Without extra connectors the selector is a round robin over the
// connectors it has, waiting when all are busy.
func TestGetDFSConnectorRoundRobinWithoutExtras(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	first := NewMockHdfsAccessor(mockCtrl)
	first.EXPECT().TryReserve().Return(true).AnyTimes()
	second := NewMockHdfsAccessor(mockCtrl)
	second.EXPECT().TryReserve().Return(true).AnyTimes()
	fs, err := NewFileSystem([]HdfsAccessor{first, second}, "/", []string{"*"}, false, DelaySyncUntilClose, NewDefaultRetryPolicy(&MockClock{}), &MockClock{})
	assert.Nil(t, err)

	assert.Same(t, second, fs.getDFSConnector())
	assert.Same(t, first, fs.getDFSConnector())
	assert.Same(t, second, fs.getDFSConnector())
	assert.Equal(t, 2, len(fs.HdfsAccessors))
}
