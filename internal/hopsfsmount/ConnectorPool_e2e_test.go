// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

// End-to-end test of the connector pool. It requires a running HopsFS cluster
// reachable at testNameNodeAddress(): the connectors are real, and the
// lookups that keep them busy wait out the subtree locks the namenode holds
// for recursive deletes. No mocks.

package hopsfsmount

import (
	"fmt"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// Lookups under a directory being deleted fill the pool: three connections
// and twenty extra stop at twenty-three, and a lookup in another directory
// waits for one of them. Once the delete releases its subtree lock, each
// lookup under it returns its connector at its next retry, the waiting lookup
// completes, the next one takes milliseconds, and the pool keeps all
// twenty-three connectors.
func TestConnectorPoolFillsAndRecoversUnderASubtreeLock(t *testing.T) {
	const connections, extra = 3, 20
	const lookups = connections + extra + 1
	backend := connectBackend(t).(*HdfsAccessorImpl)
	base := uniqueBackendBase("pool")
	ensureBackendDir(t, backend, base+"/other", "")
	t.Cleanup(func() { _ = backend.MetadataClient.RemoveAll(base) })
	seedTree(t, base+"/tree", lookups, 3000)

	tlsEnabled, err := testTLSEnabled()
	if err != nil {
		t.Fatalf("testTLSEnabled: %v", err)
	}
	retryPolicy := NewDefaultRetryPolicy(WallClock{})
	retryPolicy.MaxAttempts = 1
	newConnector := func() (HdfsAccessor, error) {
		accessor, err := NewHdfsAccessor(testNameNodeAddress(), WallClock{}, TLSConfig{
			TLS:               tlsEnabled,
			RootCABundle:      RootCABundle,
			ClientCertificate: ClientCertificate,
			ClientKey:         ClientKey,
		})
		if err != nil {
			return nil, err
		}
		return NewFaultTolerantHdfsAccessor(accessor, retryPolicy), nil
	}
	var initial []HdfsAccessor
	for i := 0; i < connections; i++ {
		connector, err := newConnector()
		if err != nil {
			t.Fatalf("newConnector: %v", err)
		}
		initial = append(initial, connector)
	}
	fileSystem, err := NewFileSystem(initial, "/", []string{"*"}, false, DelaySyncUntilClose, retryPolicy, WallClock{},
		WithExtraConnectors(extra, newConnector))
	if err != nil {
		t.Fatalf("NewFileSystem: %v", err)
	}
	poolSize := func() int {
		fileSystem.hdfsAccessorsLock.Lock()
		defer fileSystem.hdfsAccessorsLock.Unlock()
		return len(fileSystem.HdfsAccessors)
	}
	t.Cleanup(func() {
		fileSystem.hdfsAccessorsLock.Lock()
		connectors := fileSystem.HdfsAccessors
		fileSystem.hdfsAccessorsLock.Unlock()
		for _, connector := range connectors {
			connector.Close()
		}
	})

	// The directory nodes a mount holds once the tree has been listed, one
	// per lookup: a lookup holds its directory's mutex.
	root, err := fileSystem.Root()
	if err != nil {
		t.Fatalf("Root: %v", err)
	}
	lookupDir := func(dirPath string) *DirINode {
		t.Helper()
		dir := root.(*DirINode)
		for _, name := range strings.Split(strings.TrimPrefix(dirPath, "/"), "/") {
			node, err := dir.Lookup(nil, name)
			if err != nil {
				t.Fatalf("lookup %s in %s: %v", name, dirPath, err)
			}
			dir = node.(*DirINode)
		}
		return dir
	}
	other := lookupDir(base + "/other")
	var dirs []*DirINode
	for d := 0; d < lookups; d++ {
		dirs = append(dirs, lookupDir(fmt.Sprintf("%s/tree/d%d", base, d)))
	}
	if n := poolSize(); n != connections {
		t.Fatalf("%d connectors before the delete, want %d", n, connections)
	}

	removal := removeTree(t, base+"/tree")
	waitForSubtreeLock(t, base+"/tree", removal)
	results := lookupNeverUnder(dirs)
	state := func() string {
		return fmt.Sprintf("delete %s, %d of %d lookups under it returned, %d connectors", removal, len(results), lookups, poolSize())
	}

	for deadline := time.Now().Add(10 * time.Second); poolSize() < connections+extra; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("want %d connectors with %d lookups under the subtree lock: %s", connections+extra, lookups, state())
		}
	}
	full := time.Now()

	// Every connector waits on the lock, so this lookup waits for one of them.
	_, err = other.Lookup(nil, "probe1")
	stalled := time.Since(full)
	if err != syscall.ENOENT {
		t.Fatalf("lookup in %s/other with the pool full: %v", base, err)
	}
	if stalled < 500*time.Millisecond {
		t.Fatalf("a lookup with the pool full took %v: it did not wait for a connector", stalled)
	}
	returnedBeforeProbe := len(results)

	select {
	case <-removal.done:
	case <-time.After(2 * time.Minute):
		t.Fatalf("the delete is still running: %s", state())
	}
	if removal.err != nil {
		t.Fatalf("delete %s/tree: %v", base, removal.err)
	}
	for i := 0; i < lookups; i++ {
		select {
		case err := <-results:
			if err != syscall.ENOENT {
				t.Fatalf("lookup under the deleted tree: %v", err)
			}
		case <-time.After(90 * time.Second):
			t.Fatalf("%d lookups under the deleted tree still waiting: %s", lookups-i, state())
		}
	}
	probe := time.Now()
	_, err = other.Lookup(nil, "probe2")
	took := time.Since(probe)
	if err != syscall.ENOENT {
		t.Fatalf("lookup in %s/other after the delete: %v", base, err)
	}
	if took > time.Second {
		t.Fatalf("a lookup after the lookups under the tree returned took %v", took)
	}
	if n := poolSize(); n != connections+extra {
		t.Fatalf("%d connectors after the delete, want %d", n, connections+extra)
	}
	t.Logf("pool full %v after the delete started, which took %v; a lookup then waited %v and got a connector when %d of %d lookups under the tree had returned; after all had, one took %v",
		full.Sub(removal.started), removal.at.Sub(removal.started), stalled, returnedBeforeProbe, lookups, took)
}

// seedTree makes dirPath with directories d0 to d<dirs-1> and inodes empty
// directories spread under them. A recursive delete takes time per inode.
func seedTree(t *testing.T, dirPath string, dirs, inodes int) {
	t.Helper()
	const workers = 16
	accessors := make([]HdfsAccessor, workers)
	for w := range accessors {
		accessors[w] = connectBackend(t)
	}
	defer func() {
		for _, accessor := range accessors {
			accessor.Close()
		}
	}()
	if err := accessors[0].Mkdir(dirPath, 0755); err != nil {
		t.Fatalf("mkdir %s: %v", dirPath, err)
	}
	for d := 0; d < dirs; d++ {
		if err := accessors[0].Mkdir(fmt.Sprintf("%s/d%d", dirPath, d), 0755); err != nil {
			t.Fatalf("mkdir %s/d%d: %v", dirPath, d, err)
		}
	}
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for w, accessor := range accessors {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := w; i < inodes; i += workers {
				if err := accessor.Mkdir(fmt.Sprintf("%s/d%d/e%d", dirPath, i%dirs, i), 0755); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	if err := <-errs; err != nil {
		t.Fatalf("seeding %s: %v", dirPath, err)
	}
}

type treeRemoval struct {
	started time.Time
	done    chan struct{}
	at      time.Time
	err     error
}

func (r *treeRemoval) String() string {
	select {
	case <-r.done:
		return fmt.Sprintf("done in %v (error %v)", r.at.Sub(r.started).Round(time.Millisecond), r.err)
	default:
		return fmt.Sprintf("running for %v", time.Since(r.started).Round(time.Millisecond))
	}
}

// removeTree deletes dirPath recursively on a connection of its own; at and
// err are set once done is closed.
func removeTree(t *testing.T, dirPath string) *treeRemoval {
	t.Helper()
	deleter := connectBackend(t).(*HdfsAccessorImpl)
	removal := &treeRemoval{started: time.Now(), done: make(chan struct{})}
	go func() {
		defer close(removal.done)
		removal.err = deleter.MetadataClient.RemoveAll(dirPath)
		removal.at = time.Now()
		deleter.Close()
	}()
	return removal
}

// waitForSubtreeLock returns once a stat under dirPath, on a connection of its
// own, blocks: the namenode answers one in milliseconds, but while the delete
// holds the subtree lock the client waits it out, its first retry 750 ms or
// more away. Fails if the delete ends first.
func waitForSubtreeLock(t *testing.T, dirPath string, removal *treeRemoval) {
	t.Helper()
	prober := connectBackend(t)
	t.Cleanup(func() { prober.Close() })
	for i := 0; ; i++ {
		answered := make(chan struct{})
		go func() {
			_, _ = prober.Stat(fmt.Sprintf("%s/probe%d", dirPath, i))
			close(answered)
		}()
		select {
		case <-answered:
		case <-time.After(250 * time.Millisecond):
			return
		}
		select {
		case <-removal.done:
			t.Fatalf("the delete of %s ended before a stat under it waited on its subtree lock: %s", dirPath, removal)
		default:
		}
	}
}

// lookupNeverUnder looks up a name that does not exist in each of dirs at
// once, through the mount's lookup, and sends each lookup's error.
func lookupNeverUnder(dirs []*DirINode) <-chan error {
	results := make(chan error, len(dirs))
	for _, dir := range dirs {
		go func() {
			_, err := dir.Lookup(nil, "never")
			results <- err
		}()
	}
	return results
}
