// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package hopsfsmount

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// These tests run the hopsfs-mount binary against a live HopsFS, as the
// sidecar does, and send it SIGTERM with -exitTimeout set.

const exitTestTimeout = 4 * time.Second

type mountProcess struct {
	cmd    *exec.Cmd
	dir    string
	output *bytes.Buffer
	done   chan struct{}
	err    error
}

// buildMountBinary returns HOPSFS_MOUNT_BIN when set (a host without a Go
// toolchain), else builds ./cmd.
func buildMountBinary(t *testing.T) string {
	t.Helper()
	if bin := strings.TrimSpace(os.Getenv("HOPSFS_MOUNT_BIN")); bin != "" {
		return bin
	}
	bin := filepath.Join(t.TempDir(), "hopsfs-mount")
	build := exec.Command("go", "build", "-tags", "osusergo,netgo", "-o", bin, "hopsworks.ai/hopsfsmount/cmd")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build of the mount binary failed: %v\n%s", err, out)
	}
	return bin
}

func startMountProcess(t *testing.T, bin string, srcDir string) *mountProcess {
	t.Helper()
	userName, err := testConnectionUserName()
	if err != nil {
		t.Fatalf("%v", err)
	}
	tlsEnabled, err := testTLSEnabled()
	if err != nil {
		t.Fatalf("%v", err)
	}
	p := &mountProcess{dir: t.TempDir(), output: &bytes.Buffer{}, done: make(chan struct{})}
	p.cmd = exec.Command(bin,
		"-exitTimeout", exitTestTimeout.String(),
		"-logLevel", "warn",
		"-stageDir", t.TempDir(),
		"-srcDir", srcDir,
		"-hopsFSUserName", userName,
		"-tls="+strconv.FormatBool(tlsEnabled),
		"-rootCABundle", RootCABundle,
		"-clientCertificate", ClientCertificate,
		"-clientKey", ClientKey,
		testNameNodeAddress(), p.dir)
	p.cmd.Stdout = p.output
	p.cmd.Stderr = p.output
	if err := p.cmd.Start(); err != nil {
		t.Fatalf("start of the mount binary failed: %v", err)
	}
	go func() {
		p.err = p.cmd.Wait()
		close(p.done)
	}()
	t.Cleanup(func() {
		select {
		case <-p.done:
		default:
			p.cmd.Process.Kill()
			<-p.done
			exec.Command("fusermount3", "-u", p.dir).Run()
		}
	})

	deadline := time.Now().Add(30 * time.Second)
	for !isMounted(t, p.dir) {
		select {
		case <-p.done:
			t.Fatalf("the mount binary exited before mounting: %v\n%s", p.err, p.output.String())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s was not mounted within 30 s\n%s", p.dir, p.output.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
	return p
}

func isMounted(t *testing.T, dir string) bool {
	t.Helper()
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		t.Fatalf("%v", err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) > 4 && fields[4] == dir {
			return true
		}
	}
	return false
}

// waitExit returns the exit code and how long after since the process ended.
func (p *mountProcess) waitExit(t *testing.T, since time.Time, bound time.Duration) (int, time.Duration) {
	t.Helper()
	select {
	case <-p.done:
	case <-time.After(bound):
		t.Fatalf("the mount binary did not exit within %s of SIGTERM\n%s", bound, p.output.String())
	}
	elapsed := time.Since(since)
	if p.err == nil {
		return 0, elapsed
	}
	var exitErr *exec.ExitError
	if errors.As(p.err, &exitErr) {
		return exitErr.ExitCode(), elapsed
	}
	t.Fatalf("wait on the mount binary failed: %v", p.err)
	return -1, elapsed
}

// newTestBase makes a fresh directory under /tmp in HopsFS for one test and
// removes it by its exact path at the end. The tests write only below it.
func newTestBase(t *testing.T, accessor HdfsAccessor) string {
	t.Helper()
	base := "/tmp/" + uniqueTestName("exit_on_signal")
	if err := accessor.Mkdir(base, 0700); err != nil {
		t.Fatalf("mkdir of %s in HopsFS failed: %v", base, err)
	}
	t.Cleanup(func() {
		if err := accessor.Remove(base); err != nil {
			t.Errorf("remove of %s in HopsFS failed: %v", base, err)
		}
	})
	t.Logf("HopsFS base %s", base)
	return base
}

func newTestAccessor(t *testing.T) HdfsAccessor {
	t.Helper()
	if err := InitConnectionUser(); err != nil {
		t.Fatalf("Error/InitConnectionUser: %v", err)
	}
	tlsEnabled, err := testTLSEnabled()
	if err != nil {
		t.Fatalf("%v", err)
	}
	accessor, err := NewHdfsAccessor(testNameNodeAddress(), WallClock{}, TLSConfig{TLS: tlsEnabled, RootCABundle: RootCABundle, ClientCertificate: ClientCertificate, ClientKey: ClientKey})
	if err != nil {
		t.Fatalf("Error/NewHdfsAccessor: %v", err)
	}
	if err := accessor.EnsureConnected(); err != nil {
		t.Fatalf("Error/EnsureConnected: %v", err)
	}
	return accessor
}

// writeRandom writes size random bytes to f and returns their sha256.
func writeRandom(t *testing.T, f *os.File, size int64) string {
	t.Helper()
	hash := sha256.New()
	if _, err := io.CopyN(io.MultiWriter(f, hash), rand.Reader, size); err != nil {
		t.Fatalf("write to %s failed: %v", f.Name(), err)
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func assertInHopsFS(t *testing.T, accessor HdfsAccessor, path string, size int64, sum string) {
	t.Helper()
	attrs, err := accessor.Stat(path)
	if err != nil {
		t.Fatalf("stat of %s in HopsFS failed: %v", path, err)
	}
	if int64(attrs.Size) != size {
		t.Fatalf("%s in HopsFS has %d bytes, want %d", path, attrs.Size, size)
	}
	r, err := accessor.OpenRead(path)
	if err != nil {
		t.Fatalf("open of %s in HopsFS failed: %v", path, err)
	}
	defer r.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, r); err != nil {
		t.Fatalf("read of %s in HopsFS failed: %v", path, err)
	}
	if got := hex.EncodeToString(hash.Sum(nil)); got != sum {
		t.Fatalf("%s in HopsFS has sha256 %s, want %s", path, got, sum)
	}
}

// A file written and closed right before SIGTERM is whole in HopsFS, and the
// process exits 0 within 1 s.
func TestExitOnSignalAfterClose(t *testing.T) {
	bin := buildMountBinary(t)
	accessor := newTestAccessor(t)
	base := newTestBase(t, accessor)
	p := startMountProcess(t, bin, base)

	name := uniqueTestName("exit_after_close")
	hopsPath := base + "/" + name
	t.Cleanup(func() {
		if err := accessor.Remove(hopsPath); err != nil {
			t.Errorf("remove of %s in HopsFS failed: %v", hopsPath, err)
		}
	})

	const size = 1 << 20
	f, err := os.Create(filepath.Join(p.dir, name))
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}
	sum := writeRandom(t, f, size)
	if err := f.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	termAt := time.Now()
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("SIGTERM failed: %v", err)
	}
	code, elapsed := p.waitExit(t, termAt, exitTestTimeout+time.Second)
	if code != 0 {
		t.Fatalf("exit code %d, want 0\n%s", code, p.output.String())
	}
	if elapsed > time.Second {
		t.Fatalf("exit %s after SIGTERM, want under 1 s\n%s", elapsed, p.output.String())
	}
	if !strings.Contains(p.output.String(), "Exiting on terminated with every write in HopsFS") {
		t.Fatalf("no exit line in the output:\n%s", p.output.String())
	}
	assertInHopsFS(t, accessor, hopsPath, size, sum)
}

// SIGTERM while close() is uploading the file: the process waits for the
// upload, close() succeeds, the file is whole in HopsFS, and the exit code is 0.
func TestExitOnSignalDuringFlush(t *testing.T) {
	bin := buildMountBinary(t)
	accessor := newTestAccessor(t)
	base := newTestBase(t, accessor)
	p := startMountProcess(t, bin, base)

	name := uniqueTestName("exit_during_flush")
	hopsPath := base + "/" + name
	t.Cleanup(func() {
		if err := accessor.Remove(hopsPath); err != nil {
			t.Errorf("remove of %s in HopsFS failed: %v", hopsPath, err)
		}
	})

	const size = 64 << 20
	f, err := os.Create(filepath.Join(p.dir, name))
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}
	sum := writeRandom(t, f, size)

	var wg sync.WaitGroup
	var closeErr error
	var closedAt time.Time
	wg.Add(1)
	go func() {
		defer wg.Done()
		closeErr = f.Close()
		closedAt = time.Now()
	}()
	time.Sleep(50 * time.Millisecond)
	termAt := time.Now()
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("SIGTERM failed: %v", err)
	}
	code, _ := p.waitExit(t, termAt, exitTestTimeout+time.Second)
	wg.Wait()

	if !closedAt.After(termAt) {
		t.Fatalf("close returned %s before SIGTERM: the upload was not in flight, the test proves nothing", termAt.Sub(closedAt))
	}
	if closeErr != nil {
		t.Fatalf("close failed: %v\n%s", closeErr, p.output.String())
	}
	if code != 0 {
		t.Fatalf("exit code %d, want 0\n%s", code, p.output.String())
	}
	assertInHopsFS(t, accessor, hopsPath, size, sum)
}

// SIGTERM while a writer holds a dirty, unflushed file and no handler runs
// (the kernel flushing a killed process's fds after the sidecar's TERM): the
// process waits for the FLUSH, close() succeeds, the file is whole, exit 0.
func TestExitOnSignalFlushAfterTerm(t *testing.T) {
	bin := buildMountBinary(t)
	accessor := newTestAccessor(t)
	base := newTestBase(t, accessor)
	p := startMountProcess(t, bin, base)

	name := uniqueTestName("exit_flush_after_term")
	hopsPath := base + "/" + name
	t.Cleanup(func() {
		if err := accessor.Remove(hopsPath); err != nil {
			t.Errorf("remove of %s in HopsFS failed: %v", hopsPath, err)
		}
	})

	const size = 8 << 20
	f, err := os.Create(filepath.Join(p.dir, name))
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}
	sum := writeRandom(t, f, size)

	termAt := time.Now()
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("SIGTERM failed: %v", err)
	}
	time.Sleep(500 * time.Millisecond)
	select {
	case <-p.done:
		t.Fatalf("exited with a dirty file still open\n%s", p.output.String())
	default:
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close after SIGTERM failed: %v\n%s", err, p.output.String())
	}
	code, _ := p.waitExit(t, termAt, exitTestTimeout+time.Second)
	if code != 0 {
		t.Fatalf("exit code %d, want 0\n%s", code, p.output.String())
	}
	assertInHopsFS(t, accessor, hopsPath, size, sum)
}

// A dirty file still open when the bound runs out: exit 1, and the ERROR line
// names the file.
func TestExitOnSignalUnflushedAtBound(t *testing.T) {
	bin := buildMountBinary(t)
	accessor := newTestAccessor(t)
	base := newTestBase(t, accessor)
	p := startMountProcess(t, bin, base)

	name := uniqueTestName("exit_unflushed")
	hopsPath := base + "/" + name
	t.Cleanup(func() {
		// Create opened the file in HopsFS; whether an entry is left after the
		// exit is not part of this test, so only a missing file is accepted.
		if err := accessor.Remove(hopsPath); err != nil && !errors.Is(err, syscall.ENOENT) && !os.IsNotExist(err) {
			t.Errorf("remove of %s in HopsFS failed: %v", hopsPath, err)
		}
	})

	f, err := os.Create(filepath.Join(p.dir, name))
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}
	writeRandom(t, f, 1<<20)

	termAt := time.Now()
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("SIGTERM failed: %v", err)
	}
	code, elapsed := p.waitExit(t, termAt, exitTestTimeout+2*time.Second)
	f.Close()
	if code != 1 {
		t.Fatalf("exit code %d, want 1\n%s", code, p.output.String())
	}
	if elapsed < exitTestTimeout {
		t.Fatalf("exit %s after SIGTERM, before the %s bound\n%s", elapsed, exitTestTimeout, p.output.String())
	}
	if want := "unflushed " + hopsPath; !strings.Contains(p.output.String(), want) {
		t.Fatalf("the exit line does not name %q:\n%s", want, p.output.String())
	}
}
