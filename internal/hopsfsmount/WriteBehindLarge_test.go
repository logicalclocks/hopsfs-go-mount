// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package hopsfsmount

// Large-file write-behind against a real HopsFS, as WriteBehind_test.go. Named TestLargeWriteBehind
// so a run of the TestWriteBehind suite leaves them out: each writes hundreds of MB to a
// versioned bucket, where deleted bytes stay until the noncurrent-version expiry.

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const largeMB = 200

// content is size bytes the seed fixes, so the expected hash needs no second copy.
func content(seed int64, size int64) io.Reader {
	return io.LimitReader(rand.New(rand.NewSource(seed)), size)
}

func sumOf(r io.Reader) string {
	h := sha256.New()
	_, _ = io.Copy(h, r)
	return hex.EncodeToString(h.Sum(nil))
}

func (e *wbEnv) writeLarge(name string, seed int64, size int64) time.Duration {
	start := time.Now()
	f, err := os.Create(e.local(name))
	require.NoError(e.t, err)
	defer f.Close()
	_, err = io.Copy(f, content(seed, size))
	require.NoError(e.t, err)
	require.NoError(e.t, f.Close())
	return time.Since(start)
}

func (e *wbEnv) remoteSum(name string) string {
	r, err := e.ext.Open(e.remote(name))
	require.NoError(e.t, err)
	defer r.Close()
	return sumOf(r)
}

// untilUploading waits until the upload of name has its temp on HopsFS: the transfer has begun.
func (e *wbEnv) untilUploading(name string) {
	deadline := time.Now().Add(60 * time.Second)
	for {
		if _, err := e.ext.Stat(e.remote(wbTempName(name))); err == nil {
			return
		}
		require.True(e.t, time.Now().Before(deadline), "the upload of %s never began", name)
		time.Sleep(20 * time.Millisecond)
	}
}

func TestLargeWriteBehindLandsWhole(t *testing.T) {
	withWriteBehind(t, func(e *wbEnv) {
		size := int64(largeMB) << 20
		write := e.writeLarge("big.bin", 1, size)
		start := time.Now()
		assert.Empty(t, e.drain())
		upload := time.Since(start)
		assert.Equal(t, sumOf(content(1, size)), e.remoteSum("big.bin"))
		t.Logf("%d MB: close returned after %v (%.0f MB/s), the drain after %v more (%.0f MB/s end to end)",
			largeMB, write.Round(time.Millisecond), float64(largeMB)/write.Seconds(),
			upload.Round(time.Millisecond), float64(largeMB)/(write+upload).Seconds())
	})
}

// The overwrite and the unlink land while the first upload's bytes are moving.
func TestLargeWriteBehindOverwriteWhileUploading(t *testing.T) {
	withWriteBehind(t, func(e *wbEnv) {
		size := int64(largeMB) << 20
		e.writeLarge("o.bin", 1, size)
		e.untilUploading("o.bin")
		e.writeLarge("o.bin", 2, 10<<20)
		assert.Empty(t, e.drain())
		assert.Equal(t, sumOf(content(2, 10<<20)), e.remoteSum("o.bin"))
		assert.Empty(t, e.conflictFiles("o.bin"))
	})
}

func TestLargeWriteBehindExternalWriteWhileUploading(t *testing.T) {
	withWriteBehind(t, func(e *wbEnv) {
		e.writeExternal("x.bin", "v1")
		_, err := os.ReadFile(e.local("x.bin"))
		require.NoError(t, err)
		size := int64(50) << 20
		reached, release := e.hold("before-publish", "x.bin")
		defer release()
		e.writeLarge("x.bin", 3, size)
		waitFor(t, reached)
		e.writeExternal("x.bin", "theirs")
		release()
		out := e.drain()
		require.Len(t, out, 1)
		assert.Contains(t, out[0].Error, "conflict")
		assert.Equal(t, "theirs", e.readRemote("x.bin"))
		conflicts := e.conflictFiles("x.bin")
		require.Len(t, conflicts, 1)
		assert.Equal(t, sumOf(content(3, size)), e.remoteSum(conflicts[0]))
	})
}

func TestLargeWriteBehindUnlinkWhileUploading(t *testing.T) {
	withWriteBehind(t, func(e *wbEnv) {
		e.writeLarge("u.bin", 4, int64(50)<<20)
		e.untilUploading("u.bin")
		require.NoError(t, os.Remove(e.local("u.bin")))
		assert.Empty(t, e.drain())
		_, err := e.ext.Stat(e.remote("u.bin"))
		assert.ErrorIs(t, err, os.ErrNotExist)
		_, err = e.ext.Stat(e.remote(wbTempName("u.bin")))
		assert.ErrorIs(t, err, os.ErrNotExist, "no temp left behind")
	})
}
