package hopsfsmount

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func TestValidateMaxReadaheadBytes(t *testing.T) {
	for _, bytes := range []int{1, 65536, 1048576, maxReadaheadBytesLimit} {
		if err := validateMaxReadaheadBytes(bytes); err != nil {
			t.Errorf("maxReadaheadBytes %d should be accepted: %v", bytes, err)
		}
	}
	for _, bytes := range []int{-1, 0, maxReadaheadBytesLimit + 1} {
		if err := validateMaxReadaheadBytes(bytes); err == nil {
			t.Errorf("maxReadaheadBytes %d should be rejected", bytes)
		}
	}
}

func TestMaxReadaheadBytesReachesKernel(t *testing.T) {
	// Below the kernel's 128 KiB default: FUSE_INIT can only lower the
	// mount's readahead, so a larger value would not show in read_ahead_kb.
	const readaheadBytes = 32 * 1024

	previous := MaxReadaheadBytes
	MaxReadaheadBytes = readaheadBytes
	defer func() { MaxReadaheadBytes = previous }()

	withMount(t, "/", DelaySyncUntilClose, func(mountPoint string, hdfsAccessor HdfsAccessor) {
		var stat unix.Stat_t
		if err := unix.Stat(mountPoint, &stat); err != nil {
			t.Fatalf("stat %s: %v", mountPoint, err)
		}
		dev := uint64(stat.Dev)
		bdi := fmt.Sprintf("/sys/class/bdi/%d:%d/read_ahead_kb", unix.Major(dev), unix.Minor(dev))
		data, err := os.ReadFile(bdi)
		if err != nil {
			t.Fatalf("read %s: %v", bdi, err)
		}
		if got := strings.TrimSpace(string(data)); got != fmt.Sprint(readaheadBytes/1024) {
			t.Errorf("%s is %s, want %d", bdi, got, readaheadBytes/1024)
		}
	})
}
