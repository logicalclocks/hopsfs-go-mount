package hopsfsmount

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func TestValidateMaxReadaheadKb(t *testing.T) {
	for _, kb := range []int{1, 64, 1024, maxReadaheadKbLimit} {
		if err := validateMaxReadaheadKb(kb); err != nil {
			t.Errorf("maxReadaheadKb %d should be accepted: %v", kb, err)
		}
	}
	for _, kb := range []int{-1, 0, maxReadaheadKbLimit + 1} {
		if err := validateMaxReadaheadKb(kb); err == nil {
			t.Errorf("maxReadaheadKb %d should be rejected", kb)
		}
	}
}

func TestMaxReadaheadKbReachesKernel(t *testing.T) {
	// Below the kernel's 128 KiB default: FUSE_INIT can only lower the
	// mount's readahead, so a larger value would not show in read_ahead_kb.
	const readaheadKb = 32

	previous := MaxReadaheadKb
	MaxReadaheadKb = readaheadKb
	defer func() { MaxReadaheadKb = previous }()

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
		if got := strings.TrimSpace(string(data)); got != fmt.Sprint(readaheadKb) {
			t.Errorf("%s is %s, want %d", bdi, got, readaheadKb)
		}
	})
}
