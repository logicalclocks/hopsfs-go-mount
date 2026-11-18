// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package hopsfsmount

import "testing"

func TestValidateMaxReadaheadKb(t *testing.T) {
	for _, kb := range []int{0, 64, 1024, maxReadaheadKbLimit} {
		if err := validateMaxReadaheadKb(kb); err != nil {
			t.Errorf("%d KiB: %v", kb, err)
		}
	}
	for _, kb := range []int{-1, maxReadaheadKbLimit + 1, 1 << 30} {
		if err := validateMaxReadaheadKb(kb); err == nil {
			t.Errorf("%d KiB: expected an error", kb)
		}
	}
}
