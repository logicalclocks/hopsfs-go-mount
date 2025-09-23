// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package hopsfsmount

import (
	"os/exec"
	"syscall"
)

// UnmountPath unmounts a filesystem at the given path
func UnmountPath(path string) error {
	cmd := exec.Command("fusermount", "-u", path)
	err := cmd.Run()
	if err != nil {
		// If fusermount fails, try regular umount
		cmd = exec.Command("umount", path)
		err = cmd.Run()
	}
	return err
}

// BindMount performs a bind mount from source to target
func BindMount(source, target string) error {
	return syscall.Mount(source, target, "", syscall.MS_BIND, "")
}