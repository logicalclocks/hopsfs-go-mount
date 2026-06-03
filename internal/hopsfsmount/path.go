// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package hopsfsmount

// Pather lets the Parent field of an inode hold a different concrete type
// (real *DirINode/*FileINode or synthetic *VirtualDirINode) so AbsolutePath()
// can walk up the chain without caring which kind of node it traverses.
type Pather interface {
	AbsolutePath() string
}
