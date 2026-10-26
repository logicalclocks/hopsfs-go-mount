// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package hopsfsmount

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"path"
	"strconv"
	"strings"
	"syscall"
	"time"

	"bazil.org/fuse/fs"
	"hopsworks.ai/hopsfsmount/internal/hopsfsmount/logger"
)

// The control socket takes one command per connection, one line, and answers lines ending in
// "exit <code>":
//
//	drain <timeout seconds> [path]  waits for the uploads of path (a file, or a directory and
//	                                everything below it; default all). Exit 0 all landed,
//	                                1 unresolved errors, 2 timeout.
//	stop <timeout seconds>          fences writes, queues open files, drains everything (as SIGTERM).
//	discard <path>                  drops path's staged copy and keeps HopsFS's version.
//
// Each unresolved path is a line "unresolved <path>\t<error>". Paths are HopsFS paths.
func ServeControlSocket(filesystem *FileSystem, socketPath string) (net.Listener, error) {
	os.Remove(socketPath)
	l, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(socketPath, 0600); err != nil {
		l.Close()
		return nil, err
	}
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go handleControl(filesystem, c)
		}
	}()
	logger.Info("Control socket listening", logger.Fields{Path: socketPath})
	return l, nil
}

func handleControl(filesystem *FileSystem, c net.Conn) {
	defer c.Close()
	line, err := bufio.NewReader(c).ReadString('\n')
	if err != nil {
		return
	}
	fields := strings.Fields(line)
	reply := func(code int, unresolved []Unresolved, msg string) {
		for _, u := range unresolved {
			fmt.Fprintf(c, "unresolved %s\t%s\n", u.Path, u.Error)
		}
		if msg != "" {
			fmt.Fprintln(c, msg)
		}
		fmt.Fprintf(c, "exit %d\n", code)
	}
	wb := filesystem.WB
	if wb == nil || len(fields) == 0 {
		reply(64, nil, "write-behind is off, or no command")
		return
	}
	switch fields[0] {
	case "drain", "stop":
		if len(fields) < 2 {
			reply(64, nil, "usage: drain <timeout seconds> [path] | stop <timeout seconds>")
			return
		}
		secs, err := strconv.ParseFloat(fields[1], 64)
		if err != nil || secs < 0 {
			reply(64, nil, "bad timeout")
			return
		}
		timeout := time.Duration(secs * float64(time.Second))
		var out []Unresolved
		var timedOut bool
		if fields[0] == "stop" {
			out, timedOut = wb.Stop(timeout)
		} else {
			prefix := ""
			if len(fields) > 2 {
				prefix = path.Clean(fields[2])
			}
			out, timedOut = wb.Drain(prefix, timeout)
		}
		code := 0
		if timedOut {
			code = 2
		} else if len(out) > 0 {
			code = 1
		}
		logger.Info(fmt.Sprintf("Control: %s, %d unresolved, timed out %v", fields[0], len(out), timedOut), nil)
		reply(code, out, "")
	case "discard":
		if len(fields) != 2 {
			reply(64, nil, "usage: discard <path>")
			return
		}
		file, err := filesystem.fileNode(path.Clean(fields[1]))
		if err == nil {
			err = wb.Discard(file)
		}
		if err != nil {
			reply(1, nil, err.Error())
			return
		}
		reply(0, nil, "")
	default:
		reply(64, nil, "unknown command "+fields[0])
	}
}

// fileNode walks the cached tree to the file node of a HopsFS path.
func (filesystem *FileSystem) fileNode(p string) (*FileINode, error) {
	root, err := filesystem.Root()
	if err != nil {
		return nil, err
	}
	rel := strings.TrimPrefix(p, path.Clean(filesystem.SrcDir))
	var node fs.Node = root
	for _, name := range strings.Split(strings.Trim(rel, "/"), "/") {
		dir, ok := node.(*DirINode)
		if !ok || name == "" {
			return nil, syscall.ENOENT
		}
		dir.lockMutex()
		node, err = dir.LookupInt(Lookup, name)
		dir.unlockMutex()
		if err != nil {
			return nil, err
		}
	}
	file, ok := node.(*FileINode)
	if !ok {
		return nil, syscall.EISDIR
	}
	return file, nil
}
