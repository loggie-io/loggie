//go:build linux || darwin
// +build linux darwin

package file

import (
	"os"
	"strconv"
	"syscall"
)

func JobUid(file string, fileInfo os.FileInfo) string {
	stat := fileInfo.Sys().(*syscall.Stat_t)
	inode := stat.Ino
	device := stat.Dev
	var buf [64]byte
	current := strconv.AppendUint(buf[:0], inode, 10)
	current = append(current, '-')
	current = strconv.AppendUint(current, uint64(device), 10)
	return string(current)
}
