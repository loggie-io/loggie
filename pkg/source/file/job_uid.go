//go:build windows
// +build windows

package file

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

func JobUid(file string, fileInfo os.FileInfo) string {
	cmd := exec.Command("fsutil", []string{"file", "queryFileID", file}...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = os.Stderr
	err := cmd.Start()
	if err != nil {
		return ""
	}
	err = cmd.Wait()
	if err != nil {
		return ""
	}
	fid := out.String()
	fid = strings.TrimSpace(fid)
	ss := strings.Split(fid, "0x")
	fid = ss[len(ss)-1]

	device := fmt.Sprintf("%02d", strings.ToLower(string(file[0]))[0]-'a')
	return device + "-" + fid
}
