package util

// refers to https://github.com/golang/go/issues/11862
// https://github.com/mattn/go-zglob/blob/254b632af962d02711182299973af169f291925c/zglob.go
// https://github.com/elastic/beats/blob/35a7882f69d6daf9ab72f36c6509a00aaa2904d0/filebeat/input/file/glob.go#L42

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
)

var (
	envre = regexp.MustCompile(`^(\$[a-zA-Z][a-zA-Z0-9_]+|\$\([a-zA-Z][a-zA-Z0-9_]+\))$`)
)

// TraverseLink is a sentinel error for fastWalk, similar to filepath.SkipDir.
var TraverseLink = errors.New("traverse symlink, assuming target is a directory")

type zenv struct {
	dirmask string
	fre     *regexp.Regexp
	pattern string
	root    string
}

func toSlash(path string) string {
	if filepath.Separator == '/' {
		return path
	}
	var buf bytes.Buffer
	cc := []rune(path)
	for i := 0; i < len(cc); i++ {
		if i < len(cc)-2 && cc[i] == '\\' && (cc[i+1] == '{' || cc[i+1] == '}') {
			buf.WriteRune(cc[i])
			buf.WriteRune(cc[i+1])
			i++
		} else if cc[i] == '\\' {
			buf.WriteRune('/')
		} else {
			buf.WriteRune(cc[i])
		}
	}
	return buf.String()
}

func New(pattern string) (*zenv, error) {
	globmask := ""
	root := ""
	for n, i := range strings.Split(toSlash(pattern), "/") {
		if root == "" && (strings.Index(i, "*") != -1 || strings.Index(i, "{") != -1) {
			if globmask == "" {
				root = "."
			} else {
				root = toSlash(globmask)
			}
		}
		if n == 0 && i == "~" {
			if runtime.GOOS == "windows" {
				i = os.Getenv("USERPROFILE")
			} else {
				i = os.Getenv("HOME")
			}
		}
		if envre.MatchString(i) {
			i = strings.Trim(strings.Trim(os.Getenv(i[1:]), "()"), `"`)
		}

		globmask = path.Join(globmask, i)
		if n == 0 {
			if runtime.GOOS == "windows" && filepath.VolumeName(i) != "" {
				globmask = i + "/"
			} else if len(globmask) == 0 {
				globmask = "/"
			}
		}
	}
	if root == "" {
		return &zenv{
			dirmask: "",
			fre:     nil,
			pattern: pattern,
			root:    "",
		}, nil
	}
	if globmask == "" {
		globmask = "."
	}
	globmask = toSlash(path.Clean(globmask))

	cc := []rune(globmask)
	dirmask := ""
	filemask := ""
	staticDir := true
	for i := 0; i < len(cc); i++ {
		if i < len(cc)-2 && cc[i] == '\\' {
			i++
			filemask += fmt.Sprintf("[\\x%02X]", cc[i])
			if staticDir {
				dirmask += string(cc[i])
			}
		} else if cc[i] == '*' {
			staticDir = false
			if i < len(cc)-2 && cc[i+1] == '*' && cc[i+2] == '/' {
				filemask += "(.*/)?"
				i += 2
			} else {
				filemask += "[^/]*"
			}
		} else {
			if cc[i] == '{' {
				staticDir = false
				pattern := ""
				for j := i + 1; j < len(cc); j++ {
					if cc[j] == ',' {
						pattern += "|"
					} else if cc[j] == '}' {
						i = j
						break
					} else {
						c := cc[j]
						if c == '/' {
							pattern += string(c)
						} else if ('0' <= c && c <= '9') || ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || 255 < c {
							pattern += string(c)
						} else {
							pattern += fmt.Sprintf("[\\x%02X]", c)
						}
					}
				}
				if pattern != "" {
					filemask += "(" + pattern + ")"
					continue
				}
			} else if i < len(cc)-1 && cc[i] == '!' && cc[i+1] == '(' {
				i++
				pattern := ""
				for j := i + 1; j < len(cc); j++ {
					if cc[j] == ')' {
						i = j
						break
					} else {
						c := cc[j]
						pattern += fmt.Sprintf("[^\\x%02X/]*", c)
					}
				}
				if pattern != "" {
					if dirmask == "" {
						dirmask = filemask
						root = filemask
					}
					filemask += pattern
					continue
				}
			}
			c := cc[i]
			if c == '/' || ('0' <= c && c <= '9') || ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || 255 < c {
				filemask += string(c)
			} else {
				filemask += fmt.Sprintf("[\\x%02X]", c)
			}
			if staticDir {
				dirmask += string(c)
			}
		}
	}
	if len(filemask) > 0 && filemask[len(filemask)-1] == '/' {
		if root == "" {
			root = filemask
		}
		filemask += "[^/]*"
	}
	if runtime.GOOS == "windows" || runtime.GOOS == "darwin" {
		filemask = "(?i:" + filemask + ")"
	}
	return &zenv{
		dirmask: path.Dir(dirmask) + "/",
		fre:     regexp.MustCompile("^" + filemask + "$"),
		pattern: pattern,
		root:    filepath.Clean(root),
	}, nil
}

func Glob(pattern string, followSymlinks bool, dirCb func(fp string) error, fileCb func(fp string) error) ([]string, error) {
	zenv, err := New(pattern)
	if err != nil {
		return nil, err
	}
	if zenv.root == "" {
		_, err := os.Stat(pattern)
		if err != nil {
			return nil, os.ErrNotExist
		}
		return nil, nil
	}
	relative := !filepath.IsAbs(pattern)
	var matches []string

	err = filepath.WalkDir(zenv.root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return filepath.SkipDir
		}
		if zenv.root == "." && len(zenv.root) < len(path) {
			path = path[len(zenv.root)+1:]
		}
		path = filepath.ToSlash(path)
		info := d.Type()

		if followSymlinks && 0 != (info&fs.ModeSymlink) {
			followedPath, err := filepath.EvalSymlinks(path)
			if err != nil {
				fi, err := os.Lstat(followedPath)
				if err == nil && fi.IsDir() {
					return TraverseLink
				}
			}
		}

		if info.IsDir() {
			if path == "." || len(path) <= len(zenv.root) {
				return nil
			}
			if len(path) < len(zenv.dirmask) && !strings.HasPrefix(zenv.dirmask, path+"/") {
				return filepath.SkipDir
			}
			if dirCb != nil {
				err = dirCb(path)
			}
			return err
		}

		if zenv.fre.MatchString(path) {
			if relative && filepath.IsAbs(path) {
				path = path[len(zenv.root)+1:]
			}
			if fileCb != nil {
				err = fileCb(path)
			} else {
				matches = append(matches, path)
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return matches, nil
}
