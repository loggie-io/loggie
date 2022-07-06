/*
Copyright 2021 Loggie Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package file

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mattn/go-zglob"
)

func TestWatcher_scanNewFiles(t *testing.T) {
	jobs := make(map[string]string)
	jobs["a"] = "a"
	jobs["b"] = "b"
	jobs["c"] = "c"
	for k := range jobs {
		if k == "a" {
			delete(jobs, k)
		}
	}
	fmt.Println(jobs)
}

func TestMach(t *testing.T) {
	path := "/tmp/loggie/**/"
	file := "/tmp/loggie/test/loggie.log"
	// match, err := filepath.Match(path, file)
	if strings.HasSuffix(path, "**") {
		path += "/*"
	}
	if strings.HasSuffix(path, "**/") {
		path += "*"
	}
	fmt.Println(path)
	match, err := zglob.Match(path, file)
	if err != nil {
		fmt.Printf("path glob pattern(%s) match error: %v \n", path, err)
		return
	}
	fmt.Println(match)

	matches, _ := zglob.Glob(path)
	for _, s := range matches {
		fmt.Println(s)
	}
}

func TestReadSlice(t *testing.T) {
	readBufferSize := 5
	a := []byte{'a', 'b', 'c', '\n', 'd'}
	a = a[:readBufferSize]
	fmt.Println(a)
	b := a[:3]
	fmt.Printf("b: %v\n", b)
	a[0] = 'e'
	fmt.Printf("a: %v\n", a)
	fmt.Printf("b: %v\n", b)
	a = a[:0]
	fmt.Printf("a: %v\n", a)
	fmt.Printf("b: %v\n", b)
	a = append(a, 'f')
	fmt.Printf("a: %v\n", a)
	fmt.Printf("b: %v\n", b)
	a = make([]byte, readBufferSize)
	a[0] = 'g'
	fmt.Printf("a: %v\n", a)
	fmt.Printf("b: %v\n", b)
}

func TestMachDate(t *testing.T) {
	globPath := "/tmp/glob_test.log.20[0-9][0-9]-[01][0-9]-[0123][0-9].[012][0-9]"
	matches, err := filepath.Glob(globPath)
	if err != nil {
		fmt.Printf("glob fail: %s\n", err)
	}
	for _, s := range matches {
		fmt.Println(s)
	}
	fmt.Println("============")
	ms, err := zglob.Glob(globPath)
	if err != nil {
		fmt.Printf("glob fail: %s\n", err)
	}
	for _, s := range ms {
		fmt.Println(s)
	}
}
