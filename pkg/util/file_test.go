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

package util

import (
	"fmt"
	xglob "github.com/bmatcuk/doublestar/v4"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestLineCountTo(t *testing.T) {
	fileName := "/tmp/pressure-access-88.log"
	offset := int64(19)
	now := time.Now()
	count, err := LineCountTo(offset, fileName)
	if err != nil {
		fmt.Println("count fail: " + err.Error())
		return
	}
	fmt.Println(count)
	fmt.Printf("cost: %dms\n", time.Since(now)/time.Millisecond)
}

func TestLineCount(t *testing.T) {
	f, err := os.Open("/tmp/pressure-access-88.log")
	if err != nil {
		fmt.Println("count fail: " + err.Error())
		return
	}
	now := time.Now()
	count, err := LineCount(f)
	if err != nil {
		fmt.Println("count fail: " + err.Error())
		return
	}
	fmt.Println(count)
	fmt.Printf("cost: %dms\n", time.Since(now)/time.Millisecond)
}

func TestLineCount1(t *testing.T) {
	f, err := os.Open("/tmp/pressure-access-88.log")
	if err != nil {
		fmt.Println("count fail: " + err.Error())
		return
	}
	now := time.Now()
	count, err := LineCount1(f)
	if err != nil {
		fmt.Println("count fail: " + err.Error())
		return
	}
	fmt.Println(count)
	fmt.Printf("cost: %dms\n", time.Since(now)/time.Millisecond)
}

func BenchmarkLineCount(b *testing.B) {
	f, err := os.Open("/tmp/pressure-access-88.log")
	if err != nil {
		fmt.Println("count fail: " + err.Error())
		return
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Seek(0, io.SeekStart)
		_, err = LineCount(f)
		if err != nil {
			fmt.Println("count fail: " + err.Error())
			return
		}
	}
}

func BenchmarkLineCount1(b *testing.B) {
	f, err := os.Open("/tmp/pressure-access-88.log")
	if err != nil {
		fmt.Println("count fail: " + err.Error())
		return
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Seek(0, io.SeekStart)
		_, err = LineCount1(f)
		if err != nil {
			fmt.Println("count fail: " + err.Error())
			return
		}
	}
}

const (
	path                        = "/tmp/loggie/"
	globPattern                 = path + "access.log.[2-9][0-9][0-9][0-9]-[01][0-9]-[0123][0-9].[012][0-9]"
	globPatternWithStars        = path + "**/access.log.[2-9][0-9][0-9][0-9]-[01][0-9]-[0123][0-9].[012][0-9]"
	globPatternWithStarsOption  = path + "**/access.log{,.[2-9][0-9][0-9][0-9]-[01][0-9]-[0123][0-9].[012][0-9]}"
	regexPattern                = path + "access\\.log\\.[2-9][0-9][0-9][0-9]-[01][0-9]-[0123][0-9]\\.[012][0-9]"
	regexPatternWithStars       = path + ".*/access\\.log\\.[2-9][0-9][0-9][0-9]-[01][0-9]-[0123][0-9]\\.[012][0-9]"
	regexPatternWithStarsOption = path + ".*/access\\.log(?:\\.\\d{4}-\\d{2}-\\d{2}\\.\\d{2})?"

	file        = path + "access.log.2022-04-11.08"
	file1       = path + "access.log.2022-04-11.8"
	startFile   = path + "service/order/logs/access.log.2022-04-11.08"
	fileNonDate = path + "service/order/logs/access.log"
)

func TestRegexMatch(t *testing.T) {
	r := regexp.MustCompile(regexPattern)
	m := r.MatchString(file)
	fmt.Println(m)
}

func TestRegexMatchStar(t *testing.T) {
	r := regexp.MustCompile(regexPatternWithStars)
	m := r.MatchString(startFile)
	fmt.Println(m)
}

func TestRegexMatchStarOption(t *testing.T) {
	r := regexp.MustCompile(regexPatternWithStarsOption)
	m := r.MatchString(startFile)
	fmt.Println(m)
	m = r.MatchString(fileNonDate)
	fmt.Println(m)
}

func TestFilePathGlobMatch(t *testing.T) {
	m, _ := filepath.Match(globPattern, file)
	fmt.Println(m)
	m, _ = MatchWithRecursive(globPattern, file)
	fmt.Println(m)
}

func TestMatchWithRecursive(t *testing.T) {
	type args struct {
		pattern string
		name    string
	}
	tests := []struct {
		name        string
		args        args
		wantMatched bool
		wantErr     bool
	}{
		{
			name: "test match",
			args: args{
				pattern: globPattern,
				name:    file,
			},
			wantMatched: true,
			wantErr:     false,
		},
		{
			name: "test double star recursive match",
			args: args{
				pattern: globPatternWithStars,
				name:    startFile,
			},
			wantMatched: true,
			wantErr:     false,
		},
		{
			name: "test double star recursive option match1",
			args: args{
				pattern: globPatternWithStarsOption,
				name:    fileNonDate,
			},
			wantMatched: true,
			wantErr:     false,
		},
		{
			name: "test double star recursive option match2",
			args: args{
				pattern: globPatternWithStarsOption,
				name:    startFile,
			},
			wantMatched: true,
			wantErr:     false,
		},
		{
			name: "test double star recursive option match3",
			args: args{
				pattern: globPatternWithStarsOption,
				name:    file1,
			},
			wantMatched: false,
			wantErr:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotMatched, err := MatchWithRecursive(tt.args.pattern, tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("Mock() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotMatched != tt.wantMatched {
				t.Errorf("Mock() gotMatched = %v, want %v", gotMatched, tt.wantMatched)
			}
		})
	}
}

func TestGlobStarMatch1(t *testing.T) {
	pattern := `/tmp/loggie/**/access.log{,.[0-9]}`
	dir, pattern := xglob.SplitPattern(pattern)
	fmt.Println(dir)
	basePath := os.DirFS(dir)
	matches := make([]string, 0)
	err := xglob.GlobWalk(basePath, pattern, func(path string, d fs.DirEntry) error {
		fmt.Println("walk: " + d.Name())
		fmt.Println("walk path: " + path)
		matchFile := filepath.Join(dir, path)
		stat, e := os.Stat(matchFile)
		if e != nil {
			fmt.Println("stat file fail: " + e.Error())
			return nil
		}
		fmt.Println("stat file: " + stat.Name())
		matches = append(matches, matchFile)
		return nil
	})
	if err != nil {
		fmt.Println("glob fail:" + err.Error())
		return
	}
	fmt.Println("xglob: " + strings.Join(matches, "\n"))
}

func BenchmarkRegexMatch(b *testing.B) {
	b.ReportAllocs()
	r := regexp.MustCompile(regexPattern)
	for i := 0; i < b.N; i++ {
		_ = r.MatchString(file)
	}
}

func BenchmarkRegexWithStarMatch(b *testing.B) {
	b.ReportAllocs()
	r := regexp.MustCompile(regexPatternWithStars)
	for i := 0; i < b.N; i++ {
		_ = r.MatchString(startFile)
	}
}

func BenchmarkRegexWithStarOptionMatch(b *testing.B) {
	b.ReportAllocs()
	r := regexp.MustCompile(regexPatternWithStarsOption)
	for i := 0; i < b.N; i++ {
		_ = r.MatchString(startFile)
	}
}

func BenchmarkGlobMatch(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = MatchWithRecursive(globPattern, file)
	}
}

func BenchmarkGlobWithStarMatch(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = MatchWithRecursive(globPatternWithStars, startFile)
	}
}

func BenchmarkGlobWithStarOptionMatch(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = MatchWithRecursive(globPatternWithStarsOption, startFile)
	}
}
