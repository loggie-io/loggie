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
	"io"
	"os"
	"testing"
	"time"
)

func TestLineCountTo(t *testing.T) {
	fileName := "/tmp/pressure-access-88.log"
	offset := int64(19)
	now := time.Now()
	count, err := LineCountTo(offset, fileName)
	if err != nil {
		panic(err)
	}
	fmt.Println(count)
	fmt.Printf("cost: %dms\n", time.Since(now)/time.Millisecond)
}

func TestLineCount(t *testing.T) {
	f, err := os.Open("/tmp/pressure-access-88.log")
	if err != nil {
		panic(err)
	}
	now := time.Now()
	count, err := LineCount(f)
	if err != nil {
		panic(err)
	}
	fmt.Println(count)
	fmt.Printf("cost: %dms\n", time.Since(now)/time.Millisecond)
}

func TestLineCount1(t *testing.T) {
	f, err := os.Open("/tmp/pressure-access-88.log")
	if err != nil {
		panic(err)
	}
	now := time.Now()
	count, err := LineCount1(f)
	if err != nil {
		panic(err)
	}
	fmt.Println(count)
	fmt.Printf("cost: %dms\n", time.Since(now)/time.Millisecond)
}

func BenchmarkLineCount(b *testing.B) {
	f, err := os.Open("/tmp/pressure-access-88.log")
	if err != nil {
		panic(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Seek(0, io.SeekStart)
		_, err = LineCount(f)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkLineCount1(b *testing.B) {
	f, err := os.Open("/tmp/pressure-access-88.log")
	if err != nil {
		panic(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Seek(0, io.SeekStart)
		_, err = LineCount1(f)
		if err != nil {
			panic(err)
		}
	}
}
