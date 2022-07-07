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
	"io"
	"os"
	"testing"
)

func TestReadNextOffset(t *testing.T) {
	fileName := "/tmp/loggie/access.log"
	file, _ := os.Open(fileName)
	lastOffset, _ := file.Seek(0, io.SeekCurrent)
	fmt.Printf("current offset: %d \n", lastOffset)
	readBuffer := make([]byte, 1024)
	l, _ := file.Read(readBuffer)
	fmt.Printf("read size: %d \n", l)
	lastOffset, _ = file.Seek(0, io.SeekCurrent)
	fmt.Printf("current offset: %d \n", lastOffset)
	lastOffset, _ = file.Seek(1, io.SeekCurrent)
	fmt.Printf("current offset: %d \n", lastOffset)
	l, _ = file.Read(readBuffer)
	fmt.Printf("read size: %d \n", l)
}
