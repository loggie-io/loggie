/*
Copyright 2022 Loggie Authors

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
	"github.com/loggie-io/loggie/pkg/util"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_charset(t *testing.T) {
	gbk := util.AllEncodings["gbk"]
	encode := gbk.NewEncoder()
	decoder := NewCharset("gbk", nil)
	data, err := encode.Bytes([]byte("djwqiodjqiwojdioqwjdioqw"))
	assert.Equal(t, err, nil)
	data, err = decoder.decoder.Bytes(data)
	assert.Equal(t, err, nil)
	fmt.Println(string(data))
}
