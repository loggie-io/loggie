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
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/util"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_charset(t *testing.T) {
	log.InitDefaultLogger()
	gbk := util.AllEncodings["gbk"]
	encode := gbk.NewEncoder()
	decoder := NewCharset("gbk", nil)
	context := []byte("Loggie是一个日志采集Agent和中转处理的Aggregator，包含多个Pipeline管道，每个Pipeline又由Source输入、I" +
		"nterceptor拦截处理和Sink输出组成。" + "\n\n基于这种插件式设计，Loggie并不局限在日志采集，通过配置不同的" +
		"Source/Interceptor/Sink，Loggie可以组合实现各种不同的功能。\n\n简单起见，这里我们从采集日志开始。\n\n")
	data, err := encode.Bytes(context)
	assert.Equal(t, err, nil)
	_, err = decoder.decoder.Bytes(data)
	assert.Equal(t, err, nil)
}
