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
	"loggie.io/loggie/pkg/core/api"
	"loggie.io/loggie/pkg/core/context"
	"loggie.io/loggie/pkg/pipeline"
	"testing"
)

func TestSource_Product(t *testing.T) {
	source := makeSource(pipeline.Info{
		Stop: false,
	}).(api.Source)
	properties := map[string]interface{}{
		"filename": "/tmp/loggie/access.log",
	}
	source.Init(context.NewContext("memory-queue-1", "memory-queue", api.QUEUE, properties))
	source.Start()

	for i := 0; i < 100; i++ {
		event := source.Product()
		fmt.Printf("event: %v \n", event)
	}
}

func TestSlice1(t *testing.T) {
	s := make([]string, 0)
	s = append(s, "a")
	s1 := s[0]
	s = s[1:]
	fmt.Printf("s1:%s\n", s1)
	fmt.Printf("s: %v\n", s)
}
