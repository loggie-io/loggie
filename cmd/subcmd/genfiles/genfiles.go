/*
Copyright 2023 Loggie Authors

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

package genfiles

import (
	"errors"
	"flag"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/source/dev"
	"os"
)

const (
	SubCommandGenFiles = "genfiles"
)

var (
	genFilesCmd *flag.FlagSet
	totalCount  int64
	lineBytes   int
	qps         int
)

func init() {
	genFilesCmd = flag.NewFlagSet(SubCommandGenFiles, flag.ExitOnError)
	genFilesCmd.Int64Var(&totalCount, "totalCount", -1, "total line count")
	genFilesCmd.IntVar(&lineBytes, "lineBytes", 1024, "bytes per line")
	genFilesCmd.IntVar(&qps, "qps", 10, "line qps")
	log.SetFlag(genFilesCmd)
}

func RunGenFiles() error {
	if len(os.Args) > 2 {
		if err := genFilesCmd.Parse(os.Args[2:]); err != nil {
			return err
		}
	}

	log.InitDefaultLogger()

	stop := make(chan struct{})
	dev.GenLines(stop, totalCount, lineBytes, qps, func(content []byte, index int64) {
		log.Info("%d %s", index, content)
	})
	return errors.New("exit")
}
