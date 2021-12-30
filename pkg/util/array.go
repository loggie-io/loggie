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

func Contain(code string, array []string) bool {
	if len(array) > 0 {
		for _, s := range array {
			if s == code {
				return true
			}
		}
	}
	return false
}

func ContainWithFunc(code string, array []CodeFunc) bool {
	if len(array) > 0 {
		for _, keyFunc := range array {
			if keyFunc.Code() == code {
				return true
			}
		}
	}
	return false
}

type CodeFunc interface {
	Code() string
}
