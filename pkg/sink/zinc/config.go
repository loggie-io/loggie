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

package zinc

type Config struct {
	Index     string `yaml:"index" default:"default"`
	Host      string `yaml:"host" default:"http://127.0.0.1:4080"`
	Username  string `yaml:"username" default:"admin"`
	Password  string `yaml:"password" default:""`
	UserAgent string `yaml:"userAgent" default:"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36"`
}
