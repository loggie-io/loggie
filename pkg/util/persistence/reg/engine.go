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

package reg

type DbEngine interface {
	FindAll() ([]Registry, error)
	FindBy(jobUid string, sourceName string, pipelineName string) (Registry, error)
	Insert(registries []Registry) error
	Update(registries []Registry) error
	UpdateFileName(rs []Registry) error
	Delete(r Registry) error
	DeleteBy(jobUid string, sourceName string, pipelineName string) error
	Close() error
}
