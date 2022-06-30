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

package cfg

import (
	"fmt"
	"github.com/creasty/defaults"
	"github.com/go-playground/validator/v10"
	"github.com/goccy/go-yaml"
	"github.com/loggie-io/loggie/pkg/core/log"
	"io/ioutil"
	"os"
)

type CommonCfg map[string]interface{}

type ComponentBaseConfig struct {
	Enabled    *bool     `yaml:"enabled,omitempty"`
	Name       string    `yaml:"name,omitempty"`
	Type       string    `yaml:"type,omitempty" validate:"required"`
	Properties CommonCfg `yaml:",inline"`
}

// TODO
func (c CommonCfg) GetProperties() CommonCfg {
	return c
}

type Validator interface {
	Validate() error
}

func NewCommonCfg() CommonCfg {
	return make(map[string]interface{})
}

func (c CommonCfg) Put(key string, val interface{}) {
	c[key] = val
}

func (c CommonCfg) Remove(key string) {
	delete(c, key)
}

func (c CommonCfg) Get(key string) interface{} {
	return c[key]
}

func (c CommonCfg) UID() string {
	tp := c.Get("type")
	name := c.Get("name")
	return fmt.Sprintf("%s/%s", tp, name)
}

func (c CommonCfg) GetType() string {
	typeName, ok := c["type"]
	if !ok {
		return ""
	}
	return typeName.(string)
}

func (c CommonCfg) GetName() string {
	name, ok := c["name"]
	if !ok {
		return ""
	}
	return name.(string)
}

func MergeCommonCfg(base CommonCfg, from CommonCfg, override bool) CommonCfg {
	if base == nil {
		return from
	}
	if from == nil {
		return base
	}

	for k, v := range from {
		baseVal, ok := base[k]

		b, okb := baseVal.(map[string]interface{})
		f, okf := v.(map[string]interface{})
		if okb && okf {
			MergeCommonMap(b, f, override)
			continue
		}

		if ok && !override {
			continue
		}

		base[k] = v
	}
	return base
}

func MergeCommonMap(base map[string]interface{}, from map[string]interface{}, override bool) map[string]interface{} {
	if base == nil {
		return from
	}
	if from == nil {
		return base
	}

	for k, v := range from {
		baseVal, ok := base[k]

		b, okb := baseVal.(map[string]interface{})
		f, okf := v.(map[string]interface{})
		if okb && okf {
			MergeCommonMap(b, f, override)
			continue
		}

		if ok && !override {
			continue
		}

		base[k] = v
	}
	return base

}

// MergeCommonCfgListByType merge commonCfg list
// ignoreFromType: set ignoreFromType=true, if fromCommonCfg had a type A which does not exist in baseCommonCfg, then type A would not merged to baseCommonCfg
func MergeCommonCfgListByType(base []CommonCfg, from []CommonCfg, override bool, ignoreFromType bool) []CommonCfg {
	if len(base) == 0 {
		return from
	}
	if len(from) == 0 {
		return base
	}

	fromMap := make(map[string]CommonCfg)
	for _, v := range from {
		fromMap[v.GetType()] = v
	}

	for _, baseCfg := range base {
		typeName := baseCfg.GetType()
		fromCfg, ok := fromMap[typeName]
		if ok {
			MergeCommonCfg(baseCfg, fromCfg, override)
			if !ignoreFromType {
				delete(fromMap, typeName)
			}
			continue
		}
	}

	if !ignoreFromType {
		for _, v := range fromMap {
			base = append(base, v)
		}
	}
	return base
}

func MergeCommonCfgListByTypeAndName(base []CommonCfg, from []CommonCfg, override bool, ignoreFromType bool) []CommonCfg {
	if len(base) == 0 {
		return from
	}
	if len(from) == 0 {
		return base
	}

	fromMap := make(map[string]CommonCfg)
	for _, v := range from {
		fromMap[v.UID()] = v
	}

	for _, baseCfg := range base {
		baseUID := baseCfg.UID()
		fromCfg, ok := fromMap[baseUID]
		if ok {
			MergeCommonCfg(baseCfg, fromCfg, override)
			delete(fromMap, baseUID)
			continue
		}
	}

	if !ignoreFromType {
		for _, v := range fromMap {
			base = append(base, v)
		}
	}
	return base
}

func UnpackFromFileDefaultsAndValidate(path string, config interface{}) error {
	content, err := ioutil.ReadFile(path)
	if err != nil {
		log.Warn("read config error. err: %v", err)
		return err
	}

	return UnpackRawDefaultsAndValidate(content, config)
}

func UnpackFromEnvDefaultsAndValidate(key string, config interface{}) error {
	return UnpackRawDefaultsAndValidate([]byte(os.Getenv(key)), config)
}

func UnpackTypeDefaultsAndValidate(configType string, key string, config interface{}) {
	var err error
	switch configType {
	case "env":
		err = UnpackFromEnvDefaultsAndValidate(key, config)
	default:
		err = UnpackFromFileDefaultsAndValidate(key, config)
	}
	if err != nil {
		log.Fatal("unpack global config file error: %+v", err)
	}
}

func UnpackFromFileDefaults(path string, config interface{}) error {
	content, err := ioutil.ReadFile(path)
	if err != nil {
		log.Warn("read config error. err: %v", err)
		return err
	}

	return UnpackRawAndDefaults(content, config)
}

func UnpackFromFile(path string, config interface{}) error {
	content, err := ioutil.ReadFile(path)
	if err != nil {
		log.Warn("read config error. err: %v", err)
		return err
	}

	return UnpackRaw(content, config)
}

func UnpackRawDefaultsAndValidate(content []byte, config interface{}) error {
	if config == nil {
		return nil
	}
	err := yaml.Unmarshal(content, config)
	if err != nil {
		return err
	}

	if err := setDefault(config); err != nil {
		return err
	}

	if err := validate(config); err != nil {
		return err
	}

	return nil
}

func UnpackRaw(content []byte, config interface{}) error {
	if config == nil {
		return nil
	}

	err := yaml.Unmarshal(content, config)
	if err != nil {
		return err
	}

	return nil
}

func UnpackRawAndDefaults(content []byte, config interface{}) error {
	if config == nil {
		return nil
	}

	err := yaml.Unmarshal(content, config)
	if err != nil {
		return err
	}

	if err := setDefault(config); err != nil {
		return err
	}
	return nil
}

func UnpackDefaultsAndValidate(properties CommonCfg, config interface{}) error {
	if properties == nil {
		return nil
	}

	out, err := yaml.Marshal(properties)
	if err != nil {
		return err
	}

	return UnpackRawDefaultsAndValidate(out, config)
}

func UnpackAndDefaults(properties CommonCfg, config interface{}) error {
	if properties == nil {
		return nil
	}

	out, err := yaml.Marshal(properties)
	if err != nil {
		return err
	}

	return UnpackRawAndDefaults(out, config)
}

func Unpack(properties CommonCfg, config interface{}) error {
	if properties == nil {
		return nil
	}

	out, err := yaml.Marshal(properties)
	if err != nil {
		return err
	}
	return UnpackRaw(out, config)
}

func Pack(config interface{}) (CommonCfg, error) {

	if config == nil {
		return nil, nil
	}

	out, err := yaml.Marshal(config)
	if err != nil {
		return nil, err
	}

	ret := make(map[string]interface{})
	err = yaml.Unmarshal(out, &ret)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func PackAndDefault(config interface{}) (CommonCfg, error) {
	err := setDefault(config)
	if err != nil {
		return nil, err
	}
	c, err := Pack(config)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func setDefault(config interface{}) error {
	if defaults.CanUpdate(config) {
		return defaults.Set(config)
	}
	return defaults.Set(config)
}

func validate(config interface{}) error {
	if config == nil {
		return nil
	}

	validate := validator.New()
	err := validate.Struct(config)
	if err != nil {
		return err
	}

	if cfg, ok := config.(Validator); ok {
		return cfg.Validate()
	}
	return nil
}
