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
	"github.com/creasty/defaults"
	"github.com/go-playground/validator/v10"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/util/yaml"
	"github.com/pkg/errors"
	"io/ioutil"
	"os"
)

type CommonCfg map[string]interface{}

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

func (c CommonCfg) DeepCopy() CommonCfg {
	if c == nil {
		return nil
	}

	out := NewCommonCfg()
	for k, v := range c {
		out[k] = v
	}
	return out
}

// MergeCommonCfg merge `base` map with `from` map
func MergeCommonCfg(base CommonCfg, from CommonCfg, override bool) CommonCfg {
	if base == nil {
		return from
	}
	if from == nil {
		return base
	}

	for k, v := range from {
		baseVal, ok := base[k]

		b, okb := baseVal.(map[interface{}]interface{})
		f, okf := v.(map[interface{}]interface{})
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

func MergeCommonMap(base map[interface{}]interface{}, from map[interface{}]interface{}, override bool) map[interface{}]interface{} {
	if base == nil {
		return from
	}
	if from == nil {
		return base
	}

	for k, v := range from {
		baseVal, ok := base[k]

		b, okb := baseVal.(map[interface{}]interface{})
		f, okf := v.(map[interface{}]interface{})
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

type UnPack struct {
	content []byte
	config  interface{}
	err     error
}

func NewUnpack(raw []byte, config interface{}, err error) *UnPack {
	return &UnPack{
		content: raw,
		config:  config,
		err:     err,
	}
}

// UnPackFromFile create an Unpack struct from file
func UnPackFromFile(path string, config interface{}) *UnPack {
	content, err := ioutil.ReadFile(path)
	if err != nil {
		err = errors.Errorf("read config error. err: %v", err)
	}
	return NewUnpack(content, config, err).unpack()
}

// UnpackFromCommonCfg create an Unpack struct from CommonCfg
func UnpackFromCommonCfg(c CommonCfg, config interface{}) *UnPack {
	if c == nil {
		return &UnPack{
			config: config,
		}
	}

	out, err := yaml.Marshal(c)
	return NewUnpack(out, config, err).unpack()
}

// UnpackFromEnv create an Unpack struct from env
func UnpackFromEnv(key string, config interface{}) *UnPack {
	return UnPackFromRaw([]byte(os.Getenv(key)), config)
}

// UnPackFromRaw create an Unpack struct from bytes array
func UnPackFromRaw(raw []byte, config interface{}) *UnPack {
	return NewUnpack(raw, config, nil).unpack()
}

func (u *UnPack) unpack() *UnPack {
	if u.err != nil {
		return u
	}
	if u.content == nil {
		return u
	}

	err := yaml.Unmarshal(u.content, u.config)
	u.err = err
	return u
}

func (u *UnPack) Defaults() *UnPack {
	if u.err != nil {
		return u
	}

	if err := setDefault(u.config); err != nil {
		u.err = err
	}
	return u
}

func (u *UnPack) Validate() *UnPack {
	if u.err != nil {
		return u
	}

	if err := validate(u.config); err != nil {
		u.err = err
	}

	return u
}

func (u *UnPack) Contents() []byte {
	return u.content
}

// Do return the error
func (u *UnPack) Do() error {
	return u.err
}

func Pack(config interface{}) (CommonCfg, error) {
	if config == nil {
		return nil, nil
	}

	out, err := yaml.Marshal(config)
	if err != nil {
		return nil, err
	}

	ret := NewCommonCfg()
	err = yaml.Unmarshal(out, &ret)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

// setDefault will affect by `default` tag, and call setDefaults() function.
func setDefault(config interface{}) error {
	if config == nil {
		return nil
	}
	return defaults.Set(config)
}

// validate will affect by `validate` tag, and call Validate() function in Config.
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

func UnpackTypeDefaultsAndValidate(configType string, key string, config interface{}) {
	var err error
	switch configType {
	case "env":
		err = UnpackFromEnv(key, config).Defaults().Validate().Do()
	default:
		err = UnPackFromFile(key, config).Defaults().Validate().Do()
	}
	if err != nil {
		log.Fatal("unpack global config file error: %+v", err)
	}
}
