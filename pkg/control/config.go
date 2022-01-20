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

package control

import (
	"github.com/loggie-io/loggie/pkg/core/cfg"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"os"
	"path/filepath"
)

var (
	ErrPipeNameUniq = errors.New("pipeline name is duplicated")
)

type PipelineConfig struct {
	Pipelines []pipeline.Config `yaml:"pipelines" validate:"dive,required"`
}

type PipelineRawConfig struct {
	Pipelines []pipeline.ConfigRaw `yaml:"pipelines" validate:"dive,required"`
}

func (pr *PipelineRawConfig) SetDefaults() {
	for i := range pr.Pipelines {
		pr.Pipelines[i].SetDefaults()
	}
}

func (pr *PipelineRawConfig) ValidateUniquePipeName() error {
	unique := make(map[string]struct{})
	for _, p := range pr.Pipelines {
		if _, ok := unique[p.Name]; ok {
			return errors.WithMessagef(ErrPipeNameUniq, "invalidate pipeline name %s", p.Name)
		}
		unique[p.Name] = struct{}{}
	}

	return nil
}

func (pr *PipelineRawConfig) Validate() error {
	unique := make(map[string]struct{})
	for _, p := range pr.Pipelines {
		if _, ok := unique[p.Name]; ok {
			return errors.WithMessagef(ErrPipeNameUniq, "invalidate pipeline name %s", p.Name)
		}
		unique[p.Name] = struct{}{}

		err := p.Validate()
		if err != nil {
			return err
		}
	}
	return nil
}

func (pr *PipelineRawConfig) ValidateAndRemove() (*PipelineConfig, error) {
	validPipes := &PipelineConfig{}

	var errs []error
	unique := make(map[string]struct{})
	for _, p := range pr.Pipelines {
		if _, ok := unique[p.Name]; ok {
			errs = append(errs, errors.WithMessagef(ErrPipeNameUniq, "invalidate pipeline name %s", p.Name))
			continue
		}
		unique[p.Name] = struct{}{}

		pip, err := p.ValidateAndToConfig()
		if err != nil {
			errs = append(errs, err)
			continue
		}

		validPipes.Pipelines = append(validPipes.Pipelines, *pip)
	}

	return validPipes, utilerrors.NewAggregate(errs)
}

func (pr *PipelineRawConfig) DeepCopy() (dest *PipelineRawConfig, err error) {
	out, err := yaml.Marshal(pr)
	if err != nil {
		return nil, err
	}

	d := new(PipelineRawConfig)
	if err = yaml.Unmarshal(out, d); err != nil {
		return nil, err
	}
	return d, nil
}

func (pr *PipelineRawConfig) ToConfig() (*PipelineConfig, error) {
	ret := &PipelineConfig{}
	for _, p := range pr.Pipelines {
		r, err := p.ToConfig()
		if err != nil {
			return nil, err
		}

		ret.AddPipeline(*r)
	}

	return ret, nil
}

func (c *PipelineConfig) AddPipelines(cfg []pipeline.Config) {
	c.Pipelines = append(c.Pipelines, cfg...)
}

func (c *PipelineConfig) AddPipeline(cfg pipeline.Config) {
	c.Pipelines = append(c.Pipelines, cfg)
}

func (c *PipelineConfig) RemovePipelines(cfg []pipeline.Config) {
	for _, p := range cfg {
		target := p.Name
		for i := 0; i < len(c.Pipelines); i++ {
			if target == c.Pipelines[i].Name {
				c.Pipelines = append(c.Pipelines[:i], c.Pipelines[i+1:]...)
			}
		}
	}
}

type FileIgnore func(s os.FileInfo) bool

func ReadPipelineConfig(path string, ignore FileIgnore) (*PipelineConfig, error) {
	pipecfgs := &PipelineConfig{}
	matches, err := filepath.Glob(path)
	if err != nil {
		return nil, err
	}
	for _, m := range matches {
		s, err := os.Stat(m)
		if err != nil {
			return nil, err
		}
		if s.IsDir() {
			continue
		}

		if ignore(s) {
			continue
		}

		content, err := ioutil.ReadFile(m)
		if err != nil {
			log.Warn("read config error. err: %v", err)
			return nil, err
		}

		pipes, err := defaultsValidateAndRemove(content)
		if err != nil {
			// ignore invalid pipeline
			log.Info("invalidate pipeline configs: %v, \n%s", err, content)
		}
		pipecfgs.AddPipelines(pipes.Pipelines)
	}
	return pipecfgs, nil
}

// set defaults, validate pipelines, and remove invalid pipeline configs, so the invalid pipeline wouldn't start
func defaultsValidateAndRemove(content []byte) (*PipelineConfig, error) {
	pipraw := PipelineRawConfig{}
	err := cfg.UnpackRawAndDefaults(content, &pipraw)
	if err != nil {
		return nil, err
	}

	return pipraw.ValidateAndRemove()
}
