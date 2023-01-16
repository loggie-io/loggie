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
	"os"
	"path/filepath"

	"github.com/loggie-io/loggie/pkg/core/cfg"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/pipeline"
	"github.com/pkg/errors"
)

var (
	ErrPipeNameUniq  = errors.New("pipeline name is duplicated")
	ErrIgnoreAllFile = errors.New("ignore all the file")
)

type PipelineConfig struct {
	Pipelines []pipeline.Config `yaml:"pipelines" validate:"dive,required"`
}

func (c *PipelineConfig) DeepCopy() *PipelineConfig {
	if c == nil {
		return nil
	}

	out := new(PipelineConfig)
	if len(c.Pipelines) > 0 {
		pip := make([]pipeline.Config, 0)
		for _, p := range c.Pipelines {
			pip = append(pip, *p.DeepCopy())
		}
		out.Pipelines = pip
	}

	return out
}

func (c *PipelineConfig) Validate() error {
	if err := c.ValidateUniquePipeName(); err != nil {
		return err
	}
	for i := range c.Pipelines {
		if err := c.Pipelines[i].Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (c *PipelineConfig) ValidateUniquePipeName() error {
	unique := make(map[string]struct{})
	for _, p := range c.Pipelines {
		if _, ok := unique[p.Name]; ok {
			return errors.WithMessagef(ErrPipeNameUniq, "invalidate pipeline name %s", p.Name)
		}
		unique[p.Name] = struct{}{}
	}

	return nil
}

func (c *PipelineConfig) AddPipelines(cfg []pipeline.Config) {
	c.Pipelines = append(c.Pipelines, cfg...)
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

func ReadPipelineConfigFromFile(path string, ignore FileIgnore) (*PipelineConfig, error) {
	pipecfgs := &PipelineConfig{}
	matches, err := filepath.Glob(path)
	if err != nil {
		return nil, err
	}

	var all []string
	allIgnored := true
	for _, m := range matches {
		s, err := os.Stat(m)
		if err != nil {
			return nil, err
		}

		if s.IsDir() {
			continue
		}

		all = append(all, m)
		if ignore(s) {
			continue
		}

		// reach here only when there exists a recently modified cfg file
		allIgnored = false
	}

	// if all files are ignored, then do not read any file.
	if allIgnored {
		return pipecfgs, ErrIgnoreAllFile
	}

	// if any file should not be ignored, then all files are read.
	for _, fn := range all {
		pipes := &PipelineConfig{}
		unpack := cfg.UnPackFromFile(fn, pipes)
		if err = unpack.Defaults().Validate().Do(); err != nil {
			log.Error("invalid pipeline configs: %v, \n%s", err, unpack.Contents())
			continue
		}
		pipecfgs.AddPipelines(pipes.Pipelines)
	}
	return pipecfgs, nil
}

func ReadPipelineConfigFromEnv(key string, _ FileIgnore) (*PipelineConfig, error) {
	pipecfgs := &PipelineConfig{}
	if err := cfg.UnpackFromEnv(key, pipecfgs).Defaults().Validate().Do(); err != nil {
		// ignore invalid pipeline
		log.Error("pipeline configs invalid: %v, \n%s", err, key)
		return nil, err
	}
	return pipecfgs, nil
}

func ReadPipelineConfig(path string, configType string, ignore FileIgnore) (*PipelineConfig, error) {
	switch configType {
	case "file":
		return ReadPipelineConfigFromFile(path, ignore)
	case "env":
		return ReadPipelineConfigFromEnv(path, ignore)
	default:
		return ReadPipelineConfigFromFile(path, ignore)
	}
}
