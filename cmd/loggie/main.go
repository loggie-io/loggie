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

package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/loggie-io/loggie/cmd/subcmd"
	"github.com/loggie-io/loggie/pkg/control"
	"github.com/loggie-io/loggie/pkg/core/cfg"
	"github.com/loggie-io/loggie/pkg/core/global"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/core/reloader"
	"github.com/loggie-io/loggie/pkg/core/signals"
	"github.com/loggie-io/loggie/pkg/core/sysconfig"
	"github.com/loggie-io/loggie/pkg/discovery/kubernetes"
	"github.com/loggie-io/loggie/pkg/eventbus"
	_ "github.com/loggie-io/loggie/pkg/include"
	"github.com/loggie-io/loggie/pkg/ops/helper"
	"github.com/loggie-io/loggie/pkg/util/persistence"
	"github.com/loggie-io/loggie/pkg/util/yaml"
	"github.com/pkg/errors"
	"go.uber.org/automaxprocs/maxprocs"
)

var (
	globalConfigFile   string
	pipelineConfigPath string
	configType         string
	nodeName           string
)

func init() {
	hostName, _ := os.Hostname()

	flag.StringVar(&globalConfigFile, "config.system", "loggie.yml", "global config file")
	flag.StringVar(&pipelineConfigPath, "config.pipeline", "pipelines.yml", "reloadable config file")
	flag.StringVar(&configType, "config.from", "file", "config from file or env")
	flag.StringVar(&nodeName, "meta.nodeName", hostName, "override nodeName")
}

func main() {
	flag.Parse()
	// init logging configuration
	log.InitDefaultLogger()

	if err := subcmd.SwitchSubCommand(); err != nil {
		return
	}

	log.Info("version: %s", global.GetVersion())

	// set up signals, so we handle the first shutdown signal gracefully
	stopCh := signals.SetupSignalHandler()

	// Automatically set GOMAXPROCS to match Linux container CPU quota
	if _, err := maxprocs.Set(maxprocs.Logger(log.Debug)); err != nil {
		log.Fatal("set maxprocs error: %v", err)
	}
	log.Info("real GOMAXPROCS %d", runtime.GOMAXPROCS(-1))

	global.NodeName = nodeName
	log.Info("node name: %s", nodeName)

	// system config file
	syscfg := sysconfig.Config{}
	cfg.UnpackTypeDefaultsAndValidate(strings.ToLower(configType), globalConfigFile, &syscfg)

	// start eventBus listeners
	eventbus.StartAndRun(syscfg.Loggie.MonitorEventBus)
	// init log after error func
	log.AfterError = eventbus.AfterErrorFunc

	log.Info("pipelines config path: %s", pipelineConfigPath)
	// pipeline config file
	pipecfgs, err := control.ReadPipelineConfig(pipelineConfigPath, configType, func(s os.FileInfo) bool {
		return false
	})
	if pipecfgs != nil {
		out, yamlErr := yaml.Marshal(pipecfgs)
		if yamlErr != nil {
			log.Fatal("marshal initial pipelines failed: %v, config:\n%s", yamlErr, out)
		}
		log.Info("initial pipelines:\n%s", out)
	}

	if err != nil && !os.IsNotExist(err) && !errors.Is(err, control.ErrIgnoreAllFile) {
		log.Fatal("unpack config.pipeline config file err: %v", err)
	}

	persistence.SetConfig(syscfg.Loggie.Db)
	defer persistence.StopDbHandler()

	controller := control.NewController()
	controller.Start(pipecfgs)

	if syscfg.Loggie.Reload.Enabled {
		syscfg.Loggie.Reload.ConfigPath = pipelineConfigPath
		reloader.Setup(stopCh, controller, &syscfg.Loggie.Reload)
	}

	if syscfg.Loggie.Discovery.Enabled {
		k8scfg := syscfg.Loggie.Discovery.Kubernetes
		k8scfg.NodeName = nodeName
		k8scfg.ConfigFilePath = filepath.Dir(pipelineConfigPath)
		k8sDiscovery := kubernetes.NewDiscovery(&k8scfg)

		go k8sDiscovery.Start(stopCh)
	}

	// api for debugging
	helper.Setup(controller)

	if syscfg.Loggie.Http.Enabled {
		go func() {
			if err = http.ListenAndServe(fmt.Sprintf("%s:%d", syscfg.Loggie.Http.Host, syscfg.Loggie.Http.Port), nil); err != nil {
				log.Fatal("http listen and serve err: %v", err)
			}
		}()
	}

	log.Info("started Loggie")
	<-stopCh
	log.Info("shutting down Loggie")
}
