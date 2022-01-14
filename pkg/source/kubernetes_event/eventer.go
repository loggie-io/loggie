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

package kubernetes_event

import (
	"encoding/json"
	"fmt"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/pipeline"
)

const Type = "kubeEvent"

func init() {
	pipeline.Register(api.SOURCE, Type, makeSource)
}

func makeSource(info pipeline.Info) api.Component {
	return &KubeEvent{
		config:    &Config{},
		stop:      make(chan struct{}),
		eventPool: info.EventPool,
	}
}

type KubeEvent struct {
	name      string
	config    *Config
	event     chan interface{}
	stop      chan struct{}
	eventPool *event.Pool
}

func (k *KubeEvent) Config() interface{} {
	return k.config
}

func (k *KubeEvent) Category() api.Category {
	return api.SOURCE
}

func (k *KubeEvent) Type() api.Type {
	return Type
}

func (k *KubeEvent) String() string {
	return fmt.Sprintf("%s/%s", api.SOURCE, Type)
}

func (k *KubeEvent) Init(context api.Context) {
	k.name = context.Name()
	k.event = make(chan interface{}, k.config.BufferSize)
}

func (k *KubeEvent) Start() {
	config, err := clientcmd.BuildConfigFromFlags(k.config.Master, k.config.KubeConfig)
	if err != nil {
		log.Error("cannot build config: %v", err)
		return
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Error("cannot build clientSet: %v", err)
		return
	}

	informerFactory := informers.NewSharedInformerFactory(clientset, 0)
	eventInformer := informerFactory.Core().V1().Events()
	eventInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			k.event <- obj
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			k.event <- newObj
		},
		DeleteFunc: func(obj interface{}) {

		},
	})

	informerFactory.Start(k.stop)
	informerFactory.WaitForCacheSync(k.stop)
}

func (k *KubeEvent) Stop() {
	close(k.stop)
}

func (k *KubeEvent) ProductLoop(productFunc api.ProductFunc) {
	log.Info("%s start product loop", k.String())

	for obj := range k.event {
		jsonBytes, err := json.Marshal(obj)
		if err != nil {
			log.Warn("json parse error: %s", err.Error())
			return
		}

		e := k.eventPool.Get()
		e.Fill(e.Meta(), e.Header(), jsonBytes)

		productFunc(e)
	}

}

func (k *KubeEvent) Commit(events []api.Event) {
	k.eventPool.PutAll(events)
}
