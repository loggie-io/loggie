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
	"context"
	"encoding/json"
	"fmt"
	"github.com/loggie-io/loggie/pkg/core/global"
	"go.uber.org/atomic"
	"os"
	"strings"
	"time"

	"github.com/loggie-io/loggie/pkg/core/api"
	"github.com/loggie-io/loggie/pkg/core/event"
	"github.com/loggie-io/loggie/pkg/core/log"
	"github.com/loggie-io/loggie/pkg/pipeline"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

const Type = "kubeEvent"

func init() {
	pipeline.Register(api.SOURCE, Type, makeSource)
}

func makeSource(info pipeline.Info) api.Component {
	return &KubeEvent{
		config:              &Config{},
		eventPool:           info.EventPool,
		blackListNamespaces: make(map[string]struct{}),
	}
}

type KubeEvent struct {
	name      string
	config    *Config
	eventPool *event.Pool

	event        chan interface{}
	ctx          context.Context
	cancel       context.CancelFunc
	bindInformer atomic.Bool
	le           *leaderelection.LeaderElector

	startTime           time.Time
	blackListNamespaces map[string]struct{}
	blacklistEnabled    bool
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

func (k *KubeEvent) Init(context api.Context) error {
	k.name = context.Name()
	k.event = make(chan interface{}, k.config.BufferSize)

	if k.config.LatestEventsEnabled {
		k.startTime = time.Now().UTC()
		k.startTime = k.startTime.Add(-k.config.LatestEventsPreviousTime)
	}

	for _, ns := range k.config.BlackListNamespaces {
		k.blackListNamespaces[ns] = struct{}{}
	}
	if len(k.blackListNamespaces) > 0 {
		k.blacklistEnabled = true
	}

	k.bindInformer.Store(false)
	return nil
}

func (k *KubeEvent) Start() error {
	k.ctx, k.cancel = context.WithCancel(context.Background())

	config, err := clientcmd.BuildConfigFromFlags(k.config.Master, k.config.KubeConfig)
	if err != nil {
		log.Error("cannot build config: %v", err)
		return err
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Error("cannot build clientSet: %v", err)
		return err
	}

	name, err := os.Hostname()
	if err != nil {
		log.Error("get Hostname error: %s", err)
	}

	var leaderIdentity strings.Builder
	leaderIdentity.WriteString(global.NodeName)
	leaderIdentity.WriteString(":")
	leaderIdentity.WriteString(name)

	rl, err := resourcelock.New(resourcelock.LeasesResourceLock,
		k.config.LeaderElectionNamespace,
		k.config.LeaderElectionKey,
		client.CoreV1(),
		client.CoordinationV1(),
		resourcelock.ResourceLockConfig{
			Identity: leaderIdentity.String(),
		})
	if err != nil {
		log.Error("error creating lock: %v", err)
	}

	k.le, err = leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:          rl,
		LeaseDuration: 15 * time.Second,
		RenewDeadline: 10 * time.Second,
		RetryPeriod:   2 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				if k.bindInformer.Load() == true {
					return
				}
				k.run(ctx, client)
			},
			OnStoppedLeading: func() {
				select {
				case <-k.ctx.Done():
					// We were asked to terminate. Exit 0.
					log.Info("Requested to terminate. Exiting.")
				default:
					// We lost the lock.
					log.Info("leader election lost")
					k.le.Run(k.ctx)
				}
			},
		},
	})
	if err != nil {
		log.Error("leader election failed, it would be member")
		return nil
	}

	go k.le.Run(k.ctx)

	return nil
}

func (k *KubeEvent) run(ctx context.Context, cli kubernetes.Interface) {
	log.Info("starting kubernetes events informer")
	informerFactory := informers.NewSharedInformerFactory(cli, 0)
	eventInformer := informerFactory.Core().V1().Events()
	eventInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			ev := obj.(*corev1.Event)
			if ev.ResourceVersion == "" {
				return
			}
			if k.filter(ev) {
				return
			}
			k.event <- obj
		},
		UpdateFunc: func(old, new interface{}) {
			oldObj := old.(*corev1.Event)
			newObj := new.(*corev1.Event)
			if newObj.ResourceVersion == "" {
				return
			}
			if oldObj.ResourceVersion == newObj.ResourceVersion {
				return
			}
			if k.filter(newObj) {
				return
			}
			k.event <- newObj
		},
		DeleteFunc: func(obj interface{}) {

		},
	})

	informerFactory.Start(ctx.Done())
	informerFactory.WaitForCacheSync(ctx.Done())
}

func (k *KubeEvent) Stop() {
	k.cancel()
}

func (k *KubeEvent) ProductLoop(productFunc api.ProductFunc) {
	log.Info("%s start product loop", k.String())

	for {
		select {
		case <-k.ctx.Done():
			k.bindInformer.Store(false)
			return

		case obj := <-k.event:
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
}

func (k *KubeEvent) Commit(events []api.Event) {
	k.eventPool.PutAll(events)
}

func (k *KubeEvent) filter(ev *corev1.Event) bool {
	if k.config.LatestEventsEnabled {

		zeroTime := time.Time{}

		if ev.LastTimestamp.Time != zeroTime {
			if !ev.CreationTimestamp.After(k.startTime) {
				return true
			}
		}

		if ev.Series.LastObservedTime.Time != zeroTime {
			if !ev.Series.LastObservedTime.After(k.startTime) {
				return true
			}
		}

		if !ev.CreationTimestamp.After(k.startTime) {
			return true
		}
	}

	if k.blacklistEnabled {
		if _, ok := k.blackListNamespaces[ev.Namespace]; ok {
			return true
		}
	}

	return false
}
