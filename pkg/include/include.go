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

package include

import (
	_ "loggie.io/loggie/pkg/eventbus/export/prometheus"
	_ "loggie.io/loggie/pkg/eventbus/listener/filesource"
	_ "loggie.io/loggie/pkg/eventbus/listener/filewatcher"
	_ "loggie.io/loggie/pkg/eventbus/listener/logalerting"
	_ "loggie.io/loggie/pkg/eventbus/listener/pipeline"
	_ "loggie.io/loggie/pkg/eventbus/listener/queue"
	_ "loggie.io/loggie/pkg/eventbus/listener/reload"
	_ "loggie.io/loggie/pkg/eventbus/listener/sink"
	_ "loggie.io/loggie/pkg/interceptor/cost"
	_ "loggie.io/loggie/pkg/interceptor/json_decode"
	_ "loggie.io/loggie/pkg/interceptor/limit"
	_ "loggie.io/loggie/pkg/interceptor/logalert"
	_ "loggie.io/loggie/pkg/interceptor/maxbytes"
	_ "loggie.io/loggie/pkg/interceptor/metric"
	_ "loggie.io/loggie/pkg/interceptor/normalize"
	_ "loggie.io/loggie/pkg/interceptor/retry"
	_ "loggie.io/loggie/pkg/queue/channel"
	_ "loggie.io/loggie/pkg/queue/memory"
	_ "loggie.io/loggie/pkg/sink/codec/json"
	_ "loggie.io/loggie/pkg/sink/dev"
	_ "loggie.io/loggie/pkg/sink/elasticsearch"
	_ "loggie.io/loggie/pkg/sink/grpc"
	_ "loggie.io/loggie/pkg/sink/kafka"
	_ "loggie.io/loggie/pkg/source/dev"
	_ "loggie.io/loggie/pkg/source/file"
	_ "loggie.io/loggie/pkg/source/grpc"
	_ "loggie.io/loggie/pkg/source/kafka"
	_ "loggie.io/loggie/pkg/source/kubernetes_event"
)
