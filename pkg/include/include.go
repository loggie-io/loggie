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
	_ "github.com/loggie-io/loggie/pkg/eventbus/export/prometheus"
	_ "github.com/loggie-io/loggie/pkg/eventbus/listener/filesource"
	_ "github.com/loggie-io/loggie/pkg/eventbus/listener/filewatcher"
	_ "github.com/loggie-io/loggie/pkg/eventbus/listener/logalerting"
	_ "github.com/loggie-io/loggie/pkg/eventbus/listener/pipeline"
	_ "github.com/loggie-io/loggie/pkg/eventbus/listener/queue"
	_ "github.com/loggie-io/loggie/pkg/eventbus/listener/reload"
	_ "github.com/loggie-io/loggie/pkg/eventbus/listener/sink"
	_ "github.com/loggie-io/loggie/pkg/interceptor/cost"
	_ "github.com/loggie-io/loggie/pkg/interceptor/json_decode"
	_ "github.com/loggie-io/loggie/pkg/interceptor/limit"
	_ "github.com/loggie-io/loggie/pkg/interceptor/logalert"
	_ "github.com/loggie-io/loggie/pkg/interceptor/maxbytes"
	_ "github.com/loggie-io/loggie/pkg/interceptor/metric"
	_ "github.com/loggie-io/loggie/pkg/interceptor/normalize"
	_ "github.com/loggie-io/loggie/pkg/interceptor/retry"
	_ "github.com/loggie-io/loggie/pkg/queue/channel"
	_ "github.com/loggie-io/loggie/pkg/queue/memory"
	_ "github.com/loggie-io/loggie/pkg/sink/codec/json"
	_ "github.com/loggie-io/loggie/pkg/sink/dev"
	_ "github.com/loggie-io/loggie/pkg/sink/elasticsearch"
	_ "github.com/loggie-io/loggie/pkg/sink/file"
	_ "github.com/loggie-io/loggie/pkg/sink/grpc"
	_ "github.com/loggie-io/loggie/pkg/sink/kafka"
	_ "github.com/loggie-io/loggie/pkg/source/dev"
	_ "github.com/loggie-io/loggie/pkg/source/file"
	_ "github.com/loggie-io/loggie/pkg/source/grpc"
	_ "github.com/loggie-io/loggie/pkg/source/kafka"
	_ "github.com/loggie-io/loggie/pkg/source/kubernetes_event"
	_ "github.com/loggie-io/loggie/pkg/source/prometheus_exporter"
	_ "github.com/loggie-io/loggie/pkg/source/unix"
)
