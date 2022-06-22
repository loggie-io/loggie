module github.com/loggie-io/loggie

go 1.17

require (
	github.com/aliyun/aliyun-log-go-sdk v0.1.35
	github.com/andres-erbsen/clock v0.0.0-20160526145045-9e14626cd129
	github.com/bmatcuk/doublestar/v4 v4.0.2
	github.com/creasty/defaults v1.5.1
	github.com/docker/docker v17.12.0-ce-rc1.0.20200706150819-a40b877fbb9e+incompatible
	github.com/fsnotify/fsnotify v1.4.9
	github.com/go-playground/validator/v10 v10.4.1
	github.com/goccy/go-yaml v1.9.5
	github.com/gogo/protobuf v1.3.2
	github.com/golang/snappy v0.0.2
	github.com/google/go-cmp v0.5.7
	github.com/hpcloud/tail v1.0.0
	github.com/json-iterator/go v1.1.12
	github.com/mattn/go-sqlite3 v1.14.6
	github.com/mattn/go-zglob v0.0.3
	github.com/mmaxiaolei/backoff v0.0.0-20210104115436-e015e09efaba
	github.com/olivere/elastic/v7 v7.0.28
	github.com/panjf2000/ants/v2 v2.4.7
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.11.1
	github.com/prometheus/common v0.32.1
	github.com/prometheus/prom2json v1.3.0
	github.com/prometheus/prometheus v1.8.2-0.20201028100903-3245b3267b24
	github.com/rs/zerolog v1.20.0
	github.com/segmentio/kafka-go v0.4.23
	github.com/shirou/gopsutil/v3 v3.22.2
	github.com/smartystreets-prototypes/go-disruptor v0.0.0-20200316140655-c96477fd7a6a
	go.uber.org/automaxprocs v0.0.0-20200415073007-b685be8c1c23
	golang.org/x/net v0.0.0-20220127200216-cd36cc0744dd
	golang.org/x/time v0.0.0-20210723032227-1f47c861a9ac
	google.golang.org/grpc v1.40.0
	google.golang.org/protobuf v1.27.1
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
	k8s.io/api v0.22.2
	k8s.io/apimachinery v0.22.2
	k8s.io/client-go v0.22.2
	k8s.io/code-generator v0.22.2
	k8s.io/cri-api v0.24.0
)

require (
	github.com/apache/pulsar-client-go v0.8.1
	github.com/sirupsen/logrus v1.6.0
)

replace (
	github.com/docker/docker => github.com/docker/docker v1.13.1
	google.golang.org/grpc => google.golang.org/grpc v1.33.2
	google.golang.org/protobuf => google.golang.org/protobuf v1.26.0
	gopkg.in/natefinch/lumberjack.v2 v2.0.0 => github.com/machine3/lumberjack v0.1.0
)
