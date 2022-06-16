# Release v1.2.0

### :star2: Features

- Support container root filesystem log collection, so Loggie could collect the log files in container even if the pod is not mounting any volumes. #209
- Add `sls` sink for aliyun. #172
- Support specifying more time intervals(like 1d, 1w, 1y) for the "ignoreOlder" config of file source. #153 (by @iamyeka)
- Add `ignoreError` in normalize interceptor. #205
- Add system listener, which can export CPU/Memory metrics of Loggie. #88
- Set `log.jsonFormat` default to false, so Loggie would no longer print json format logs in default. #186
- Add `callerSkipCount` and `noColor` logger flag. #185
- `file sink` support codec. #215 (by @lyp256)
- Support linux arm64. #229 (by @ziyu-zhao)
- `addk8smeta` support adding workload metadata. #221 (by @lyp256)
- Support extra labels in prometheusExporter source. #233
- Logconfig matchFields labels, annotations and env support wildcards(*). #235 (by @LeiZhang-Hunter)
- Add source codec for file source. #239
- Some specific fields pattern now supports `${_env.XX}`, `${+date}`, `${object}`, `${_k8s.XX}`. #248
- We support rename `body` of events in normalize interceptor now. #238
- Use local time format in pattern fields. #234
- Refactor reader process chain. #166
- Optimize merge strategy of default interceptors. #174 (by @iamyeka)
- Merge defaults configurations support recursion. #212
- Cache env for optimizing performance when using fieldsFromEnv. #178 (by @iamyeka)
- Update yaml lib to goccy/go-yaml. #242
- Oomit empty pipeline configurations. #244

### :bug: Bug Fixes

- Fix json error with go 1.18. #164
- Fix list logconfigs by namespace when handling pod events. #173
- Fix watcher stop timeout in file source. #188
- Fix MultiFileWriter in file sink. #202 (by @lyp256)
- Fix pipeline stop may block. #207
- Fix unstable interceptor sort. #216
- Fix grpc source ack panic. #218
- Fix sub path in kubernetes discovery. #236 (by @yuzhichang)


# Release v1.1.0


### :star2: Features

- Loki sink supported, so you can send messages to Loki now. #119

  > eg:
  >
  > ```yaml
  > sink:
  >   type: loki
  >   url: "http://localhost:3100/loki/api/v1/push"
  > ```

- Add spec.interceptors and spec.sink fields in ClusterLogConfig/LogConfig CRD #120

  > eg:
  >
  > ```yaml
  > apiVersion: loggie.io/v1beta1
  > kind: LogConfig
  > metadata:
  >   name: tomcat
  >   namespace: default
  > spec:
  >   selector:
  >     type: pod
  >     labelSelector:
  >       app: tomcat
  >   pipeline:
  >     sources: |
  >       - type: file
  >         name: common
  >         paths:
  >           - stdout
  >     sink: |
  >       type: dev
  >       printEvents: false
  >     interceptors: |
  >       - type: rateLimit
  >         qps: 90000
  > ```

- A new codec type `raw` is added. #137

  > eg:
  >
  > ```yaml
  >     sink:
  >       type: dev
  >       codec:
  >         type: raw
  > ```

- A new approach to external configurations, `-config.from=env`, allows Loggie to read configs from env.  #132 (by @chaunceyjiang)

- Normalize interceptor now support `fmt` processor. This can reformat the fields by patterns. #141

  > eg:
  >
  > ```yaml
  > interceptors:
  > - type: normalize
  >   processors:
  >   - fmt:
  >       fields:
  >         d: new-${a.b}-${c}
  > ```

- Adding `enabled` fields in source/interceptor. #140

- We would flush the pipelines.yml immediately when updating sink/interceptor CR. #139

- Upgraded client-go version to 0.22.2  #99 (by @fengshunli)

- `sniff` are disabled in elasticsearch sink by default. #138

- Updated kubeEvent source #144

  - leader selection support.

  - blacklist of namespaces support.

  - adding `watchLatestEvents` fields, allows Loggie watch the lastest events from K8s.

  > eg:
  >
  > ```yaml
  >       - type: kubeEvent
  >         name: eventer
  >         kubeconfig: ～/.kube/config
  >         watchLatestEvents: true
  >         blackListNamespaces: ["default"]
  > ```

- Brace Expansion：{alt1,...} suppport, matches a sequence of characters if one of the comma-separated alternatives matches.

  > eg:
  >
  > log files following:
  >
  > 1. /tmp/loggie/service/order/access.log
  > 2. /tmp/loggie/service/order/access.log.2022-04-11
  > 3. /tmp/loggie/service/pay/access.log
  > 4. /tmp/loggie/service/pay/access.log.2022-04-11
  >
  > then the path field in filesource could be: /tmp/loggie/**/access.log{,.[2-9][0-9][0-9][0-9]-[01][0-9]-[0123][0-9]}

- Adding `local` in normalize interceptor, in case we want to parse timestamp with local time.

- We support a simple way to add file state when collecting log files, enable `addonMeta`, then Loggie will add more metadata of files in events.

  > eg:
  >
  > ```json
  > {
  >   "body": "this is test",
  >   "state": {
  >     "pipeline": "local",
  >     "source": "demo",
  >     "filename": "/var/log/a.log",
  >     "timestamp": "2006-01-02T15:04:05.000Z",
  >     "offset": 1024,
  >     "bytes": 4096,
  >     "hostname": "node-1"
  >   }
  > }
  > ```

- Adding `pod.ip`和`node.ip` in K8s discovery.

- Customized `documentId` supported in elasticsearch sink.

### :bug: Bug Fixes

- Fix read chan size error in filesource  #102
- Update default value of `maxAcceptedBytes` in kafka source which may cause panic in component starting. #112

---

# Release v1.0.0

First Loggie Release! :sparkler:

### :star2: Features

add components:
- sources:
    - file
    - kafka
    - grpc
    - unix
    - kubeEvent
    - prometheusExporter
    - dev

- interceptors:
    - normalize
    - rateLimit
    - addK8sMeta
    - logAlert
    - metric
    - retry
    - maxbytes

- sink:
    - elasticsearch
    - kafka
    - grpc
    - file
    - dev

- discovery:
    - Kubernetes discovery, introduced CRD: ClusterLogConfig/LogConfig/Interceptor/Sink

- monitor:
    - filesource
    - filewatcher
    - logAlert
    - queue
    - sink
    - reload

### :bug: Bug Fixes