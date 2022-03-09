# Loggie release v1.0.0

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