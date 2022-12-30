
<img src="https://github.com/loggie-io/loggie/blob/main/logo/loggie.svg" width="250">

[![go.dev reference](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white)](https://loggie-io.github.io/docs/)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/569/badge)](https://bestpractices.coreinfrastructure.org/projects/569)
[![dockerHub](https://img.shields.io/badge/dockerHub-loggieio%2Floggie-9cf)](https://hub.docker.com/r/loggieio/loggie/)

> English | [中文](./README_cn.md)

Loggie is a lightweight, high-performance, cloud-native agent and aggregator based on Golang.

- Supports multiple pipeline and pluggable components, including data transfer, filtering, parsing, and alarm functions.
- Uses native Kubernetes CRD for operation and management.
- Offers a range of observability, reliability, and automation features suitable for production environments.

## Architecture

![](https://loggie-io.github.io/docs/getting-started/imgs/loggie-arch.png)


## [Documentation](https://loggie-io.github.io/docs-en/)

### [Setup](https://loggie-io.github.io/docs-en/getting-started/overview/)

- [Quickstart](https://loggie-io.github.io/docs-en/getting-started/quick-start/quick-start/)
- Installation ([Kubernetes](https://loggie-io.github.io/docs-en/getting-started/install/kubernetes/), [Node](https://loggie-io.github.io/docs-en/getting-started/install/node/))

### [User Guide](https://loggie-io.github.io/docs-en/user-guide/)

- [Architecture](https://loggie-io.github.io/docs-en/user-guide/architecture/core-arch/)
- [Kubernetes](https://loggie-io.github.io/docs-en/user-guide/use-in-kubernetes/general-usage/)
- [Monitoring](https://loggie-io.github.io/docs-en/user-guide/monitor/loggie-monitor/)

### [Reference](https://loggie-io.github.io/docs-en/reference/)

- [Args](https://loggie-io.github.io/docs-en/reference/global/args/)
- [System](https://loggie-io.github.io/docs-en/reference/global/monitor/)
- Pipelines
    - source: [file](https://loggie-io.github.io/docs-en/reference/pipelines/source/file/), [kafka](https://loggie-io.github.io/docs-en/reference/pipelines/source/kafka/), [kubeEvent](https://loggie-io.github.io/docs-en/reference/pipelines/source/kube-event/), [grpc](https://loggie-io.github.io/docs-en/reference/pipelines/source/grpc/)..
    - sink: [elasticsearch](https://loggie-io.github.io/docs-en/reference/pipelines/sink/elasticsearch/), [kafka](https://loggie-io.github.io/docs-en/reference/pipelines/sink/kafka/), [grpc](https://loggie-io.github.io/docs-en/reference/pipelines/sink/grpc/), [dev](https://loggie-io.github.io/docs-en/reference/pipelines/sink/dev/)..
    - interceptor: [transformer](https://loggie-io.github.io/docs-en/reference/pipelines/interceptor/transformer/), [limit](https://loggie-io.github.io/docs-en/reference/pipelines/interceptor/limit/), [logAlert](https://loggie-io.github.io/docs-en/reference/pipelines/interceptor/logalert/), [maxbytes](https://loggie-io.github.io/docs-en/reference/pipelines/interceptor/maxbytes/)..
- CRD ([logConfig](https://loggie-io.github.io/docs-en/reference/discovery/kubernetes/logconfig/), [sink](https://loggie-io.github.io/docs-en/reference/discovery/kubernetes/sink/), [interceptor](https://loggie-io.github.io/docs-en/reference/discovery/kubernetes/interceptors/))

## License

[Apache-2.0](https://choosealicense.com/licenses/apache-2.0/)

## Contributions

Pull requests, comments and suggestions are welcome.

Please see [CONTRIBUTING.md](CONTRIBUTING.md) for more information.
