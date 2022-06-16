
<img src="https://github.com/loggie-io/loggie/blob/main/logo/loggie.svg" width="250">

[![go.dev reference](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white)](https://loggie-io.github.io/docs/)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/569/badge)](https://bestpractices.coreinfrastructure.org/projects/569)
[![dockerHub](https://img.shields.io/badge/dockerHub-loggieio%2Floggie-9cf)](https://hub.docker.com/r/loggieio/loggie/)

Loggie是一个基于Golang的轻量级、高性能、云原生日志采集Agent和中转处理Aggregator，支持多Pipeline和组件热插拔，提供了：

- **一栈式日志解决方案**：同时支持日志中转、过滤、解析、切分、日志报警等
- **云原生的日志形态**：快速便捷的容器日志采集方式，原生的Kubernetes CRD动态配置下发
- **生产级的特性**：基于长期的大规模运维经验，形成了全方位的可观测性、快速排障、异常预警、自动化运维能力

## 架构

![](https://loggie-io.github.io/docs/getting-started/imgs/loggie-arch.png)


## 文档
请参考Loggie[文档](https://loggie-io.github.io/docs/)。

### 开始

- [快速上手](https://loggie-io.github.io/docs/getting-started/quick-start/quick-start/)
- 部署([Kubernetes](https://loggie-io.github.io/docs/getting-started/install/kubernetes/), [主机](https://loggie-io.github.io/docs/getting-started/install/node/))

### 用户指南

- [设计与架构](https://loggie-io.github.io/docs/user-guide/architecture/core-arch/)
- [在Kubernetes下使用](https://loggie-io.github.io/docs/user-guide/use-in-kubernetes/general-usage/)
- [监控与告警](https://loggie-io.github.io/docs/user-guide/monitor/loggie-monitor/)

### 组件配置

- [启动参数](https://loggie-io.github.io/docs/reference/global/args/)
- [系统配置](https://loggie-io.github.io/docs/reference/global/system/)
- Pipeline配置
  - source: [file](https://loggie-io.github.io/docs/reference/pipelines/source/file/), [kafka](https://loggie-io.github.io/docs/reference/pipelines/source/kafka/), [kubeEvent](https://loggie-io.github.io/docs/reference/pipelines/source/kubeEvent/), [grpc](https://loggie-io.github.io/docs/reference/pipelines/source/grpc/)..
  - sink: [elassticsearch](https://loggie-io.github.io/docs/reference/pipelines/sink/elasticsearch/), [kafka](https://loggie-io.github.io/docs/reference/pipelines/sink/kafka/), [grpc](https://loggie-io.github.io/docs/reference/pipelines/sink/grpc/), [dev](https://loggie-io.github.io/docs/reference/pipelines/sink/dev/)..
  - interceptor: [normalize](https://loggie-io.github.io/docs/reference/pipelines/interceptor/normalize/), [limit](https://loggie-io.github.io/docs/reference/pipelines/interceptor/limit/), [logAlert](https://loggie-io.github.io/docs/reference/pipelines/interceptor/logalert/), [maxbytes](https://loggie-io.github.io/docs/reference/pipelines/interceptor/maxbytes/)..
- CRD([logConfig](https://loggie-io.github.io/docs/reference/discovery/kubernetes/logconfig/), [sink](https://loggie-io.github.io/docs/reference/discovery/kubernetes/sink/), [interceptor](https://loggie-io.github.io/docs/reference/discovery/kubernetes/interceptors/))

## 交流讨论
在使用Loggie的时候遇到问题？ 请提issues或者联系我们。

微信扫码加入Loggie讨论群：

![loggie-bot](https://loggie-io.github.io/docs/getting-started/imgs/loggie-bot.png)

## License

[Apache-2.0](https://choosealicense.com/licenses/apache-2.0/)

## 贡献

欢迎提交代码、发表评论和建议。

查看 [CONTRIBUTING.md](CONTRIBUTING.md) 获得更多信息。
