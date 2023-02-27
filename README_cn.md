
<img src="https://github.com/loggie-io/loggie/blob/main/logo/loggie-draw.png" width="250">

[![go.dev reference](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white)](https://loggie-io.github.io/docs/)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/569/badge)](https://bestpractices.coreinfrastructure.org/projects/569)
[![dockerHub](https://img.shields.io/badge/dockerHub-loggieio%2Floggie-9cf)](https://hub.docker.com/r/loggieio/loggie/)

Loggie是一个基于Golang的轻量级、高性能、云原生日志采集Agent和中转处理Aggregator，支持多Pipeline和组件热插拔，提供了：

- **一栈式日志解决方案**：同时支持日志中转、过滤、解析、切分、日志报警等
- **云原生的日志形态**：快速便捷的容器日志采集方式，原生的Kubernetes CRD动态配置下发
- **生产级的特性**：基于长期的大规模运维经验，形成了全方位的可观测性、快速排障、异常预警、自动化运维能力

我们可以基于Loggie，打造一套的云原生可扩展的全链路日志数据平台。

![](https://loggie-io.github.io/docs/user-guide/enterprise-practice/imgs/loggie-extend.png)


## 特性

### 新一代的云原生日志采集和传输方式

#### 基于CRD的快速配置和使用

Loggie包含LogConfig/ClusterLogConfig/Interceptor/Sink CRD，只需简单的创建一些YAML文件，即可搭建一系列的数据采集、传输、处理、发送流水线。

示例：
```yaml
apiVersion: loggie.io/v1beta1
kind: LogConfig
metadata:
  name: tomcat
  namespace: default
spec:
  selector:
    type: pod
    labelSelector:
      app: tomcat
  pipeline:
    sources: |
      - type: file
        name: common
        paths:
          - stdout
          - /usr/local/tomcat/logs/*.log
    sinkRef: default
    interceptorRef: default
```

![crd-usage](https://loggie-io.github.io/docs/user-guide/use-in-kubernetes/imgs/loggie-crd-usage.png)

#### 支持多种部署架构

- **Agent**: 使用DaemonSet部署，无需业务容器挂载Volume即可采集日志文件

- **Sidecar**: 支持Loggie sidecar无侵入自动注入，无需手动添加到Deployment/StatefulSet部署模版

- **Aggregator**: 支持Deployment独立部署成中转机形态，可接收聚合Loggie Agent发送的数据，也可单独用于消费处理各类数据源

但不管是哪种部署架构，Loggie仍然保持着简单直观的内部设计。

![](https://loggie-io.github.io/docs/getting-started/imgs/loggie-arch.png)

### 轻量级和高性能

#### 基准压测对比
配置Filebeat和Loggie采集日志，并发送至Kafka某个Topic，不使用客户端压缩，Kafka Topic配置Partition为3。

在保证Agent规格资源充足的情况下，修改采集的文件个数、发送客户端并发度（配置Filebeat worker和Loggie parallelism)，观察各自的CPU、Memory和Pod网卡发送速率。

| Agent    | 文件大小 | 日志文件数 | 发送并发度 | CPU      | MEM (rss) | 网卡发包速率    |
|----------|------|-------|-------|----------|-----------|-----------|
| Filebeat | 3.2G | 1     | 3     | 7.5~8.5c | 63.8MiB   | 75.9MiB/s |
| Filebeat | 3.2G | 1     | 8     | 10c      | 65MiB     | 70MiB/s   |
| Filebeat | 3.2G | 10    | 8     | 11c      | 65MiB     | 80MiB/s   |
|          |      |       |       |          |           |           |
| Loggie   | 3.2G | 1     | 3     | 2.1c     | 60MiB     | 120MiB/s  |
| Loggie   | 3.2G | 1     | 8     | 2.4c     | 68.7MiB   | 120MiB/s  |
| Loggie   | 3.2G | 10    | 8     | 3.5c     | 70MiB     | 210MiB/s  |


#### 自适应sink并发度

打开sink并发度配置后，Loggie可做到：
- 根据下游数据响应的实际情况，自动调整下游数据发送并行数，尽量发挥下游服务端的性能，且不影响其性能。
- 在上游数据收集被阻塞时，适当调整下游数据发送速度，缓解上游阻塞。

### 轻量级流式数据分析与监控
日志本身是一种通用的，和平台、系统无关的数据，如何更好的利用到这些数据，是Loggie关注和主要发展的核心能力。

![](https://loggie-io.github.io/docs/user-guide/enterprise-practice/imgs/loggie-chain.png)

#### 实时解析和转换
只需配置transformer interceptor，通过配置函数式的action，即可实现：
- 各种数据格式的解析（json, grok, regex, split...)
- 各种字段的转换（add, copy, move, set, del, fmt...)
- 支持条件判断和处理逻辑（if, else, return, dropEvent, ignoreError...)

可用于：
- 日志提取出日志级别level，并且drop掉DEBUG日志
- 日志里混合包括有json和plain的日志形式，可以判断json形式的日志并且进行处理
- 根据访问日志里的status code，增加不同的topic字段

示例：
```yaml
interceptors:
  - type: transformer
    actions:
      - action: regex(body)
        pattern: (?<ip>\S+) (?<id>\S+) (?<u>\S+) (?<time>\[.*?\]) (?<url>\".*?\") (?<status>\S+) (?<size>\S+)
      - if: equal(status, 404)
        then:
          - action: add(topic, not_found)
          - action: return()
      - if: equal(status, 500)
        then:
          - action: dropEvent()
```

#### 检测识别与报警
帮你快速检测到数据中可能出现的问题和异常，及时发出报警。

支持匹配方式：
- 无数据：配置的时间段内无日志数据产生
- 匹配
  - 模糊匹配
  - 正则匹配
- 条件判断
  - 字段比较：equal/less/greater…

支持部署形态：
- 在数据采集链路检测：简单易用，无需额外部署
- 独立链路检测两种形态：独立部署Aggregator，消费Kafka/Elasticsearch等进行数据的匹配和报警

均可支持自定义webhook对接各类报警渠道。

#### 业务数据聚合与监控
很多时候指标数据Metrics不仅仅是通过prometheus exporter来暴露，日志数据本身也可以提供指标的来源。
比如说，通过统计网关的access日志，可以计算出一段时间间隔内5xx或者4xx的statusCode个数，聚合某个接口的qps，计算出传输body的总量等等。

该功能正在内测中，敬请期待。

示例：
```yaml
- type: aggregator
  interval: 1m
  select:
  # 算子：COUNT/COUNT-DISTINCT/SUM/AVG/MAX/MIN
  - {key: amount, operator: SUM, as: amount_total}
  - {key: quantity, operator: SUM, as: qty_total}
  groupby: ["city"]
  # 计算：根据字段中的值，再计算处理
  calculate:
  - {expression: " ${amount_total} / ${qty_total} ", as: avg_amount}
```

### 全链路的快速排障与可观测性

- Loggie提供了可配置的、丰富的数据指标，还有dashboard可以一键导入到grafana中

  <img src="https://loggie-io.github.io/docs/user-guide/monitor/img/grafana-agent-1.png" width="1000"/>

- 使用Loggie terminal和help接口快速便捷的排查Loggie本身的问题，数据传输过程中的问题

  <img src="https://loggie-io.github.io/docs/user-guide/troubleshot/img/loggie-dashboard.png" width="1000"/>

## FAQs

### Loggie vs Filebeat/Fluentd/Logstash/Flume

|                  | Loggie                                              | Filebeat                           | Fluentd                  | Logstash       | Flume          |
|------------------|-----------------------------------------------------|------------------------------------|--------------------------|----------------|----------------|
| 开发语言             | Golang                                              | Golang                             | Ruby                     | JRuby          | Java           |
| 多Pipeline        | 支持                                                  | 单队列                                | 单队列                      | 支持             | 支持             |
| 多输出源             | 支持                                                  | 不支持，仅一个Output                      | 配置copy                   | 支持             | 支持             |
| 中转机              | 支持                                                  | 不支持                                | 支持                       | 支持             | 支持             |
| 日志报警             | 支持                                                  | 不支持                                | 不支持                      | 不支持            | 不支持            |
| Kubernetes容器日志采集 | 支持容器的stdout和容器内部日志文件                                | 只支持容器stdout                        | 只支持容器stdout              | 不支持            | 不支持            |
| 配置下发             | Kubernetes下可通过CRD配置，主机场景配置中心陆续支持中                   | 手动配置                               | 手动配置                     | 手动配置           | 手动配置           |
| 监控               | 原生支持Prometheus metrics，同时可配置单独输出指标日志文件、发送metrics等方式 | API接口暴露，接入Prometheus需使用额外的exporter | 支持API和Prometheus metrics | 需使用额外的exporter | 需使用额外的exporter |
| 资源占用             | 低                                                   | 低                                  | 一般                       | 较高             | 较高             |


## 文档
请参考Loggie[文档](https://loggie-io.github.io/docs/)。

## 快速上手

- [快速上手](https://loggie-io.github.io/docs/getting-started/quick-start/quick-start/)
- 部署([Kubernetes](https://loggie-io.github.io/docs/getting-started/install/kubernetes/), [主机](https://loggie-io.github.io/docs/getting-started/install/node/))


## 组件配置

- [启动参数](https://loggie-io.github.io/docs/reference/global/args/)
- [系统配置](https://loggie-io.github.io/docs/reference/global/system/)
- Pipeline配置
  - Source: [file](https://loggie-io.github.io/docs/reference/pipelines/source/file/), [kafka](https://loggie-io.github.io/docs/reference/pipelines/source/kafka/), [kubeEvent](https://loggie-io.github.io/docs/reference/pipelines/source/kubeEvent/), [grpc](https://loggie-io.github.io/docs/reference/pipelines/source/grpc/), [elasticsearch](https://loggie-io.github.io/docs/reference/pipelines/source/elasticsearch/), [prometheusExporter](https://loggie-io.github.io/docs/reference/pipelines/source/prometheus-exporter/)..
  - Sink: [elasticsearch](https://loggie-io.github.io/docs/reference/pipelines/sink/elasticsearch/), [kafka](https://loggie-io.github.io/docs/reference/pipelines/sink/kafka/), [grpc](https://loggie-io.github.io/docs/reference/pipelines/sink/grpc/), [loki](https://loggie-io.github.io/docs/reference/pipelines/sink/loki/), [zinc](https://loggie-io.github.io/docs/reference/pipelines/sink/zinc/), [alertWebhook](https://loggie-io.github.io/docs/reference/pipelines/sink/webhook/), [dev](https://loggie-io.github.io/docs/reference/pipelines/sink/dev/)..
  - Interceptor: [transformer](https://loggie-io.github.io/docs/reference/pipelines/interceptor/transformer/), [schema](https://loggie-io.github.io/docs/reference/pipelines/interceptor/schema/), [limit](https://loggie-io.github.io/docs/reference/pipelines/interceptor/limit/), [logAlert](https://loggie-io.github.io/docs/reference/pipelines/interceptor/logalert/), [maxbytes](https://loggie-io.github.io/docs/reference/pipelines/interceptor/maxbytes/)..
- CRD: [LogConfig](https://loggie-io.github.io/docs/reference/discovery/kubernetes/logconfig/), [ClusterLogConfig](https://loggie-io.github.io/docs/reference/discovery/kubernetes/clusterlogconfig/), [Sink](https://loggie-io.github.io/docs/reference/discovery/kubernetes/sink/), [Interceptor](https://loggie-io.github.io/docs/reference/discovery/kubernetes/interceptors/)

## RoadMap
- [RoadMap 2023](https://loggie-io.github.io/docs/getting-started/roadmap/roadmap-2023/)

## 交流讨论
在使用Loggie的时候遇到问题？ 请提issues或者联系我们。

微信扫码加入Loggie讨论群：

![loggie-bot](https://loggie-io.github.io/docs/getting-started/imgs/loggie-bot.png)

## License

[Apache-2.0](https://choosealicense.com/licenses/apache-2.0/)

## 贡献

欢迎提交代码、发表评论和建议。

查看 [CONTRIBUTING.md](CONTRIBUTING.md) 获得更多信息。
