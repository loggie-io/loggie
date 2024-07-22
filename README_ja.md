<img src="https://github.com/loggie-io/loggie/blob/main/logo/loggie-draw.png" width="250">

[![go.dev reference](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white)](https://loggie-io.github.io/docs/)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/569/badge)](https://bestpractices.coreinfrastructure.org/projects/569)
[![dockerHub](https://img.shields.io/badge/dockerHub-loggieio%2Floggie-9cf)](https://hub.docker.com/r/loggieio/loggie/)

> 日本語 | [English](./README.md) | [中文](./README_cn.md)

Loggieは、Golangベースの軽量で高性能なクラウドネイティブのエージェントおよびアグリゲーターです。

- データ転送、フィルタリング、解析、アラートなど、複数のパイプラインとプラグイン可能なコンポーネントをサポートします。
- 操作と管理のためにネイティブのKubernetes CRDを使用します。
- 本番環境に適した一連の可観測性、信頼性、および自動化機能を提供します。

Loggieを基盤にして、クラウドネイティブでスケーラブルなログデータプラットフォームを構築できます。

![](https://loggie-io.github.io/docs/user-guide/enterprise-practice/imgs/loggie-extend.png)

## 特徴

### 次世代のクラウドネイティブログ収集と転送

#### CRDに基づくパイプラインの構築

LoggieにはLogConfig/ClusterLogConfig/Interceptor/Sink CRDが含まれており、簡単なYAMLファイルの作成を通じてデータ収集、転送、処理、および送信のパイプラインを作成できます。

例：
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

#### 複数のアーキテクチャ

- **エージェント**: DaemonSetを介してデプロイされ、Loggieはコンテナがボリュームをマウントする必要なくログファイルを収集できます。

- **サイドカー**: Loggieサイドカーの非侵入的な自動注入をサポートし、Deployment/StatefulSetテンプレートに手動で追加する必要はありません。

- **アグリゲーター**: 独立した中間機としてデプロイをサポートし、Loggieエージェントが送信する集約データを受信でき、さまざまなデータソースを消費および処理するためにも使用できます。

ただし、デプロイメントアーキテクチャに関係なく、Loggieは依然としてシンプルで直感的な内部設計を維持しています。

![](https://loggie-io.github.io/docs/getting-started/imgs/loggie-arch.png)

### 高性能

#### ベンチマーク

FilebeatとLoggieを構成してログを収集し、クライアント圧縮を使用せずにKafkaトピックに送信し、Kafkaトピックパーティションを3に構成します。

エージェント仕様のリソースが十分である場合、収集するファイルの数、送信クライアントの並行性（FilebeatワーカーとLoggieの並行性を構成）を変更し、それぞれのCPU、メモリ、およびポッドネットワークカードの送信速度を観察します。

| エージェント    | ファイルサイズ | ファイル数 | 送信並行性 | CPU      | メモリ (rss) | 送信速度    |
|----------|-----------|------------|------------------|----------|-----------|--------------------|
| Filebeat | 3.2G      | 1          | 3                | 7.5~8.5c | 63.8MiB   | 75.9MiB/s          |
| Filebeat | 3.2G      | 1          | 8                | 10c      | 65MiB     | 70MiB/s            |
| Filebeat | 3.2G      | 10         | 8                | 11c      | 65MiB     | 80MiB/s            |
|          |           |            |                  |          |           |                    |
| Loggie   | 3.2G      | 1          | 3                | 2.1c     | 60MiB     | 120MiB/s           |
| Loggie   | 3.2G      | 1          | 8                | 2.4c     | 68.7MiB   | 120MiB/s           |
| Loggie   | 3.2G      | 10         | 8                | 3.5c     | 70MiB     | 210MiB/s           |

#### アダプティブシンク並行性

シンク並行性構成が有効になっている場合、Loggieは次のことができます：

- 実際の下流データ応答に基づいて下流データ送信の並行性を自動的に調整し、下流サーバーのパフォーマンスを最大限に活用し、そのパフォーマンスに影響を与えません。
- 上流データ収集がブロックされている場合、上流のブロックを緩和するために下流データ送信速度を適切に調整します。

### 軽量なストリーミングデータ分析と監視

ログは一般的なデータタイプであり、プラットフォームやシステムに関連しません。このデータをどのようにうまく活用するかが、Loggieが注力し開発するコア機能です。

![](https://loggie-io.github.io/docs/user-guide/enterprise-practice/imgs/loggie-chain.png)

#### リアルタイム解析と変換

トランスフォーマインターセプターの構成と機能アクションの構成により、Loggieは次のことを実現できます：

- さまざまなデータ形式の解析（json、grok、regex、splitなど）
- さまざまなフィールドの変換（add、copy、move、set、del、fmtなど）
- 条件判断と処理ロジックのサポート（if、else、return、dropEvent、ignoreErrorなど）

例：
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

#### 検出、認識、およびアラート

データ内の潜在的な問題や異常を迅速に検出し、タイムリーにアラートを発行します。さまざまなアラートチャネルに接続するためのカスタムWebhookをサポートします。

次のようなマッチング方法をサポートします：

- データなし：構成された時間内にログデータが生成されない。
- あいまいなマッチング
- 正規表現マッチング
- 条件判断
  - フィールド比較：equal/less/greater...

#### ログデータの集約と監視

多くの場合、メトリックデータはプロメテウスエクスポーターを介して公開されるだけでなく、ログデータ自体もメトリックのソースを提供できます。たとえば、ゲートウェイのアクセスログをカウントすることで、特定の時間間隔内の5xxまたは4xxステータスコードの数を計算し、特定のインターフェースのqpsを集約し、ボディデータの総量を計算するなどです。

例：
```yaml
- type: aggregator
  interval: 1m
  select:
    # 演算子：COUNT/COUNT-DISTINCT/SUM/AVG/MAX/MIN
    - {key: amount, operator: SUM, as: amount_total}
    - {key: quantity, operator: SUM, as: qty_total}
  groupBy: ["city"]
  calculate:
    - {expression: " ${amount_total} / ${qty_total} ", as: avg_amount}
```

### 可観測性と迅速なトラブルシューティング

- Loggieは、構成可能で豊富なメトリックを提供し、Grafanaにワンクリックでインポートできるダッシュボードを提供します。

  <img src="https://loggie-io.github.io/docs/user-guide/monitor/img/grafana-agent-1.png" width="1000"/>

- Loggieターミナルを使用して、Loggie自体とデータ転送中の問題を迅速にトラブルシューティングします。

  <img src="https://loggie-io.github.io/docs/user-guide/troubleshot/img/loggie-dashboard.png" width="1000"/>

## FAQs

### Loggie vs Filebeat/Fluentd/Logstash/Flume

|                                     | Loggie                                                                                                           | Filebeat     | Fluentd            | Logstash      | Flume         |
|-------------------------------------|------------------------------------------------------------------------------------------------------------------|--------------|--------------------|---------------|---------------|
| 言語                                | Golang                                                                                                           | Golang       | Ruby               | JRuby         | Java          |
| 複数のパイプライン                  | ✓                                                                                                                | 単一キュー    | 単一キュー         | ✓             | ✓             |
| 複数の出力                          | ✓                                                                                                                | 1つの出力    | コピー             | ✓             | ✓             |
| アグリゲーター                      | ✓                                                                                                                | ✓            | ✓                  | ✓             | ✓             |
| ログアラーム                        | ✓                                                                                                                |              |                    |               |               |
| Kubernetesコンテナログ収集          | コンテナのstdoutとログファイルをサポート                                                                          | stdout       | stdout             |               |               |
| 構成の配信                          | CRDを通じて                                                                                                      | 手動         | 手動               | 手動          | 手動          |
| 監視                                | Prometheusメトリックをサポートし、メトリックログファイルの出力、メトリックの送信などを構成できます。               |              | prometheusメトリック | エクスポーターが必要 | エクスポーターが必要 |
| リソース使用量                      | 低い                                                                                                             | 低い         | 平均               | 高い          | 高い          |


## [ドキュメント](https://loggie-io.github.io/docs-ja/)

### [セットアップ](https://loggie-io.github.io/docs-ja/getting-started/overview/)

- [クイックスタート](https://loggie-io.github.io/docs-ja/getting-started/quick-start/quick-start/)
- インストール ([Kubernetes](https://loggie-io.github.io/docs-ja/getting-started/install/kubernetes/), [ノード](https://loggie-io.github.io/docs-ja/getting-started/install/node/))

### [ユーザーガイド](https://loggie-io.github.io/docs-ja/user-guide/)

- [アーキテクチャ](https://loggie-io.github.io/docs-ja/user-guide/architecture/core-arch/)
- [Kubernetes](https://loggie-io.github.io/docs-ja/user-guide/use-in-kubernetes/general-usage/)
- [監視](https://loggie-io.github.io/docs-ja/user-guide/monitor/loggie-monitor/)

### [リファレンス](https://loggie-io.github.io/docs-ja/reference/)

- [引数](https://loggie-io.github.io/docs-ja/reference/global/args/)
- [システム](https://loggie-io.github.io/docs-ja/reference/global/monitor/)
- パイプライン
  - ソース: [file](https://loggie-io.github.io/docs-ja/reference/pipelines/source/file/), [kafka](https://loggie-io.github.io/docs-ja/reference/pipelines/source/kafka/), [kubeEvent](https://loggie-io.github.io/docs-ja/reference/pipelines/source/kubeEvent/), [grpc](https://loggie-io.github.io/docs-ja/reference/pipelines/source/grpc/), [elasticsearch](https://loggie-io.github.io/docs-ja/reference/pipelines/source/elasticsearch/), [prometheusExporter](https://loggie-io.github.io/docs-ja/reference/pipelines/source/prometheus-exporter/)..
  - シンク: [elasticsearch](https://loggie-io.github.io/docs-ja/reference/pipelines/sink/elasticsearch/), [kafka](https://loggie-io.github.io/docs-ja/reference/pipelines/sink/kafka/), [grpc](https://loggie-io.github.io/docs-ja/reference/pipelines/sink/grpc/), [loki](https://loggie-io.github.io/docs-ja/reference/pipelines/sink/loki/), [zinc](https://loggie-io.github.io/docs-ja/reference/pipelines/sink/zinc/), [alertWebhook](https://loggie-io.github.io/docs-ja/reference/pipelines/sink/webhook/), [dev](https://loggie-io.github.io/docs-ja/reference/pipelines/sink/dev/)..
  - インターセプター: [transformer](https://loggie-io.github.io/docs-ja/reference/pipelines/interceptor/transformer/), [schema](https://loggie-io.github.io/docs-ja/reference/pipelines/interceptor/schema/), [limit](https://loggie-io.github.io/docs-ja/reference/pipelines/interceptor/limit/), [logAlert](https://loggie-io.github.io/docs-ja/reference/pipelines/interceptor/logalert/), [maxbytes](https://loggie-io.github.io/docs-ja/reference/pipelines/interceptor/maxbytes/)..
- CRD: [LogConfig](https://loggie-io.github.io/docs-ja/reference/discovery/kubernetes/logconfig/), [ClusterLogConfig](https://loggie-io.github.io/docs-ja/reference/discovery/kubernetes/clusterlogconfig/), [Sink](https://loggie-io.github.io/docs-ja/reference/discovery/kubernetes/sink/), [Interceptor](https://loggie-io.github.io/docs-ja/reference/discovery/kubernetes/interceptors/)

## ロードマップ
[ロードマップ 2023](https://loggie-io.github.io/docs-ja/getting-started/roadmap/roadmap-2023/)

## ライセンス

[Apache-2.0](https://choosealicense.com/licenses/apache-2.0/)

## 貢献

プルリクエスト、コメント、提案を歓迎します。

詳細については、[CONTRIBUTING.md](CONTRIBUTING.md)をご覧ください。
