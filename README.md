# Kafka 高危告警转 Telegram

独立的 Java 17 / Spring Boot 3 项目，消费 `sls-hunter-flink-mining` 的 `WindowMiningJob` 固定窗口快照或 `MiningJob` 日快照，筛选信号并发送到 Telegram 群。无须修改或重新构建上游 Flink 作业。`KAFKA_TOPIC` 须指向所选作业的结果 topic，不要指向 DLQ 或 late topic。

## 运行

准备 Java 17、Maven 3.6.3+、可访问的 Kafka，以及已加入目标群并有发送权限的 Telegram bot。复制 `.env.example` 中的变量到部署环境，由密钥管理系统注入 bot token，勿将凭据提交到仓库。示例：

```sh
mvn clean package
export KAFKA_BOOTSTRAP_SERVERS='localhost:9092'
export KAFKA_TOPIC='sls-mining-window-fixed'
export KAFKA_GROUP_ID='sls-telegram-alert-v1'
export TELEGRAM_BOT_TOKEN='由密钥管理系统注入'
export TELEGRAM_CHAT_ID='-1001234567890'
export RISK_MODE='score'
export MIN_SCORE='70'
java -jar target/sls-hunter-realtime-process-1.0.0.jar
```

实际 broker 地址以部署环境为准；本仓库示例配置中的地址是内网地址。可通过 Spring Boot 配置 `spring.kafka.properties.*` 设置 SASL/TLS，不要将密码写进版本库。首次建组默认 `latest`，只消费启动后的新消息；要回放历史可设 `KAFKA_AUTO_OFFSET_RESET=earliest`，并评估群消息量。已有消费组位点时该选项不改变位点。消费者设置 `read_committed`，上游事务提交后才可见。

### 筛选策略

- `RISK_MODE=score`（默认）：接受日快照及 `baseline_mode=none` 的固定窗口快照，筛选所有评分大于等于 `MIN_SCORE` 的信号。固定窗口适用于 `history.compare=false` 配置；评分阈值是告警策略，不能证明攻击已发生。
- `RISK_MODE=channel`：仅筛选 `channel=high_risk` 且达到评分阈值的信号。只接受 `baseline_mode=same_time_previous_days`，适用于上游 `history.compare=true` 的双通道模式。切换模式需要从上游确认输出配置；不要将无通道的旧规则快照用于 channel 模式。

上游数据必须是 `kind=SNAPSHOT`、`operation=REPLACE`，包含 `index_family`、固定窗口的 `window_start` 或日窗口的 `target_day`、正整数 `revision` 和 `all_signals`，每个信号必须有唯一的 `signal_id`；被筛选的信号须有数字 `score`。服务直接处理每条快照，并发送其中所有符合条件的信号，不保存窗口状态或去重。同一信号出现在多个快照时会重复告警；通知包含状态和版本，便于识别修订。

## 可靠性与运行限制

- 处理完一条 Kafka 记录后才提交位点。发送中途失败或发送成功后提交位点前进程退出时，重放该记录可能重复发送已成功的告警；Telegram 接收成功但响应超时也可能导致重复。
- 非法消息、Telegram 拒绝、限流、超时或网络错误会停止 Kafka listener，不提交该条位点；Spring Boot 进程可能仍在运行。必须监控 listener 停止事件及消费延迟，修复原因后重启进程；配置进程存活探针本身不足以检测该故障。若需要自动恢复，应配合外部监控重启并设置退避，避免 Telegram 限流时频繁重试。
- 上游不发送空快照，某些全部信号撤销的情况不可见；服务也不发送撤销通知。
- 通知只包含摘要，不包含原始命令样本，但仍可能包含主机、用户名等敏感信息。上线前确认群权限和数据处理要求。网络发送在消费线程执行；调大 `KAFKA_MAX_POLL_INTERVAL_MS` 可降低长时间处理触发重平衡的风险，但不能解决持续发送失败。

运行测试：`mvn test`。上线前请使用隔离 topic 和测试群验证 Kafka、Telegram、重启重放、权限和限流。当前工程未携带真实凭据，也不会自动发送测试消息。
