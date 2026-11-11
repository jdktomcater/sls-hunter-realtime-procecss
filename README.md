# Kafka 高危告警转 Telegram

独立的 Java 17 / Spring Boot 3 项目，消费本仓库 `WindowMiningJob` 写入 Kafka 的固定窗口快照，筛选高危信号并发送到 Telegram 群。无须修改或重新构建上游 Flink 作业。

## 运行

准备 Java 17、Maven 3.6.3+、可访问的 Kafka，以及已加入目标群并有发送权限的 Telegram bot。复制 `.env.example` 中的变量到部署环境，由密钥管理系统注入 bot token，勿将凭据提交到仓库。示例：

```sh
cd telegram-alert-java
mvn clean package
export KAFKA_BOOTSTRAP_SERVERS='localhost:9092'
export KAFKA_TOPIC='sls-mining-window-fixed'
export KAFKA_GROUP_ID='sls-telegram-alert-v1'
export TELEGRAM_BOT_TOKEN='由密钥管理系统注入'
export TELEGRAM_CHAT_ID='-1001234567890'
export STATE_FILE='/var/lib/sls-telegram/alert-state.db'
export RISK_MODE='score'
export MIN_SCORE='70'
java -jar target/telegram-alert-1.0.0.jar
```

实际 broker 地址以部署环境为准；本仓库示例配置中的地址是内网地址。`STATE_FILE` 的父目录会自动创建，必须持久化且仅供该服务访问。可通过 Spring Boot 配置 `spring.kafka.properties.*` 设置 SASL/TLS，不要将密码写进版本库。首次建组默认 `latest`，只消费启动后的新消息；要回放历史可设 `KAFKA_AUTO_OFFSET_RESET=earliest`，并评估群消息量。已有消费组位点时该选项不改变位点。消费者设置 `read_committed`，上游事务提交后才可见。

### 筛选策略

- `RISK_MODE=score`（默认）：只接受 `baseline_mode=none` 的固定窗口快照，筛选所有评分大于等于 `MIN_SCORE` 的信号。适用于本仓库当前 `history.compare=false` 配置；评分阈值是告警策略，不能证明攻击已发生。
- `RISK_MODE=channel`：仅筛选 `channel=high_risk` 且达到评分阈值的信号。只接受 `baseline_mode=same_time_previous_days`，适用于上游 `history.compare=true` 的双通道模式。切换模式需要从上游确认输出配置；不要将无通道的旧规则快照用于 channel 模式。

上游数据必须是 `kind=SNAPSHOT`、`operation=REPLACE`，包含 `index_family`、`window_start`、正整数 `revision` 和 `all_signals`，每个信号必须有唯一的 `signal_id`；被筛选的信号须有数字 `score`。不支持日快照或其他格式。服务按窗口及信号 ID 比较完整快照：新出现的信号发送一次，已存在信号的评分、计数、详情变化不再发送，消失后重新出现则再次发送。首次见到窗口时符合条件的现有信号都会发送。

## 可靠性与运行限制

- 每个消费组只能运行一个实例，并始终使用同一持久状态文件。文件锁只保护访问同一文件的进程，不会阻止其他机器使用不同文件加入同一消费组。SQLite 状态持续增长，不自动清理，应监控磁盘和备份；不要独立回滚状态文件或 Kafka 位点。规则重跑、revision 重置或切换数据流时应使用隔离 topic、消费组和状态文件。
- 处理完一条 Kafka 记录后才同步提交位点；每个发送成功的信号先记录到 SQLite，后续发送失败时重放会跳过已记录信号。Telegram 已接收但返回超时，或者收到成功响应后进程在持久化前崩溃，可能重复发送，无法保证端到端精确一次。
- 非法消息、Telegram 拒绝、限流、超时或网络错误会停止 Kafka listener，不提交该条位点；Spring Boot 进程可能仍在运行。必须监控 listener 停止事件及消费延迟，修复原因后重启进程；配置进程存活探针本身不足以检测该故障。若需要自动恢复，应配合外部监控重启并设置退避，避免 Telegram 限流时频繁重试。
- 上游用窗口 key 保证同窗口记录在同一分区；不要在运行时改变分区映射或增加分区。同一窗口 revision 必须递增。上游不发送空快照，某些全部信号撤销的情况不可见；服务也不发送撤销通知。
- 通知只包含摘要，不包含原始命令样本，但仍可能包含主机、用户名等敏感信息。上线前确认群权限和数据处理要求。网络发送在消费线程执行；调大 `KAFKA_MAX_POLL_INTERVAL_MS` 可降低长时间处理触发重平衡的风险，但不能解决持续发送失败。

运行测试：`mvn test`。上线前请使用隔离 topic 和测试群验证 Kafka、Telegram、重启重放、权限和限流。当前工程未携带真实凭据，也不会自动发送测试消息。
