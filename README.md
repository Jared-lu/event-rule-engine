# Event Rule Engine

基于事件流的通用规则引擎平台。统一接入和管理业务规则，实时计算每个用户对应每条规则的进度，触发后通知业务方。

## 背景

各业务平台的规则散落在各需求代码里，缺乏统一管理。本平台解决这一痛点：

- 统一接入和管理业务规则
- 实时计算每个用户对规则的进度
- 触发规则后通过消息队列通知业务方

## 核心流程

```
业务方 → Kafka → Rule Engine → State Store → EventBus → 业务方
```

1. 业务方将业务事件（充值、送礼、关注等）发布到 Kafka
2. Rule Engine 消费事件，按 EventType 匹配订阅规则
3. 每条规则并发执行 match pipeline，更新用户进度
4. 触发后将 RuleEvent 发布到 Kafka 通知业务方

## 架构

```
internal/
├── domain/         # 核心模型和接口定义（Rule, Event, Bucket, RuleProgress 等）
├── repository/     # 持久化层
│   ├── rule_dao.go         # rules 表 GORM 操作
│   ├── rule_repo.go        # RuleRepository 实现
│   └── state_store.go      # StateStore 实现（MySQL + Redis 双写）
├── service/        # 业务逻辑
│   ├── engine.go           # 事件消费和 match pipeline
│   ├── registry.go         # RuleRegistry：规则生命周期管理
│   ├── rule.go             # CompiledRule 和 Compile 函数
│   ├── condition/          # CEL 表达式过滤
│   ├── window/             # 时间窗口（global/fixed/sliding/range）
│   ├── aggregator/         # 聚合器（sum/count）
│   └── trigger/            # 触发策略（every/threshold/all_gte/count_gte）
├── events/         # Kafka 消费者和 EventBus 生产者
├── web/            # HTTP 进度查询接口
└── pkg/            # 基础设施工具
    └── idempotency.go      # Redis 幂等去重
```

## 规则配置

每条规则由五个组件组成：

| 组件 | 作用 | 可选值 |
|------|------|--------|
| Condition | CEL 表达式过滤事件 | 任意 CEL 表达式，如 `coin > 0` |
| Window | 决定事件归属哪个时间桶 | `global` / `fixed` / `sliding` / `range` |
| Aggregator | 从 payload 提取增量值 | `sum`（指定字段）/ `count` |
| Trigger | 判断进度是否触发 | `every` / `threshold` / `all_gte` / `count_gte` |

### 示例规则

**每累计送礼金币满 100 万升一级：**
```json
{
  "biz": "live",
  "id": 1,
  "event_type": "gift_send",
  "condition": { "expression": "coin > 0" },
  "window": { "type": "global" },
  "aggregate": { "type": "sum", "field": "coin" },
  "trigger": { "type": "every", "step": 1000000 }
}
```

**连续 3 天每天充值 >= 500：**
```json
{
  "biz": "live",
  "id": 2,
  "event_type": "recharge",
  "condition": { "expression": "amount > 0" },
  "window": { "type": "sliding", "size": "3d", "bucket": "day" },
  "aggregate": { "type": "sum", "field": "amount" },
  "trigger": { "type": "all_gte", "threshold": 500 }
}
```

**最近 7 天至少 5 天获得 20 个以上关注：**
```json
{
  "biz": "live",
  "id": 3,
  "event_type": "follow",
  "condition": { "expression": "followed == true" },
  "window": { "type": "sliding", "size": "7d", "bucket": "day" },
  "aggregate": { "type": "count" },
  "trigger": { "type": "count_gte", "threshold": 20, "count": 5 }
}
```

## Match Pipeline

每个事件命中规则后，按以下顺序处理：

1. **Condition** — CEL 表达式过滤，不匹配则丢弃
2. **Window.BucketKey** — 确定事件归属的时间桶（range 窗口范围外返回空串丢弃）
3. **Aggregator.Extract** — 从 payload 提取增量值
4. **StateStore.IncrBucket** — Redis 原子写 + MySQL 持久化
5. **Window.ActiveKeys** — 获取触发判断需要的桶列表
6. **StateStore.GetBuckets** — 读取桶数据（Redis 优先，miss 查 MySQL）
7. **StateStore.GetProgress** — 读取用户当前进度
8. **Trigger.Check** — 判断是否达到触发条件
9. **StateStore.SaveProgress** — 乐观锁更新进度（最多重试 3 次）
10. **EventBus.Publish** — 发布 RuleEvent 通知业务方

## 存储设计

**Bucket（桶数据）：**
- 写：Redis `INCRBY`（原子） + MySQL `INSERT ON DUPLICATE KEY UPDATE`
- 读：Redis 优先，miss 回查 MySQL
- Redis Key：`bucket:{biz}:{userID}:{ruleID}:{bucketKey}`

**Progress（用户进度）：**
- 存储：MySQL（`rule_progress` 表）
- 并发：乐观锁（`version` 字段），冲突时最多重试 3 次

**幂等：**
- Redis `SET NX` 按 EID 去重，TTL 24h

## 快速开始

### 环境依赖

- Go 1.25+
- MySQL 8.0
- Redis 7
- Kafka

### 配置

通过环境变量配置：

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `MYSQL_DSN` | `root:root@tcp(localhost:3306)/rule_engine?...` | MySQL 连接串 |
| `REDIS_ADDR` | `localhost:6379` | Redis 地址 |
| `REDIS_PASSWORD` | 空 | Redis 密码 |
| `KAFKA_BROKER` | `localhost:9092` | Kafka Broker |
| `KAFKA_CONSUMER_GROUP` | `rule-engine` | 消费者组 |
| `BIZ_EVENT_TOPIC` | `biz-events` | 业务事件 Topic |
| `RULE_EVENT_TOPIC` | `rule-events` | 规则触发事件 Topic |
| `HTTP_ADDR` | `:8080` | HTTP 监听地址 |

### 运行

```bash
go run main.go
```

### 查询用户进度

```
GET /progress?biz={biz}&user_id={userID}&rule_id={ruleID}
```

## 测试

```bash
# 单元测试
go test ./internal/service/... ./internal/repository/... -v

# 集成测试（需要 Docker，自动拉起 MySQL + Redis）
go test ./internal/integration/... -v
```

集成测试覆盖 6 个典型场景：全局累计、条件过滤计数、滑动窗口 all_gte、固定窗口 threshold、滑动窗口 count_gte、固定时间范围 threshold。

也支持通过环境变量接入外部实例：

```bash
TEST_MYSQL_DSN="root:pass@tcp(localhost:3306)/testdb?..." \
TEST_REDIS_ADDR="localhost:6379" \
go test ./internal/integration/... -v
```

## 技术栈

- **语言：** Go
- **消息队列：** Kafka (IBM/sarama)
- **数据库：** MySQL + GORM v2
- **缓存：** Redis (go-redis/v9)
- **Web 框架：** Gin
- **条件表达式：** CEL (google/cel-go)
- **测试：** testify + testcontainers
