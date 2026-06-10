# Event Rule Engine — 设计文档

**日期**: 2026-06-10  
**状态**: 已批准

---

## 1. 背景

每个业务平台都会定义很多业务规则，如果某个用户达成规则，就会执行特定动作（如每充值满 100 美元送奖励）。各业务规则散落在各需求代码里，缺乏统一管理。本平台统一接入和管理业务规则，计算并保存每个用户对应每条规则的进度，触发规则时通知业务方。

---

## 2. 整体分层架构

```
┌─────────────────────────────────────────────────┐
│  HTTP Layer (Gin)                               │
│  GET /users/:userId/rules/:ruleId/progress      │
└────────────────────┬────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────┐
│  Service Layer                                  │
│  Engine (消费事件，分发给匹配的 Rule)              │
│  RuleRegistry (注册/加载规则)                    │
└──────┬─────────────┬──────────────┬─────────────┘
       │             │              │
┌──────▼──────┐ ┌────▼─────┐ ┌─────▼──────┐
│  Window     │ │Aggregator│ │  Trigger   │
│  interface  │ │interface │ │  interface │
└──────┬──────┘ └────┬─────┘ └─────┬──────┘
       └─────────────┴──────────────┘
                     │
┌────────────────────▼────────────────────────────┐
│  StateStore interface                           │
│  MySQL (持久化) + Redis (缓存/原子更新)           │
└─────────────────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────┐
│  Kafka Consumer (events/consumer.go)            │
│  → engine.Consume(event)                        │
└─────────────────────────────────────────────────┘
```

**依赖方向**：严格从上到下，所有跨层依赖通过接口（interface）注入，不在内部 new 具体实现。

---

## 3. Domain 模型

```go
// Rule 完整定义
type Rule struct {
    Biz         string
    ID          int64
    Name        string
    Description string
    EventType   string
    Condition   ConditionConfig   // CEL 表达式配置
    Window      WindowConfig      // global/fixed/sliding/range
    Aggregator  AggregatorConfig  // sum/count + field
    Trigger     TriggerConfig     // every/threshold/all_gte/count_gte
}

// Bucket：某个时间窗口格子的聚合值
type Bucket struct {
    Key   string // 如 "2026-05", "2026-05-09", "global"
    Value int64
}

// RuleProgress：用户对某条规则的完整进度
type RuleProgress struct {
    Biz             string
    UserID          int64
    RuleID          int64
    Buckets         []Bucket
    CurrentLevel    int64  // 适用于 every 类型规则
    NextThreshold   int64  // 下次触发阈值
    LastTriggeredAt int64  // 上次触发时间戳
}
```

---

## 4. 核心接口定义

所有组件通过接口定义，依赖注入，便于 mock 测试和替换实现。

```go
// Condition：CEL 表达式匹配，过滤不符合条件的事件
type Condition interface {
    Match(payload map[string]interface{}) (bool, error)
}

// Window：决定事件归属哪个 bucket key，以及触发判断时需要哪些 keys
type Window interface {
    BucketKey(eventTime int64) string     // 当前事件归属的 bucket
    ActiveKeys(now int64) []string        // Trigger 需要看的所有 bucket keys
}

// Aggregator：从 payload 提取增量值
type Aggregator interface {
    Extract(payload map[string]interface{}) (int64, error)
}

// Trigger：判断 buckets 是否满足触发条件，返回是否触发及下一阈值
type Trigger interface {
    Check(buckets []Bucket, progress RuleProgress) (triggered bool, nextThreshold int64)
}

// StateStore：进度的持久化与查询
type StateStore interface {
    GetBuckets(ctx context.Context, biz string, userID, ruleID int64, keys []string) ([]Bucket, error)
    IncrBucket(ctx context.Context, biz string, userID, ruleID int64, key string, delta int64) error
    GetProgress(ctx context.Context, biz string, userID, ruleID int64) (RuleProgress, error)
    SaveProgress(ctx context.Context, progress RuleProgress) error
}
```

---

## 5. Window 实现

| 类型 | 配置 | BucketKey 示例 | ActiveKeys |
|------|------|----------------|------------|
| global | `{"type":"global"}` | `"global"` | `["global"]` |
| fixed/month | `{"type":"fixed","unit":"month"}` | `"2026-05"` | `["2026-05"]` |
| fixed/week | `{"type":"fixed","unit":"week"}` | `"2026-W23"` | `["2026-W23"]` |
| fixed/day | `{"type":"fixed","unit":"day"}` | `"2026-05-09"` | `["2026-05-09"]` |
| sliding | `{"type":"sliding","size":"7d","bucket":"day"}` | `"2026-05-09"` | 最近 N 天的所有 key |
| range | `{"type":"range","start_time":"...","end_time":"..."}` | `"range"` | `["range"]` |

---

## 6. Aggregator 实现

| 类型 | 说明 |
|------|------|
| `sum` | 累加 payload 中指定 field 的值 |
| `count` | 每次事件计数 +1 |

---

## 7. Trigger 实现

| 类型 | 说明 | 示例规则 |
|------|------|----------|
| `every` | 每累计满 step 触发一次，level+1 | 每满 100 万金币升 1 级 |
| `threshold` | 累计值首次达到 threshold 触发 | 月度活跃满 100 次 |
| `all_gte` | 所有 active buckets 的值都 >= threshold | 连续 3 天充值 >=500 |
| `count_gte` | active buckets 中 >= threshold 的 bucket 数量 >= count | 7 天内至少 5 天 >=20 关注 |

---

## 8. Engine 核心流程

```
engine.Consume(event):
  1. 幂等检查：Redis SET NX "eid:{eid}"，已处理则跳过
  2. 从 RuleRegistry 取出订阅该 event.Type 的所有 Rule
  3. 对每条 Rule，启动 goroutine 执行 match(event, rule)

engine.match(event, rule):
  1. condition.Match(event.Payload)              // CEL 过滤
  2. window.BucketKey(event.Timestamp)           // 定位 bucket key
  3. aggregator.Extract(event.Payload)           // 提取增量
  4. store.IncrBucket(biz, userId, ruleId, key, delta)  // 原子更新 bucket
  5. keys = window.ActiveKeys(now)               // 取触发判断所需 keys
  6. buckets = store.GetBuckets(..., keys)       // 读取 buckets
  7. progress = store.GetProgress(...)           // 读取当前进度
  8. triggered, nextThreshold = trigger.Check(buckets, progress)
  9. if triggered:
       store.SaveProgress(updated progress)      // 乐观锁更新
       eventbus.Publish(RuleEvent)               // 通知业务方
```

---

## 9. 并发安全策略

**Bucket 更新（高频写）**：
- Redis 主路径：使用 `INCRBY` 原子命令，无需加锁。
- MySQL 持久化：使用 `INSERT ... ON DUPLICATE KEY UPDATE value = value + ?`，避免 read-modify-write。

**Progress 更新（触发时写）**：
- 涉及读-判断-写三步，使用乐观锁。
- `rule_progress` 表加 `version` 字段，更新时 `WHERE version = ?` 且 `version = version + 1`，失败则重试（最多 3 次）。

**RuleRegistry（动态规则变更）**：
- Engine 内部 `rules map` 用 `sync.RWMutex` 保护。
- 消费事件时持读锁；注册/删除规则时持写锁。

**幂等去重**：
- Redis `SET NX "eid:{eid}" 1 EX 86400`，TTL 24 小时。
- 在 `Consume` 入口处拦截，防止同一事件被重复处理。

---

## 10. 存储设计

### MySQL 表结构

```sql
-- 规则表（持久化规则配置）
CREATE TABLE rules (
  id          BIGINT PRIMARY KEY AUTO_INCREMENT,
  biz         VARCHAR(64) NOT NULL,
  rule_id     BIGINT NOT NULL,
  name        VARCHAR(128),
  description TEXT,
  event_type  VARCHAR(64),
  config      JSON NOT NULL,  -- 完整 Rule 配置 JSON
  created_at  BIGINT,
  updated_at  BIGINT,
  UNIQUE KEY uk_biz_rule (biz, rule_id)
);

-- 用户规则进度表
CREATE TABLE rule_progress (
  id                BIGINT PRIMARY KEY AUTO_INCREMENT,
  biz               VARCHAR(64) NOT NULL,
  user_id           BIGINT NOT NULL,
  rule_id           BIGINT NOT NULL,
  current_level     BIGINT  DEFAULT 0,
  next_threshold    BIGINT  DEFAULT 0,
  last_triggered_at BIGINT  DEFAULT 0,
  version           BIGINT  DEFAULT 0,  -- 乐观锁版本号
  updated_at        BIGINT,
  UNIQUE KEY uk_biz_user_rule (biz, user_id, rule_id)
);

-- Bucket 持久化表
CREATE TABLE rule_buckets (
  id         BIGINT PRIMARY KEY AUTO_INCREMENT,
  biz        VARCHAR(64) NOT NULL,
  user_id    BIGINT NOT NULL,
  rule_id    BIGINT NOT NULL,
  bucket_key VARCHAR(32) NOT NULL,
  value      BIGINT DEFAULT 0,
  updated_at BIGINT,
  UNIQUE KEY uk_bucket (biz, user_id, rule_id, bucket_key)
);
```

### Redis Key 设计

| Key | 类型 | 用途 | TTL |
|-----|------|------|-----|
| `bucket:{biz}:{userId}:{ruleId}:{key}` | String | bucket 聚合值，INCRBY 原子更新 | 按 window 类型设置（sliding: 8d，fixed/month: 32d，global: 永久）|
| `eid:{eid}` | String | 事件幂等去重 | 24h |

---

## 11. HTTP API

```
GET /users/:userId/rules/:ruleId/progress?biz=xxx
```

响应：
```json
{
  "biz": "live",
  "user_id": 12345,
  "rule_id": 1001,
  "current_level": 2,
  "next_threshold": 3000000,
  "last_triggered_at": 1746748800,
  "buckets": [
    {"key": "global", "value": 2500000}
  ]
}
```

---

## 12. 测试策略

**单元测试**（每个组件独立）：
- Window 实现：验证各类型的 BucketKey 和 ActiveKeys 计算
- Aggregator 实现：验证 sum/count 从 payload 提取值
- Trigger 实现：验证 every/threshold/all_gte/count_gte 逻辑
- Engine.match：StateStore 用 mock，验证整个 match 流程

**集成测试**（Docker Compose）：
- 起 MySQL + Redis
- 用真实 StateStore 实现跑 rule_example.md 中全部 6 条规则
- 验证 progress 查询结果正确

**端到端测试**（Docker Compose + Kafka）：
- 通过 Kafka producer 发送事件
- 验证 Consumer → Engine → StateStore 全链路
- 通过 HTTP API 查询最终 progress

---

## 13. 技术栈

| 组件 | 技术 |
|------|------|
| 消息队列 | Kafka (IBM/sarama) |
| 数据库 | MySQL + GORM v2 |
| 缓存 | Redis (go-redis) |
| Web 框架 | Gin |
| Condition 解析 | CEL (google/cel-go) |
| 测试 | testify, testify/mock |
| 容器化 | Docker Compose |
