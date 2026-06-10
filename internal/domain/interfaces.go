package domain

import "context"

// Condition 负责过滤事件，CEL 表达式匹配
type Condition interface {
	Match(payload map[string]interface{}) (bool, error)
}

// Window 决定事件归属哪个 bucket key，以及触发判断时需要哪些 keys
type Window interface {
	BucketKey(eventTime int64) string // 当前事件归属的 bucket key
	ActiveKeys(now int64) []string    // Trigger 判断时需要的所有 bucket keys
}

// Aggregator 从 payload 提取增量值
type Aggregator interface {
	Extract(payload map[string]interface{}) (int64, error)
}

// Trigger 判断 buckets 是否满足触发条件，返回是否触发及下一阈值
type Trigger interface {
	Check(buckets []Bucket, progress RuleProgress) (triggered bool, nextThreshold int64)
}

// StateStore 进度的持久化与查询
type StateStore interface {
	GetBuckets(ctx context.Context, biz string, userID, ruleID int64, keys []string) ([]Bucket, error)
	IncrBucket(ctx context.Context, biz string, userID, ruleID int64, key string, delta int64) error
	GetProgress(ctx context.Context, biz string, userID, ruleID int64) (RuleProgress, error)
	SaveProgress(ctx context.Context, progress RuleProgress) error
}

// EventBus 将平台触发事件发布到消息队列
type EventBus interface {
	Publish(ctx context.Context, event RuleEvent) error
}
