package domain

// ConditionConfig 条件配置，使用 CEL 表达式
type ConditionConfig struct {
	Expression string `json:"expression"`
}

// WindowConfig 时间窗口配置
type WindowConfig struct {
	Type      string `json:"type"`                 // global | fixed | sliding | range
	Unit      string `json:"unit,omitempty"`       // fixed: month | week | day
	Size      string `json:"size,omitempty"`       // sliding: e.g. "7d"
	BucketUnit string `json:"bucket,omitempty"`    // sliding: day | week
	StartTime string `json:"start_time,omitempty"` // range
	EndTime   string `json:"end_time,omitempty"`   // range
}

// AggregatorConfig 聚合器配置
type AggregatorConfig struct {
	Type  string `json:"type"`            // sum | count
	Field string `json:"field,omitempty"` // sum 时指定字段名
}

// TriggerConfig 触发器配置
type TriggerConfig struct {
	Type      string `json:"type"`                // every | threshold | all_gte | count_gte
	Step      int64  `json:"step,omitempty"`      // every: 步长
	Threshold int64  `json:"threshold,omitempty"` // threshold / all_gte / count_gte
	Count     int64  `json:"count,omitempty"`     // count_gte: 满足条件的 bucket 数量
}

// Rule 业务规则完整定义
type Rule struct {
	Biz         string           `json:"biz"`
	ID          int64            `json:"id"`
	Name        string           `json:"name"`
	Description string           `json:"description"`
	EventType   string           `json:"event_type"`
	Condition   ConditionConfig  `json:"condition"`
	Window      WindowConfig     `json:"window"`
	Aggregator  AggregatorConfig `json:"aggregate"`
	Trigger     TriggerConfig    `json:"trigger"`
}

// Bucket 某个时间窗口格子的聚合值
type Bucket struct {
	Key   string `json:"key"`   // e.g. "global", "2026-05", "2026-05-09"
	Value int64  `json:"value"`
}

// RuleProgress 用户对某条规则的完整进度
type RuleProgress struct {
	Biz             string   `json:"biz"`
	UserID          int64    `json:"user_id"`
	RuleID          int64    `json:"rule_id"`
	Buckets         []Bucket `json:"buckets"`
	CurrentLevel    int64    `json:"current_level"`  // 适用于 every 类型
	NextThreshold   int64    `json:"next_threshold"` // 下次触发阈值
	LastTriggeredAt int64    `json:"last_triggered_at"`
	Version         int64    `json:"version"` // 乐观锁版本号
}

// RuleEvent 平台触发事件，通知业务方
type RuleEvent struct {
	Biz          string `json:"biz"`
	UserID       int64  `json:"user_id"`
	RuleID       int64  `json:"rule_id"`
	CurrentValue int64  `json:"current_value"`
	Threshold    int64  `json:"threshold"`
}
