package trigger

import (
	"fmt"

	"github.com/Jared-lu/event-rule-engine/internal/domain"
)

// EveryTrigger 每累计满 step 触发一次，level+1
type EveryTrigger struct {
	Step int64
}

func (e *EveryTrigger) Check(buckets []domain.Bucket, progress domain.RuleProgress) (bool, int64) {
	total := sumBuckets(buckets)
	nextThreshold := progress.NextThreshold
	if nextThreshold == 0 {
		nextThreshold = e.Step
	}
	if total >= nextThreshold {
		// 可能连续跨越多个阈值，但每次 match 只触发一次并更新到下一档
		return true, nextThreshold + e.Step
	}
	return false, nextThreshold
}

// ThresholdTrigger 累计值首次达到 threshold 触发一次
type ThresholdTrigger struct {
	Threshold int64
}

func (t *ThresholdTrigger) Check(buckets []domain.Bucket, progress domain.RuleProgress) (bool, int64) {
	// 已触发过（NextThreshold == 0 表示已完成，不再触发）
	if progress.LastTriggeredAt > 0 {
		return false, 0
	}
	total := sumBuckets(buckets)
	if total >= t.Threshold {
		return true, 0
	}
	return false, t.Threshold
}

// AllGteTrigger 所有 active buckets 的值都 >= threshold
type AllGteTrigger struct {
	Threshold int64
}

func (a *AllGteTrigger) Check(buckets []domain.Bucket, _ domain.RuleProgress) (bool, int64) {
	if len(buckets) == 0 {
		return false, a.Threshold
	}
	for _, b := range buckets {
		if b.Value < a.Threshold {
			return false, a.Threshold
		}
	}
	return true, a.Threshold
}

// CountGteTrigger active buckets 中 >= threshold 的 bucket 数量 >= count
type CountGteTrigger struct {
	Threshold int64
	Count     int64
}

func (c *CountGteTrigger) Check(buckets []domain.Bucket, _ domain.RuleProgress) (bool, int64) {
	var qualified int64
	for _, b := range buckets {
		if b.Value >= c.Threshold {
			qualified++
		}
	}
	return qualified >= c.Count, c.Threshold
}

// New 根据 TriggerConfig 构建 Trigger 实现
func New(cfg domain.TriggerConfig) (domain.Trigger, error) {
	switch cfg.Type {
	case "every":
		return &EveryTrigger{Step: cfg.Step}, nil
	case "threshold":
		return &ThresholdTrigger{Threshold: cfg.Threshold}, nil
	case "all_gte":
		return &AllGteTrigger{Threshold: cfg.Threshold}, nil
	case "count_gte":
		return &CountGteTrigger{Threshold: cfg.Threshold, Count: cfg.Count}, nil
	default:
		return nil, fmt.Errorf("trigger: unknown type %q", cfg.Type)
	}
}

func sumBuckets(buckets []domain.Bucket) int64 {
	var total int64
	for _, b := range buckets {
		total += b.Value
	}
	return total
}
