package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Jared-lu/event-rule-engine/internal/domain"
)

// Engine 负责消费 Event，按 EventType 找出匹配的规则并并发执行
type Engine struct {
	mu       sync.RWMutex
	rules    map[string][]CompiledRule // eventType -> rules
	store    domain.StateStore
	eventBus domain.EventBus
	idempotency Idempotency
}

// Idempotency 幂等检查接口，使用 Redis SET NX 实现
type Idempotency interface {
	CheckAndSet(ctx context.Context, eid int64) (alreadyProcessed bool, err error)
}

func NewEngine(store domain.StateStore, eventBus domain.EventBus, idempotency Idempotency) *Engine {
	return &Engine{
		rules:       make(map[string][]CompiledRule),
		store:       store,
		eventBus:    eventBus,
		idempotency: idempotency,
	}
}

// RegisterRule 注册一条规则（已编译为 CompiledRule）
func (e *Engine) RegisterRule(rule domain.Rule) error {
	compiled, err := Compile(rule)
	if err != nil {
		return fmt.Errorf("engine: compile rule %d: %w", rule.ID, err)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.rules[rule.EventType] = append(e.rules[rule.EventType], compiled)
	return nil
}

// RemoveRule 从本地注册表移除规则
func (e *Engine) RemoveRule(biz string, ruleID int64, eventType string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	rules := e.rules[eventType]
	filtered := rules[:0]
	for _, r := range rules {
		if r.Rule.Biz != biz || r.Rule.ID != ruleID {
			filtered = append(filtered, r)
		}
	}
	e.rules[eventType] = filtered
}

// Consume 消费一个 Event
func (e *Engine) Consume(ctx context.Context, event domain.Event) {
	// 幂等检查
	already, err := e.idempotency.CheckAndSet(ctx, event.EID)
	if err != nil || already {
		return
	}

	e.mu.RLock()
	rules := make([]CompiledRule, len(e.rules[event.Type]))
	copy(rules, e.rules[event.Type])
	e.mu.RUnlock()

	for _, rule := range rules {
		go e.match(ctx, event, rule)
	}
}

func (e *Engine) match(ctx context.Context, event domain.Event, rule CompiledRule) {
	// 1. Condition 过滤
	matched, err := rule.Condition.Match(event.Payload)
	if err != nil || !matched {
		return
	}

	// 2. 确定 bucket key
	key := rule.Window.BucketKey(event.Timestamp)
	if key == "" {
		// range window 且事件不在范围内
		return
	}

	// 3. 提取增量值
	delta, err := rule.Aggregator.Extract(event.Payload)
	if err != nil || delta == 0 {
		return
	}

	// 4. 原子更新 bucket
	r := rule.Rule
	if err := e.store.IncrBucket(ctx, r.Biz, event.UserId, r.ID, key, delta); err != nil {
		return
	}

	// 5. 取触发判断所需的 keys
	now := time.Now().Unix()
	activeKeys := rule.Window.ActiveKeys(now)

	// 6. 读取 buckets
	buckets, err := e.store.GetBuckets(ctx, r.Biz, event.UserId, r.ID, activeKeys)
	if err != nil {
		return
	}

	// 7. 读取当前进度
	progress, err := e.store.GetProgress(ctx, r.Biz, event.UserId, r.ID)
	if err != nil {
		return
	}

	// 8. 触发判断
	triggered, nextThreshold := rule.Trigger.Check(buckets, progress)
	if !triggered {
		return
	}

	// 9. 更新进度（乐观锁，失败由 Store 实现重试）
	total := sumBuckets(buckets)
	updated := domain.RuleProgress{
		Biz:             r.Biz,
		UserID:          event.UserId,
		RuleID:          r.ID,
		Buckets:         buckets,
		CurrentLevel:    progress.CurrentLevel + 1,
		NextThreshold:   nextThreshold,
		LastTriggeredAt: now,
		Version:         progress.Version,
	}
	if err := e.store.SaveProgress(ctx, updated); err != nil {
		return
	}

	// 10. 发布平台事件
	_ = e.eventBus.Publish(ctx, domain.RuleEvent{
		Biz:          r.Biz,
		UserID:       event.UserId,
		RuleID:       r.ID,
		CurrentValue: total,
		Threshold:    nextThreshold - (nextThreshold - progress.NextThreshold),
	})
}

func sumBuckets(buckets []domain.Bucket) int64 {
	var total int64
	for _, b := range buckets {
		total += b.Value
	}
	return total
}
