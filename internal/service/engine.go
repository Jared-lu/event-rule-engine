package service

import (
	"context"
	"time"

	"github.com/Jared-lu/event-rule-engine/internal/domain"
)

// Engine 负责消费 Event，按 EventType 通过 RuleRegistry 找出规则并并发执行
type Engine struct {
	registry    *RuleRegistry
	store       domain.StateStore
	eventBus    domain.EventBus
	idempotency domain.Idempotency
}

func NewEngine(registry *RuleRegistry, store domain.StateStore, eventBus domain.EventBus, idempotency domain.Idempotency) *Engine {
	return &Engine{
		registry:    registry,
		store:       store,
		eventBus:    eventBus,
		idempotency: idempotency,
	}
}

// Consume 消费一个 Event
func (e *Engine) Consume(ctx context.Context, event domain.Event) {
	already, err := e.idempotency.CheckAndSet(ctx, event.EID)
	if err != nil || already {
		return
	}

	rules := e.registry.GetRule(event.Type)
	for _, rule := range rules {
		go e.match(ctx, event, rule)
	}
}

func (e *Engine) match(ctx context.Context, event domain.Event, rule CompiledRule) {
	matched, err := rule.Condition.Match(event.Payload)
	if err != nil || !matched {
		return
	}

	key := rule.Window.BucketKey(event.Timestamp)
	if key == "" {
		return
	}

	delta, err := rule.Aggregator.Extract(event.Payload)
	if err != nil || delta == 0 {
		return
	}

	r := rule.Rule
	if err := e.store.IncrBucket(ctx, r.Biz, event.UserId, r.ID, key, delta); err != nil {
		return
	}

	now := time.Now().Unix()
	activeKeys := rule.Window.ActiveKeys(now)

	buckets, err := e.store.GetBuckets(ctx, r.Biz, event.UserId, r.ID, activeKeys)
	if err != nil {
		return
	}

	progress, err := e.store.GetProgress(ctx, r.Biz, event.UserId, r.ID)
	if err != nil {
		return
	}

	triggered, nextThreshold := rule.Trigger.Check(buckets, progress)
	if !triggered {
		return
	}

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
