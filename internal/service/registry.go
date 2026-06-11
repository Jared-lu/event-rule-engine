package service

import (
	"context"
	"fmt"
	"sync"

	"github.com/Jared-lu/event-rule-engine/internal/domain"
)

// RuleRegistry 管理规则的生命周期：持久化、编译、内存索引
// 并发安全：读写均通过 mu 保护
type RuleRegistry struct {
	mu       sync.RWMutex
	compiled map[string][]CompiledRule // eventType -> compiled rules
	repo     domain.RuleRepository
}

// NewRuleRegistry 创建 RuleRegistry 并全量加载、编译所有规则
func NewRuleRegistry(ctx context.Context, repo domain.RuleRepository) (*RuleRegistry, error) {
	r := &RuleRegistry{
		compiled: make(map[string][]CompiledRule),
		repo:     repo,
	}
	rules, err := repo.FindAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("registry: load rules: %w", err)
	}
	for _, rule := range rules {
		compiled, err := Compile(rule)
		if err != nil {
			return nil, fmt.Errorf("registry: compile rule %d: %w", rule.ID, err)
		}
		r.compiled[rule.EventType] = append(r.compiled[rule.EventType], compiled)
	}
	return r, nil
}

// Register 持久化规则，编译后追加到内存索引
func (r *RuleRegistry) Register(ctx context.Context, rule domain.Rule) error {
	compiled, err := Compile(rule)
	if err != nil {
		return fmt.Errorf("registry: compile rule %d: %w", rule.ID, err)
	}
	if err := r.repo.Save(ctx, rule); err != nil {
		return fmt.Errorf("registry: save rule %d: %w", rule.ID, err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.compiled[rule.EventType] = append(r.compiled[rule.EventType], compiled)
	return nil
}

// Remove 从库和内存中删除规则
func (r *RuleRegistry) Remove(ctx context.Context, biz string, ruleID int64, eventType string) error {
	if err := r.repo.Delete(ctx, biz, ruleID); err != nil {
		return fmt.Errorf("registry: delete rule %d: %w", ruleID, err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	rules := r.compiled[eventType]
	filtered := rules[:0]
	for _, c := range rules {
		if c.Rule.Biz != biz || c.Rule.ID != ruleID {
			filtered = append(filtered, c)
		}
	}
	r.compiled[eventType] = filtered
	return nil
}

// GetRule 返回订阅指定 eventType 的已编译规则列表（返回副本，并发安全）
func (r *RuleRegistry) GetRule(eventType string) []CompiledRule {
	r.mu.RLock()
	defer r.mu.RUnlock()
	src := r.compiled[eventType]
	if len(src) == 0 {
		return nil
	}
	cp := make([]CompiledRule, len(src))
	copy(cp, src)
	return cp
}
