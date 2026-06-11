package service_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Jared-lu/event-rule-engine/internal/domain"
	"github.com/Jared-lu/event-rule-engine/internal/service"
)

// mockRuleRepo 内存实现 domain.RuleRepository，仅用于测试
type mockRuleRepo struct {
	rules []domain.Rule
}

func (m *mockRuleRepo) Save(_ context.Context, rule domain.Rule) error {
	for i, r := range m.rules {
		if r.Biz == rule.Biz && r.ID == rule.ID {
			m.rules[i] = rule
			return nil
		}
	}
	m.rules = append(m.rules, rule)
	return nil
}

func (m *mockRuleRepo) Delete(_ context.Context, biz string, ruleID int64) error {
	filtered := m.rules[:0]
	for _, r := range m.rules {
		if r.Biz != biz || r.ID != ruleID {
			filtered = append(filtered, r)
		}
	}
	m.rules = filtered
	return nil
}

func (m *mockRuleRepo) FindByEventType(_ context.Context, eventType string) ([]domain.Rule, error) {
	var result []domain.Rule
	for _, r := range m.rules {
		if r.EventType == eventType {
			result = append(result, r)
		}
	}
	return result, nil
}

func (m *mockRuleRepo) FindAll(_ context.Context) ([]domain.Rule, error) {
	cp := make([]domain.Rule, len(m.rules))
	copy(cp, m.rules)
	return cp, nil
}

func validRule(biz string, id int64, eventType string) domain.Rule {
	return domain.Rule{
		Biz:        biz,
		ID:         id,
		EventType:  eventType,
		Condition:  domain.ConditionConfig{Expression: ""},
		Window:     domain.WindowConfig{Type: "global"},
		Aggregator: domain.AggregatorConfig{Type: "count"},
		Trigger:    domain.TriggerConfig{Type: "every", Step: 1},
	}
}

func TestRuleRegistry_LoadOnStart(t *testing.T) {
	repo := &mockRuleRepo{rules: []domain.Rule{
		validRule("biz", 1, "ev_a"),
		validRule("biz", 2, "ev_b"),
	}}
	reg, err := service.NewRuleRegistry(context.Background(), repo)
	require.NoError(t, err)

	rules := reg.GetRule("ev_a")
	require.Len(t, rules, 1)
	assert.Equal(t, int64(1), rules[0].Rule.ID)

	rules = reg.GetRule("ev_b")
	require.Len(t, rules, 1)
	assert.Equal(t, int64(2), rules[0].Rule.ID)
}

func TestRuleRegistry_Register(t *testing.T) {
	repo := &mockRuleRepo{}
	reg, err := service.NewRuleRegistry(context.Background(), repo)
	require.NoError(t, err)

	rule := validRule("biz", 10, "ev_x")
	require.NoError(t, reg.Register(context.Background(), rule))

	rules := reg.GetRule("ev_x")
	require.Len(t, rules, 1)
	assert.Equal(t, int64(10), rules[0].Rule.ID)

	// 验证已持久化
	persisted, _ := repo.FindByEventType(context.Background(), "ev_x")
	require.Len(t, persisted, 1)
}

func TestRuleRegistry_Remove(t *testing.T) {
	repo := &mockRuleRepo{rules: []domain.Rule{
		validRule("biz", 5, "ev_y"),
	}}
	reg, err := service.NewRuleRegistry(context.Background(), repo)
	require.NoError(t, err)

	require.NoError(t, reg.Remove(context.Background(), "biz", 5, "ev_y"))

	assert.Empty(t, reg.GetRule("ev_y"))

	persisted, _ := repo.FindByEventType(context.Background(), "ev_y")
	assert.Empty(t, persisted)
}

func TestRuleRegistry_GetRule_UnknownEventType(t *testing.T) {
	repo := &mockRuleRepo{}
	reg, err := service.NewRuleRegistry(context.Background(), repo)
	require.NoError(t, err)

	rules := reg.GetRule("no_such_event")
	assert.Empty(t, rules)
}

func TestRuleRegistry_ConcurrentRegisterAndGet(t *testing.T) {
	repo := &mockRuleRepo{}
	reg, err := service.NewRuleRegistry(context.Background(), repo)
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		for i := int64(0); i < 100; i++ {
			_ = reg.Register(context.Background(), validRule("biz", i, "ev_z"))
		}
		close(done)
	}()
	for i := 0; i < 100; i++ {
		_ = reg.GetRule("ev_z")
	}
	<-done
}
