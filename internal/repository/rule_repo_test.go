package repository_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Jared-lu/event-rule-engine/internal/domain"
	"github.com/Jared-lu/event-rule-engine/internal/repository"
)

func TestRuleRepository_SaveAndFindByEventType(t *testing.T) {
	dao := repository.NewMockRuleDAO()
	repo := repository.NewRuleRepository(dao)
	ctx := context.Background()

	rule := domain.Rule{
		Biz:       "test",
		ID:        1,
		Name:      "test rule",
		EventType: "gift_send",
		Condition: domain.ConditionConfig{Expression: "coin > 0"},
		Window:    domain.WindowConfig{Type: "global"},
		Aggregator: domain.AggregatorConfig{Type: "count"},
		Trigger:   domain.TriggerConfig{Type: "every", Step: 100},
	}

	require.NoError(t, repo.Save(ctx, rule))

	rules, err := repo.FindByEventType(ctx, "gift_send")
	require.NoError(t, err)
	require.Len(t, rules, 1)
	assert.Equal(t, rule.ID, rules[0].ID)
	assert.Equal(t, rule.EventType, rules[0].EventType)
}

func TestRuleRepository_Delete(t *testing.T) {
	dao := repository.NewMockRuleDAO()
	repo := repository.NewRuleRepository(dao)
	ctx := context.Background()

	rule := domain.Rule{Biz: "test", ID: 2, EventType: "recharge",
		Window:     domain.WindowConfig{Type: "global"},
		Aggregator: domain.AggregatorConfig{Type: "count"},
		Trigger:    domain.TriggerConfig{Type: "every", Step: 1},
	}
	require.NoError(t, repo.Save(ctx, rule))
	require.NoError(t, repo.Delete(ctx, "test", 2))

	rules, err := repo.FindByEventType(ctx, "recharge")
	require.NoError(t, err)
	assert.Empty(t, rules)
}

func TestRuleRepository_FindAll(t *testing.T) {
	dao := repository.NewMockRuleDAO()
	repo := repository.NewRuleRepository(dao)
	ctx := context.Background()

	for i := int64(1); i <= 3; i++ {
		require.NoError(t, repo.Save(ctx, domain.Rule{
			Biz: "test", ID: i, EventType: "ev",
			Window:     domain.WindowConfig{Type: "global"},
			Aggregator: domain.AggregatorConfig{Type: "count"},
			Trigger:    domain.TriggerConfig{Type: "every", Step: 1},
		}))
	}
	rules, err := repo.FindAll(ctx)
	require.NoError(t, err)
	assert.Len(t, rules, 3)
}
