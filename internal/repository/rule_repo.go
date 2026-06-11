package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/Jared-lu/event-rule-engine/internal/domain"
)

// ruleDAOIface is the minimal DAO interface, making it easy to swap in a test double.
type ruleDAOIface interface {
	Upsert(ctx context.Context, rule domain.Rule) error
	Delete(ctx context.Context, biz string, ruleID int64) error
	FindByEventType(ctx context.Context, eventType string) ([]RuleModel, error)
	FindAll(ctx context.Context) ([]RuleModel, error)
}

// RuleRepositoryImpl implements domain.RuleRepository.
type RuleRepositoryImpl struct {
	dao ruleDAOIface
}

func NewRuleRepository(dao ruleDAOIface) domain.RuleRepository {
	return &RuleRepositoryImpl{dao: dao}
}

func (r *RuleRepositoryImpl) Save(ctx context.Context, rule domain.Rule) error {
	return r.dao.Upsert(ctx, rule)
}

func (r *RuleRepositoryImpl) Delete(ctx context.Context, biz string, ruleID int64) error {
	return r.dao.Delete(ctx, biz, ruleID)
}

func (r *RuleRepositoryImpl) FindByEventType(ctx context.Context, eventType string) ([]domain.Rule, error) {
	rows, err := r.dao.FindByEventType(ctx, eventType)
	if err != nil {
		return nil, err
	}
	return rowsToRules(rows)
}

func (r *RuleRepositoryImpl) FindAll(ctx context.Context) ([]domain.Rule, error) {
	rows, err := r.dao.FindAll(ctx)
	if err != nil {
		return nil, err
	}
	return rowsToRules(rows)
}

func rowsToRules(rows []RuleModel) ([]domain.Rule, error) {
	rules := make([]domain.Rule, 0, len(rows))
	for _, row := range rows {
		rule, err := toRule(row)
		if err != nil {
			return nil, err
		}
		rules = append(rules, rule)
	}
	return rules, nil
}

// MockRuleDAO is an in-memory DAO implementation for use in tests only.
type MockRuleDAO struct {
	mu   sync.RWMutex
	rows map[string]RuleModel // key: biz:ruleID
}

func NewMockRuleDAO() *MockRuleDAO {
	return &MockRuleDAO{rows: make(map[string]RuleModel)}
}

func (m *MockRuleDAO) Upsert(_ context.Context, rule domain.Rule) error {
	data, _ := json.Marshal(rule)
	m.mu.Lock()
	defer m.mu.Unlock()
	key := fmt.Sprintf("%s:%d", rule.Biz, rule.ID)
	m.rows[key] = RuleModel{
		Biz:       rule.Biz,
		RuleID:    rule.ID,
		EventType: rule.EventType,
		Config:    string(data),
	}
	return nil
}

func (m *MockRuleDAO) Delete(_ context.Context, biz string, ruleID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.rows, fmt.Sprintf("%s:%d", biz, ruleID))
	return nil
}

func (m *MockRuleDAO) FindByEventType(_ context.Context, eventType string) ([]RuleModel, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []RuleModel
	for _, row := range m.rows {
		if row.EventType == eventType {
			result = append(result, row)
		}
	}
	return result, nil
}

func (m *MockRuleDAO) FindAll(_ context.Context) ([]RuleModel, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]RuleModel, 0, len(m.rows))
	for _, row := range m.rows {
		result = append(result, row)
	}
	return result, nil
}
