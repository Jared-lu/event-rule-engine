package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/Jared-lu/event-rule-engine/internal/domain"
)

// RuleModel GORM 映射表 rules
type RuleModel struct {
	ID          int64  `gorm:"primaryKey;autoIncrement"`
	Biz         string `gorm:"uniqueIndex:uk_biz_rule;not null;type:varchar(100)"`
	RuleID      int64  `gorm:"uniqueIndex:uk_biz_rule;not null;column:rule_id"`
	Name        string `gorm:"not null;type:varchar(200)"`
	Description string `gorm:"type:text"`
	EventType   string `gorm:"not null;index;type:varchar(100);column:event_type"`
	Config      string `gorm:"not null;type:text"` // domain.Rule JSON
	CreatedAt   int64  `gorm:"column:created_at;autoCreateTime"`
	UpdatedAt   int64  `gorm:"column:updated_at;autoUpdateTime"`
}

func (RuleModel) TableName() string { return "rules" }

// RuleDAO GORM 操作 rules 表
type RuleDAO struct {
	db *gorm.DB
}

func NewRuleDAO(db *gorm.DB) *RuleDAO {
	return &RuleDAO{db: db}
}

// AutoMigrate 建表（开发/测试用）
func (d *RuleDAO) AutoMigrate() error {
	return d.db.AutoMigrate(&RuleModel{})
}

func (d *RuleDAO) Upsert(ctx context.Context, rule domain.Rule) error {
	data, err := json.Marshal(rule)
	if err != nil {
		return fmt.Errorf("rule_dao: marshal: %w", err)
	}
	now := time.Now().Unix()
	row := RuleModel{
		Biz:         rule.Biz,
		RuleID:      rule.ID,
		Name:        rule.Name,
		Description: rule.Description,
		EventType:   rule.EventType,
		Config:      string(data),
		UpdatedAt:   now,
	}
	res := d.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "biz"}, {Name: "rule_id"}},
		DoUpdates: clause.AssignmentColumns([]string{"name", "description", "event_type", "config", "updated_at"}),
	}).Create(&row)
	return res.Error
}

func (d *RuleDAO) Delete(ctx context.Context, biz string, ruleID int64) error {
	res := d.db.WithContext(ctx).
		Where("biz = ? AND rule_id = ?", biz, ruleID).
		Delete(&RuleModel{})
	return res.Error
}

func (d *RuleDAO) FindByEventType(ctx context.Context, eventType string) ([]RuleModel, error) {
	var rows []RuleModel
	err := d.db.WithContext(ctx).
		Where("event_type = ?", eventType).
		Find(&rows).Error
	return rows, err
}

func (d *RuleDAO) FindAll(ctx context.Context) ([]RuleModel, error) {
	var rows []RuleModel
	err := d.db.WithContext(ctx).Find(&rows).Error
	return rows, err
}

// toRule 将 RuleModel 反序列化为 domain.Rule
func toRule(row RuleModel) (domain.Rule, error) {
	var rule domain.Rule
	if err := json.Unmarshal([]byte(row.Config), &rule); err != nil {
		return domain.Rule{}, fmt.Errorf("rule_dao: unmarshal rule %d: %w", row.RuleID, err)
	}
	return rule, nil
}

var ErrRuleNotFound = errors.New("rule_dao: rule not found")
