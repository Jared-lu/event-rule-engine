package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/Jared-lu/event-rule-engine/internal/domain"
)

// RuleProgressDAO GORM 映射表 rule_progress
type RuleProgressDAO struct {
	ID              int64  `gorm:"primaryKey;autoIncrement"`
	Biz             string `gorm:"uniqueIndex:uk_biz_user_rule;not null;type:varchar(100)"`
	UserID          int64  `gorm:"uniqueIndex:uk_biz_user_rule;not null;column:user_id"`
	RuleID          int64  `gorm:"uniqueIndex:uk_biz_user_rule;not null;column:rule_id"`
	CurrentLevel    int64  `gorm:"default:0;column:current_level"`
	NextThreshold   int64  `gorm:"default:0;column:next_threshold"`
	LastTriggeredAt int64  `gorm:"default:0;column:last_triggered_at"`
	Version         int64  `gorm:"default:0"`
	UpdatedAt       int64  `gorm:"column:updated_at"`
}

func (RuleProgressDAO) TableName() string { return "rule_progress" }

// RuleBucketDAO GORM 映射表 rule_buckets
type RuleBucketDAO struct {
	ID        int64  `gorm:"primaryKey;autoIncrement"`
	Biz       string `gorm:"uniqueIndex:uk_bucket;not null;type:varchar(100)"`
	UserID    int64  `gorm:"uniqueIndex:uk_bucket;not null;column:user_id"`
	RuleID    int64  `gorm:"uniqueIndex:uk_bucket;not null;column:rule_id"`
	BucketKey string `gorm:"uniqueIndex:uk_bucket;not null;column:bucket_key;type:varchar(50)"`
	Value     int64  `gorm:"default:0"`
	UpdatedAt int64  `gorm:"column:updated_at"`
}

func (RuleBucketDAO) TableName() string { return "rule_buckets" }

// StateStoreImpl MySQL + Redis 双写实现
type StateStoreImpl struct {
	db    *gorm.DB
	redis *redis.Client
}

func NewStateStore(db *gorm.DB, rdb *redis.Client) domain.StateStore {
	return &StateStoreImpl{db: db, redis: rdb}
}

// AutoMigrate 自动建表（开发/测试用）
func AutoMigrate(db *gorm.DB) error {
	return db.AutoMigrate(&RuleProgressDAO{}, &RuleBucketDAO{})
}

// ---- IncrBucket ----

func (s *StateStoreImpl) IncrBucket(ctx context.Context, biz string, userID, ruleID int64, key string, delta int64) error {
	// Redis 原子 INCRBY
	rKey := bucketRedisKey(biz, userID, ruleID, key)
	if err := s.redis.IncrBy(ctx, rKey, delta).Err(); err != nil {
		return fmt.Errorf("store: redis IncrBy %s: %w", rKey, err)
	}

	// MySQL 持久化，INSERT ... ON DUPLICATE KEY UPDATE value = value + delta
	now := time.Now().Unix()
	res := s.db.WithContext(ctx).Exec(
		`INSERT INTO rule_buckets (biz, user_id, rule_id, bucket_key, value, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON DUPLICATE KEY UPDATE value = value + ?, updated_at = ?`,
		biz, userID, ruleID, key, delta, now, delta, now,
	)
	return res.Error
}

// ---- GetBuckets ----

func (s *StateStoreImpl) GetBuckets(ctx context.Context, biz string, userID, ruleID int64, keys []string) ([]domain.Bucket, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	result := make([]domain.Bucket, 0, len(keys))
	for _, key := range keys {
		rKey := bucketRedisKey(biz, userID, ruleID, key)
		val, err := s.redis.Get(ctx, rKey).Int64()
		if err == nil {
			result = append(result, domain.Bucket{Key: key, Value: val})
			continue
		}
		if !errors.Is(err, redis.Nil) {
			return nil, fmt.Errorf("store: redis Get %s: %w", rKey, err)
		}
		// Redis miss，查 MySQL
		var row RuleBucketDAO
		dbErr := s.db.WithContext(ctx).
			Where("biz = ? AND user_id = ? AND rule_id = ? AND bucket_key = ?", biz, userID, ruleID, key).
			First(&row).Error
		if errors.Is(dbErr, gorm.ErrRecordNotFound) {
			result = append(result, domain.Bucket{Key: key, Value: 0})
		} else if dbErr != nil {
			return nil, fmt.Errorf("store: mysql GetBucket: %w", dbErr)
		} else {
			result = append(result, domain.Bucket{Key: key, Value: row.Value})
		}
	}
	return result, nil
}

// ---- GetProgress ----

func (s *StateStoreImpl) GetProgress(ctx context.Context, biz string, userID, ruleID int64) (domain.RuleProgress, error) {
	var row RuleProgressDAO
	err := s.db.WithContext(ctx).
		Where("biz = ? AND user_id = ? AND rule_id = ?", biz, userID, ruleID).
		First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return domain.RuleProgress{Biz: biz, UserID: userID, RuleID: ruleID}, nil
	}
	if err != nil {
		return domain.RuleProgress{}, fmt.Errorf("store: GetProgress: %w", err)
	}
	return domain.RuleProgress{
		Biz:             row.Biz,
		UserID:          row.UserID,
		RuleID:          row.RuleID,
		CurrentLevel:    row.CurrentLevel,
		NextThreshold:   row.NextThreshold,
		LastTriggeredAt: row.LastTriggeredAt,
		Version:         row.Version,
	}, nil
}

// ---- SaveProgress（乐观锁，最多 3 次重试）----

func (s *StateStoreImpl) SaveProgress(ctx context.Context, progress domain.RuleProgress) error {
	const maxRetry = 3
	for i := 0; i < maxRetry; i++ {
		err := s.trySaveProgress(ctx, progress)
		if err == nil {
			return nil
		}
		if !errors.Is(err, ErrVersionConflict) {
			return err
		}
		// 重新读取最新版本
		latest, getErr := s.GetProgress(ctx, progress.Biz, progress.UserID, progress.RuleID)
		if getErr != nil {
			return getErr
		}
		progress.Version = latest.Version
	}
	return ErrVersionConflict
}

var ErrVersionConflict = errors.New("store: version conflict")

func (s *StateStoreImpl) trySaveProgress(ctx context.Context, p domain.RuleProgress) error {
	now := time.Now().Unix()
	row := RuleProgressDAO{
		Biz:             p.Biz,
		UserID:          p.UserID,
		RuleID:          p.RuleID,
		CurrentLevel:    p.CurrentLevel,
		NextThreshold:   p.NextThreshold,
		LastTriggeredAt: p.LastTriggeredAt,
		Version:         p.Version + 1,
		UpdatedAt:       now,
	}
	// 尝试 upsert
	res := s.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "biz"}, {Name: "user_id"}, {Name: "rule_id"}},
		DoUpdates: clause.Assignments(map[string]interface{}{
			"current_level":     p.CurrentLevel,
			"next_threshold":    p.NextThreshold,
			"last_triggered_at": p.LastTriggeredAt,
			"version":           gorm.Expr("CASE WHEN version = ? THEN ? ELSE version END", p.Version, p.Version+1),
			"updated_at":        now,
		}),
	}).Create(&row)
	if res.Error != nil {
		return fmt.Errorf("store: SaveProgress: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return ErrVersionConflict
	}
	return nil
}

func bucketRedisKey(biz string, userID, ruleID int64, key string) string {
	return fmt.Sprintf("bucket:%s:%d:%d:%s", biz, userID, ruleID, key)
}
