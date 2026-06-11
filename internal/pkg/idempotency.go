package pkg

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Jared-lu/event-rule-engine/internal/domain"
)

// RedisIdempotency Redis SET NX 实现幂等去重
type RedisIdempotency struct {
	rdb *redis.Client
	ttl time.Duration
}

func NewRedisIdempotency(rdb *redis.Client) domain.Idempotency {
	return &RedisIdempotency{rdb: rdb, ttl: 24 * time.Hour}
}

func (r *RedisIdempotency) CheckAndSet(ctx context.Context, eid int64) (bool, error) {
	key := fmt.Sprintf("eid:%d", eid)
	ok, err := r.rdb.SetNX(ctx, key, 1, r.ttl).Result()
	if err != nil {
		return false, err
	}
	return !ok, nil
}
