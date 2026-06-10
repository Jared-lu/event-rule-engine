package aggregator

import (
	"fmt"

	"github.com/Jared-lu/event-rule-engine/internal/domain"
)

// SumAggregator 累加 payload 中指定字段的值
type SumAggregator struct {
	Field string
}

func (s *SumAggregator) Extract(payload map[string]interface{}) (int64, error) {
	v, ok := payload[s.Field]
	if !ok {
		return 0, fmt.Errorf("aggregator: field %q not found in payload", s.Field)
	}
	return toInt64(v)
}

// CountAggregator 每次事件计数 +1
type CountAggregator struct{}

func (c *CountAggregator) Extract(_ map[string]interface{}) (int64, error) {
	return 1, nil
}

// New 根据 AggregatorConfig 构建 Aggregator 实现
func New(cfg domain.AggregatorConfig) (domain.Aggregator, error) {
	switch cfg.Type {
	case "sum":
		if cfg.Field == "" {
			return nil, fmt.Errorf("aggregator: sum requires a field name")
		}
		return &SumAggregator{Field: cfg.Field}, nil
	case "count":
		return &CountAggregator{}, nil
	default:
		return nil, fmt.Errorf("aggregator: unknown type %q", cfg.Type)
	}
}

func toInt64(v interface{}) (int64, error) {
	switch n := v.(type) {
	case int64:
		return n, nil
	case int:
		return int64(n), nil
	case float64:
		return int64(n), nil
	case float32:
		return int64(n), nil
	default:
		return 0, fmt.Errorf("aggregator: cannot convert %T to int64", v)
	}
}
